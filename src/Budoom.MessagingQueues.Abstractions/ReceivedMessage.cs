using System.Globalization;

namespace Budoom.MessagingQueues;

/// <summary>
/// One delivery: its headers, the deserialized body, and the four ways to settle it. Exactly one of
/// <see cref="AckAsync"/>, <see cref="NackAsync"/>, <see cref="RejectAsync"/> or
/// <see cref="RetryAsync"/> must be called — a delivery left unsettled holds a prefetch slot until
/// the connection drops, and once it drops the broker redelivers it.
/// </summary>
/// <remarks>
/// Broker packages derive from this and implement the <c>…CoreAsync</c> members; the settle-once
/// guard and the retry precondition live here so every broker behaves the same.
/// </remarks>
/// <typeparam name="TMessage">Type the JSON body was deserialized into.</typeparam>
public abstract class ReceivedMessage<TMessage>
{
    private int settled;

    /// <summary>Creates a delivery. Called by broker packages only.</summary>
    protected ReceivedMessage(
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        bool redelivered,
        string? messageId,
        string? messageType,
        string? contentType,
        string? correlationId,
        TMessage? message,
        Exception? deserializationError)
    {
        Queue = queue;
        Body = body;
        Headers = headers;
        Redelivered = redelivered;
        MessageId = messageId;
        MessageType = messageType;
        ContentType = contentType;
        CorrelationId = correlationId;
        Message = message;
        DeserializationError = deserializationError;
    }

    /// <summary>Logical name of the queue this came from.</summary>
    public string Queue { get; }

    /// <summary>Application headers, flattened to strings.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; }

    /// <summary>
    /// The deserialized body, or null if the payload did not fit <typeparamref name="TMessage"/> —
    /// check <see cref="DeserializationError"/> before using it.
    /// </summary>
    public TMessage? Message { get; }

    /// <summary>Set when the body could not be deserialized. The raw <see cref="Body"/> is still available.</summary>
    public Exception? DeserializationError { get; }

    /// <summary>The raw body. Already copied out of the client's buffer, so it stays valid.</summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>Unique id assigned when the message was first published.</summary>
    public string? MessageId { get; }

    /// <summary>The CLR type the publisher serialized from, for diagnostics. Not enforced on consume.</summary>
    public string? MessageType { get; }

    /// <summary>MIME type of <see cref="Body"/>, normally <c>application/json</c>.</summary>
    public string? ContentType { get; }

    /// <summary>Correlation id, when the publisher set one.</summary>
    public string? CorrelationId { get; }

    /// <summary>True when the broker has handed this message out before, for example after a connection drop.</summary>
    public bool Redelivered { get; }

    /// <summary>Delivery number for this message, starting at 1. Incremented by <see cref="RetryAsync"/>.</summary>
    public int Attempt =>
        Headers.TryGetValue(MessageHeaderNames.Attempt, out var value)
        && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var attempt)
            ? attempt
            : 1;

    /// <summary>
    /// Deliveries allowed before <see cref="RetryAsync"/> sends the message to the error queue. 1 when
    /// the queue has no retry policy.
    /// </summary>
    public abstract int MaxAttempts { get; }

    /// <summary>
    /// How long <see cref="RetryAsync"/> will hold the message before the next attempt, when called
    /// without a delay override. Null when the queue has no retry policy or this is the last attempt.
    /// </summary>
    public abstract TimeSpan? NextRetryDelay { get; }

    /// <summary>Whether the queue has a retry policy, i.e. whether <see cref="RetryAsync"/> may be called.</summary>
    public abstract bool SupportsRetry { get; }

    /// <summary>Whether one of the settlement methods has already been called.</summary>
    public bool IsSettled => Volatile.Read(ref settled) == 1;

    /// <summary>Handled successfully. The broker drops the message.</summary>
    public Task AckAsync(CancellationToken cancellationToken = default)
    {
        MarkSettled();

        return AckCoreAsync(cancellationToken);
    }

    /// <summary>
    /// Put the message back for immediate redelivery. For a blip that will have cleared by the time
    /// it comes round again — it comes back at once, so this is not a backoff.
    /// </summary>
    public Task NackAsync(CancellationToken cancellationToken = default)
    {
        MarkSettled();

        return NackCoreAsync(cancellationToken);
    }

    /// <summary>
    /// Discard the message without retrying. It is dead-lettered to the queue's error queue, where
    /// it can be inspected and replayed by hand.
    /// </summary>
    public Task RejectAsync(CancellationToken cancellationToken = default)
    {
        MarkSettled();

        return RejectCoreAsync(cancellationToken);
    }

    /// <summary>
    /// Hand the message back for a later attempt: it is parked and comes back once the delay expires.
    /// When attempts are exhausted it goes to the error queue instead.
    /// </summary>
    /// <param name="delay">
    /// Overrides the queue's backoff schedule. A broker may only support the configured delays, in
    /// which case the value is rounded up to the nearest one.
    /// </param>
    /// <param name="reason">Recorded on the message so the error queue says why it ended up there.</param>
    /// <param name="cancellationToken">Cancels the settlement.</param>
    /// <remarks>
    /// The retry copy is stored before the original is acknowledged, so a message is never lost — but
    /// a failure in between can duplicate it. Handlers should be idempotent.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The queue has no retry policy (<see cref="SupportsRetry"/> is false).</exception>
    public Task<RetryOutcome> RetryAsync(
        TimeSpan? delay = null,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        if (!SupportsRetry)
            throw new InvalidOperationException(
                $"Queue '{Queue}' has no retry policy, so RetryAsync is not available. Use NackAsync or RejectAsync, or configure retry on the queue.");

        MarkSettled();

        return RetryCoreAsync(delay, reason, cancellationToken);
    }

    /// <summary>Broker-specific acknowledgement.</summary>
    protected abstract Task AckCoreAsync(CancellationToken cancellationToken);

    /// <summary>Broker-specific immediate requeue.</summary>
    protected abstract Task NackCoreAsync(CancellationToken cancellationToken);

    /// <summary>Broker-specific dead-letter.</summary>
    protected abstract Task RejectCoreAsync(CancellationToken cancellationToken);

    /// <summary>Broker-specific delayed retry. Only called when <see cref="SupportsRetry"/> is true.</summary>
    protected abstract Task<RetryOutcome> RetryCoreAsync(TimeSpan? delay, string? reason, CancellationToken cancellationToken);

    private void MarkSettled()
    {
        if (Interlocked.Exchange(ref settled, 1) == 1)
            throw new InvalidOperationException("This message has already been settled. Acknowledge, reject or retry a delivery exactly once.");
    }
}
