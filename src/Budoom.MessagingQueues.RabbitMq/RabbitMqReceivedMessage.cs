using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Globalization;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// A RabbitMQ delivery. Settles on the channel it arrived on; <see cref="RetryCoreAsync"/> republishes
/// onto the queue's delay queue (or its error queue once attempts run out) before acknowledging.
/// </summary>
internal sealed class RabbitMqReceivedMessage<TMessage>(
    DeliveryContext context,
    ulong deliveryTag,
    ReadOnlyMemory<byte> body,
    IReadOnlyDictionary<string, string> headers,
    bool redelivered,
    string? messageId,
    string? messageType,
    string? contentType,
    string? correlationId,
    TMessage? message,
    Exception? deserializationError)
    : ReceivedMessage<TMessage>(
        context.Queue.LogicalName,
        body,
        headers,
        redelivered,
        messageId,
        messageType,
        contentType,
        correlationId,
        message,
        deserializationError)
{
    /// <summary>The first delivery plus one per configured delay. 1 when the queue has no retry policy.</summary>
    public override int MaxAttempts => context.Queue.Retry?.MaxAttempts ?? 1;

    public override TimeSpan? NextRetryDelay =>
        context.Queue.Retry is { } retry && Attempt < retry.MaxAttempts
            ? retry.DelayFor(Attempt + 1)
            : null;

    public override bool SupportsRetry => context.Queue.Retry is not null;

    protected override Task AckCoreAsync(CancellationToken cancellationToken) =>
        SettleAsync(context.Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken));

    /// <remarks>A quorum queue's delivery limit eventually dead-letters a message stuck in a nack loop.</remarks>
    protected override Task NackCoreAsync(CancellationToken cancellationToken) =>
        SettleAsync(context.Channel.BasicNackAsync(deliveryTag, multiple: false, requeue: true, cancellationToken));

    protected override Task RejectCoreAsync(CancellationToken cancellationToken) =>
        SettleAsync(context.Channel.BasicRejectAsync(deliveryTag, requeue: false, cancellationToken));

    /// <remarks>
    /// Only the configured delays have a queue, so a requested delay is rounded up to the nearest one.
    /// The retry copy is confirmed by the broker before the original is acknowledged.
    /// </remarks>
    protected override async Task<RetryOutcome> RetryCoreAsync(TimeSpan? delay, string? reason, CancellationToken cancellationToken)
    {
        var queue = context.Queue;
        var retry = queue.Retry!;

        var nextAttempt = Attempt + 1;

        var properties = BuildProperties(nextAttempt, reason);

        if (nextAttempt > retry.MaxAttempts)
        {
            await DeadLetterAsync(properties, retry.MaxAttempts, reason, cancellationToken);

            return RetryOutcome.DeadLettered;
        }

        var resolvedDelay = delay is { } requested ? retry.Snap(requested) : retry.DelayFor(nextAttempt);

        await context.Publisher.PublishRawAsync(
            MessagePublisher.DefaultExchange,
            queue.RetryQueueName(resolvedDelay),
            Body,
            properties,
            cancellationToken);

        await SettleAsync(context.Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken));

        RabbitMqLog.RetryScheduled(context.Logger, queue.LogicalName, nextAttempt, retry.MaxAttempts, resolvedDelay, reason);

        return RetryOutcome.Scheduled;
    }

    private async Task DeadLetterAsync(BasicProperties properties, int maxAttempts, string? reason, CancellationToken cancellationToken)
    {
        var queue = context.Queue;

        if (queue.ErrorQueueName is not { } errorQueueName)
        {
            await SettleAsync(context.Channel.BasicRejectAsync(deliveryTag, requeue: false, cancellationToken));

            return;
        }

        await context.Publisher.PublishRawAsync(
            MessagePublisher.DefaultExchange,
            errorQueueName,
            Body,
            properties,
            cancellationToken);

        await SettleAsync(context.Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken));

        RabbitMqLog.DeadLettered(context.Logger, queue.LogicalName, maxAttempts, errorQueueName, reason);
    }

    private BasicProperties BuildProperties(int nextAttempt, string? reason)
    {
        var amqpHeaders = AmqpHeaders.ToAmqp(Headers);

        amqpHeaders[MessageHeaderNames.Attempt] = nextAttempt.ToString(CultureInfo.InvariantCulture);
        amqpHeaders[MessageHeaderNames.OriginalQueue] = context.Queue.Name;
        amqpHeaders[MessageHeaderNames.FailedAt] = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        if (!string.IsNullOrWhiteSpace(reason))
            amqpHeaders[MessageHeaderNames.Reason] = reason;

        return new BasicProperties
        {
            Persistent = true,
            ContentType = ContentType ?? MessageSerializer.ContentType,
            MessageId = MessageId,
            Type = MessageType,
            CorrelationId = CorrelationId,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = amqpHeaders
        };
    }

    /// <summary>
    /// A settlement that arrives after the connection dropped cannot succeed and does not need to:
    /// the broker has already put the delivery back. Anything else is a real failure.
    /// </summary>
    private async Task SettleAsync(ValueTask settlement)
    {
        try
        {
            await settlement;
        }
        catch (Exception exception) when (exception is AlreadyClosedException or ObjectDisposedException)
        {
            RabbitMqLog.SettlementFailed(context.Logger, context.Queue.LogicalName, exception);
        }
    }
}
