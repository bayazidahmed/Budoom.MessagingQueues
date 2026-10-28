using System.Diagnostics;

namespace Budoom.MessagingQueues;

/// <summary>
/// Tracing for message handling. Register <see cref="ActivitySourceName"/> with your tracing setup —
/// for OpenTelemetry, <c>.AddSource(MessagingDiagnostics.ActivitySourceName)</c> — and call
/// <see cref="ReceivedMessage{TMessage}.StartActivity"/> for each delivery to get one span per handled
/// message, joined to the trace of whoever published it.
/// </summary>
/// <remarks>
/// Only <c>System.Diagnostics</c> is used, so no OpenTelemetry package is required here. Trace
/// context travels in the W3C <c>traceparent</c> / <c>tracestate</c> message headers, read and written
/// through <see cref="DistributedContextPropagator.Current"/>.
/// </remarks>
public static class MessagingDiagnostics
{
    /// <summary>Name of the <see cref="ActivitySource"/> the messaging packages create activities on.</summary>
    public const string ActivitySourceName = "Budoom.MessagingQueues";

    /// <summary>How the delivery was settled: <c>ack</c>, <c>nack</c>, <c>reject</c>, <c>retry</c> or <c>dead_letter</c>.</summary>
    public const string OutcomeTagName = "budoom.messaging.outcome";

    /// <summary>The reason passed to <see cref="ReceivedMessage{TMessage}.RetryAsync"/>, when there was one.</summary>
    public const string ReasonTagName = "budoom.messaging.reason";

    /// <summary>Delivery number of the message, starting at 1.</summary>
    public const string AttemptTagName = "budoom.messaging.attempt";

    internal static readonly ActivitySource Source =
        new(ActivitySourceName, typeof(MessagingDiagnostics).Assembly.GetName().Version?.ToString());

    /// <summary>
    /// Starts a consumer span named <c>process {queue}</c>. Its parent is the trace context found in the
    /// message headers; without one it falls back to the ambient <see cref="Activity.Current"/>, if any.
    /// </summary>
    internal static Activity? StartProcessActivity(
        string queue,
        IReadOnlyDictionary<string, string> headers,
        string? messagingSystem,
        string? messageId,
        string? correlationId,
        int bodySize,
        int attempt)
    {
        if (!Source.HasListeners())
            return null;

        var activity = Source.StartActivity(string.Concat("process ", queue), ActivityKind.Consumer, ExtractParent(headers));

        if (activity is null || !activity.IsAllDataRequested)
            return activity;

        // OpenTelemetry messaging semantic conventions, plus the library's own attempt counter.
        activity.SetTag("messaging.system", messagingSystem);
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.operation.name", "process");
        activity.SetTag("messaging.destination.name", queue);
        activity.SetTag("messaging.message.id", messageId);
        activity.SetTag("messaging.message.conversation_id", correlationId);
        activity.SetTag("messaging.message.body.size", bodySize);
        activity.SetTag(AttemptTagName, attempt);

        return activity;
    }

    private static ActivityContext ExtractParent(IReadOnlyDictionary<string, string> headers)
    {
        DistributedContextPropagator.Current.ExtractTraceIdAndState(
            headers,
            static (object? carrier, string fieldName, out string? fieldValue, out IEnumerable<string>? fieldValues) =>
            {
                fieldValues = null;
                fieldValue = carrier is IReadOnlyDictionary<string, string> values && values.TryGetValue(fieldName, out var value)
                    ? value
                    : null;
            },
            out var traceParent,
            out var traceState);

        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var context) ? context : default;
    }
}
