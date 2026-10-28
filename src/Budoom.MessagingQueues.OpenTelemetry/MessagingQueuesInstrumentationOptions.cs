namespace Budoom.MessagingQueues.OpenTelemetry;

/// <summary>What <c>AddMessagingQueuesInstrumentation</c> records besides the library's own process spans.</summary>
public sealed class MessagingQueuesInstrumentationOptions
{
    /// <summary>
    /// Record the broker client's publish spans (exchange, routing key, body size, publish time).
    /// On by default: they show how long a publish took inside the request that made it.
    /// </summary>
    public bool IncludeBrokerPublishSpans { get; set; } = true;

    /// <summary>
    /// Record the broker client's deliver spans. Off by default: a deliver span only covers handing the
    /// message to the consume loop, not handling it, so it mostly adds noise next to the process span
    /// that <see cref="ReceivedMessage{TMessage}.StartActivity"/> records.
    /// </summary>
    public bool IncludeBrokerDeliverSpans { get; set; }
}
