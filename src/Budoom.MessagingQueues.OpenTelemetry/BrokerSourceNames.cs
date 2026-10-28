namespace Budoom.MessagingQueues.OpenTelemetry;

/// <summary>
/// Activity source names of the broker client libraries. Written out rather than taken from the
/// clients' own constants so this package stays broker-neutral and pulls in no client library. A test
/// compares each one with the client's constant, so a rename upstream fails the build here.
/// A source for a broker the app does not use is simply never created, so listing it costs nothing.
/// </summary>
internal static class BrokerSourceNames
{
    /// <summary><c>RabbitMQ.Client.RabbitMQActivitySource.PublisherSourceName</c>.</summary>
    public const string RabbitMqPublisher = "RabbitMQ.Client.Publisher";

    /// <summary><c>RabbitMQ.Client.RabbitMQActivitySource.SubscriberSourceName</c>.</summary>
    public const string RabbitMqSubscriber = "RabbitMQ.Client.Subscriber";

    public static readonly string[] Publish = [RabbitMqPublisher];

    public static readonly string[] Deliver = [RabbitMqSubscriber];
}
