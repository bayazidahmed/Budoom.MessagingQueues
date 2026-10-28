using Budoom.MessagingQueues.RabbitMq.Topology;
using RabbitMQ.Client;
using System.Diagnostics;

namespace Budoom.MessagingQueues.RabbitMq;

internal sealed class MessagePublisher(
    ConnectionProvider connectionProvider,
    PublisherChannelPool channelPool,
    MessageSerializer messageSerializer,
    TopologyRegistry topologyRegistry,
    ILogger<MessagePublisher> logger) : IMessagePublisher
{
    /// <summary>The default exchange routes by queue name.</summary>
    public const string DefaultExchange = "";

    public Task SendAsync<TMessage>(
        string queue,
        TMessage message,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default) =>
        PublishAsync(DefaultExchange, topologyRegistry.ResolveQueueName(queue), message, headers, cancellationToken);

    public async Task PublishAsync<TMessage>(
        string topic,
        string routingKey,
        TMessage message,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        if (!connectionProvider.IsEnabled)
        {
            RabbitMqLog.PublishSkipped(logger, topic, routingKey);

            return;
        }

        var amqpHeaders = AmqpHeaders.ToAmqp(headers);
        amqpHeaders[MessageHeaderNames.MessageType] = MessageSerializer.TypeNameOf<TMessage>();

        var basicProperties = new BasicProperties
        {
            Persistent = true,
            ContentType = MessageSerializer.ContentType,
            MessageId = Guid.CreateVersion7().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Type = MessageSerializer.TypeNameOf<TMessage>(),
            Headers = amqpHeaders
        };

        await PublishRawAsync(topic, routingKey, messageSerializer.Serialize(message), basicProperties, topologyRegistry.RequiresBoundQueue(topic), cancellationToken);
    }

    /// <summary>
    /// Publishes an already-serialized body. Used when a message is moved without being rewritten —
    /// onto a retry queue or an error queue — so the original bytes reach their destination intact.
    /// </summary>
    internal async Task PublishRawAsync(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        BasicProperties basicProperties,
        bool mandatory,
        CancellationToken cancellationToken = default)
    {
        InjectTraceContext(basicProperties);

        await using var lease = await channelPool.RentAsync(cancellationToken);

        // Mandatory makes the broker return a message no queue receives, and with confirmation tracking the
        // client then fails the publish. Not mandatory, the broker drops it quietly instead.
        await lease.Channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: mandatory,
            basicProperties: basicProperties,
            body: body,
            cancellationToken: cancellationToken);

        RabbitMqLog.Published(logger, exchange, routingKey);
    }

    /// <summary>
    /// Writes the current trace context into the message headers (W3C <c>traceparent</c>/<c>tracestate</c>),
    /// so the consumer's <see cref="ReceivedMessage{TMessage}.StartActivity"/> continues the same trace.
    /// </summary>
    /// <remarks>
    /// Done here rather than left to RabbitMQ.Client, which only propagates when its own
    /// <c>RabbitMQ.Client.Publisher</c> source is being listened to. When it is, it overwrites these
    /// headers with its publish span, which is a child of the current activity — still the same trace.
    /// Covers retry and dead-letter copies too: a retried message continues the trace of the attempt
    /// that failed. With no current activity, headers copied from the original delivery are left as they are.
    /// </remarks>
    private static void InjectTraceContext(BasicProperties basicProperties)
    {
        if (Activity.Current is not { } current)
            return;

        var headers = basicProperties.Headers ??= new Dictionary<string, object?>(StringComparer.Ordinal);

        DistributedContextPropagator.Current.Inject(current, headers, static (carrier, fieldName, fieldValue) =>
        {
            if (carrier is IDictionary<string, object?> amqpHeaders)
                amqpHeaders[fieldName] = fieldValue;
        });
    }
}
