using Budoom.MessagingQueues.RabbitMq.Topology;
using RabbitMQ.Client;

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

        await PublishRawAsync(topic, routingKey, messageSerializer.Serialize(message), basicProperties, cancellationToken);
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
        CancellationToken cancellationToken = default)
    {
        await using var lease = await channelPool.RentAsync(cancellationToken);

        await lease.Channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: basicProperties,
            body: body,
            cancellationToken: cancellationToken);

        RabbitMqLog.Published(logger, exchange, routingKey);
    }
}
