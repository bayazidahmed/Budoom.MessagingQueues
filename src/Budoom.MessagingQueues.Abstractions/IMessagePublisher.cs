namespace Budoom.MessagingQueues;

/// <summary>
/// Publishes messages. Inject it wherever something needs to be sent. Implementations complete the
/// call only once the broker has accepted the message.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Sends straight to one queue. Use it for point-to-point work where exactly one consumer
    /// should pick the message up, and for replies to a <c>ReplyTo</c> queue.
    /// </summary>
    /// <param name="queue">
    /// Logical queue name as declared in the topology. A name that is not declared is used as the
    /// broker name unchanged, so a queue owned by another system (or another instance's reply
    /// queue) can still be addressed.
    /// </param>
    /// <param name="message">The body, serialized as JSON.</param>
    /// <param name="headers">Optional application headers.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task SendAsync<TMessage>(
        string queue,
        TMessage message,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes to a topic and lets its subscriptions decide which queues receive it: one queue,
    /// several, or none.
    /// </summary>
    /// <param name="topic">The fan-out destination: a RabbitMQ exchange, a Service Bus topic, a Kafka topic.</param>
    /// <param name="routingKey">What subscriptions filter on: a RabbitMQ routing key, a Service Bus subject, a Kafka key.</param>
    /// <param name="message">The body, serialized as JSON.</param>
    /// <param name="headers">Optional application headers.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync<TMessage>(
        string topic,
        string routingKey,
        TMessage message,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}
