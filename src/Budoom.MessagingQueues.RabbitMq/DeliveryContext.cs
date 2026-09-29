using Budoom.MessagingQueues.RabbitMq.Topology;
using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Everything a delivery needs to settle itself, shared by all messages from one subscription.
/// </summary>
internal sealed class DeliveryContext(IChannel channel, QueueDefinition queue, MessagePublisher publisher, ILogger logger)
{
    public IChannel Channel { get; } = channel;

    public QueueDefinition Queue { get; } = queue;

    public MessagePublisher Publisher { get; } = publisher;

    public ILogger Logger { get; } = logger;
}
