namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// The resolved topology. Singleton, built once from <see cref="TopologyBuilder"/>, and the single
/// source of truth for what a logical queue name maps to on the broker.
/// </summary>
internal sealed class TopologyRegistry(IReadOnlyList<ExchangeDefinition> exchanges, IReadOnlyList<QueueDefinition> queues) : IQueueNameResolver
{
    private readonly Dictionary<string, QueueDefinition> queuesByLogicalName =
        queues.ToDictionary(queue => queue.LogicalName, StringComparer.Ordinal);

    public IReadOnlyList<ExchangeDefinition> Exchanges { get; } = exchanges;

    public IReadOnlyList<QueueDefinition> Queues { get; } = queues;

    public QueueDefinition GetQueue(string logicalName)
    {
        if (!queuesByLogicalName.TryGetValue(logicalName, out var queue))
            throw new InvalidOperationException(
                $"Queue '{logicalName}' is not declared. Declare it with AddRabbitMqTopology before publishing to or consuming from it.");

        return queue;
    }

    public string GetBrokerName(string queue) => GetQueue(queue).Name;

    /// <summary>
    /// Maps a logical queue name to the name on the broker. Unknown names pass through unchanged so
    /// that queues owned by another system can still be published to by name.
    /// </summary>
    public string ResolveQueueName(string logicalName) =>
        queuesByLogicalName.TryGetValue(logicalName, out var queue) ? queue.Name : logicalName;
}
