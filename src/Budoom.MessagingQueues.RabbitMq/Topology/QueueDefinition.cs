namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// A queue as declared at startup. <see cref="LogicalName"/> is what application code passes to
/// the publisher and consumer; <see cref="Name"/> is what exists on the broker. They differ only
/// for transient per-instance queues, whose real name carries an instance suffix.
/// </summary>
internal sealed class QueueDefinition
{
    public required string LogicalName { get; init; }

    public required string Name { get; init; }

    public required QueueType Type { get; init; }

    public required bool Durable { get; init; }

    public required bool Exclusive { get; init; }

    public required bool AutoDelete { get; init; }

    public required IReadOnlyList<BindingDefinition> Bindings { get; init; }

    public required int? PrefetchCount { get; init; }

    /// <summary>Null when the queue opts out of delayed retry, which transient queues always do.</summary>
    public required RetryPolicy? Retry { get; init; }

    /// <summary>
    /// Queue that rejected and exhausted messages land in. Null when the queue has no error queue,
    /// in which case a rejected message is simply dropped.
    /// </summary>
    public string? ErrorQueueName => HasErrorQueue ? $"{Name}_error" : null;

    /// <summary>Transient queues are torn down with the connection, so there is nothing durable to dead-letter into.</summary>
    public bool HasErrorQueue => Durable && !Exclusive && !AutoDelete;

    public string RetryQueueName(TimeSpan delay) => $"{Name}_retry_{(int)delay.TotalSeconds}s";
}
