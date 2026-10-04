namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// Options for one exchange, returned by <see cref="TopologyBuilder.Exchange"/>. The topology methods are
/// repeated here so declarations can still be chained one after another.
/// </summary>
public sealed class ExchangeTopologyBuilder
{
    private readonly TopologyBuilder topology;

    internal ExchangeTopologyBuilder(TopologyBuilder topology, string name)
    {
        this.topology = topology;
        Name = name;
    }

    /// <summary>Exchange name.</summary>
    public string Name { get; }

    /// <summary>
    /// Lets a publish to this exchange succeed when no queue is bound to it, or none of its bindings matches the
    /// routing key. The broker drops such a message instead of returning it, so nothing is thrown and nothing is
    /// logged. Use it for an exchange whose subscribers may not exist yet, where a message nobody listens for is
    /// not an error. Without it, an unroutable publish fails.
    /// </summary>
    /// <remarks>Declaring the same exchange again without this does not take it away.</remarks>
    public ExchangeTopologyBuilder WithoutBoundQueue()
    {
        topology.AllowWithoutBoundQueue(Name);

        return this;
    }

    /// <inheritdoc cref="TopologyBuilder.Exchange"/>
    public ExchangeTopologyBuilder Exchange(string name, string type) => topology.Exchange(name, type);

    /// <inheritdoc cref="TopologyBuilder.Queue"/>
    public QueueTopologyBuilder Queue(string name) => topology.Queue(name);

    /// <inheritdoc cref="TopologyBuilder.TransientQueue"/>
    public QueueTopologyBuilder TransientQueue(string name) => topology.TransientQueue(name);
}
