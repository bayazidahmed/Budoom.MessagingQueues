namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// Collects the exchanges and queues the application uses. Features declare their own topology
/// from their own registration code via <c>builder.AddRabbitMqTopology(topology =&gt; …)</c>;
/// everything is declared once at startup, so publishing and consuming just name things.
/// </summary>
public sealed class TopologyBuilder
{
    private readonly Dictionary<string, ExchangeDefinition> exchanges = new(StringComparer.Ordinal);
    private readonly Dictionary<string, QueueTopologyBuilder> queues = new(StringComparer.Ordinal);

    /// <summary>Declares a durable exchange. Declaring the same name twice with the same type is a no-op.</summary>
    /// <param name="name">
    /// Exchange name. Name exchanges in kebab-case (<c>styleme-search</c>), queues in snake_case and
    /// routing keys dotted, so any name says what kind of object it is.
    /// </param>
    /// <param name="type">One of the <see cref="RabbitMQ.Client.ExchangeType"/> constants.</param>
    public TopologyBuilder Exchange(string name, string type)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(type);

        if (exchanges.TryGetValue(name, out var existing))
        {
            if (!string.Equals(existing.Type, type, StringComparison.Ordinal))
                throw new InvalidOperationException($"Exchange '{name}' is already declared as '{existing.Type}' and cannot be redeclared as '{type}'.");

            return this;
        }

        exchanges.Add(name, new ExchangeDefinition(name, type));

        return this;
    }

    /// <summary>
    /// Declares a durable quorum queue shared by every instance: deliveries are spread across
    /// whoever is consuming, so exactly one instance handles each message.
    /// </summary>
    /// <param name="name">
    /// Name the queue in snake_case. Routing keys are dotted because topic wildcards match on dot
    /// separated words; queue names have no such constraint, and keeping them visibly different
    /// stops the two from being confused in the management UI and in logs. The derived retry and
    /// error queues extend the name the same way, as <c>&lt;name&gt;_retry_15s</c> and <c>&lt;name&gt;_error</c>.
    /// </param>
    public QueueTopologyBuilder Queue(string name) => AddQueue(name, transient: false);

    /// <summary>
    /// Declares a per-instance queue: exclusive, auto-deleting, and named with an instance suffix,
    /// so a message routed to it is delivered to every running instance rather than one of them.
    /// Use it to broadcast state that each instance keeps in memory. Nothing survives a restart,
    /// and delayed retry is not available.
    /// </summary>
    public QueueTopologyBuilder TransientQueue(string name) => AddQueue(name, transient: true);

    private QueueTopologyBuilder AddQueue(string name, bool transient)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (queues.ContainsKey(name))
            throw new InvalidOperationException($"Queue '{name}' is already declared.");

        var queue = new QueueTopologyBuilder(name, transient);

        queues.Add(name, queue);

        return queue;
    }

    internal TopologyRegistry Build(RabbitMqOptions option, string instanceId)
    {
        var builtQueues = queues.Values
            .Select(queue => queue.Build(option, instanceId))
            .ToArray();

        var missingExchange = builtQueues
            .SelectMany(queue => queue.Bindings, (queue, binding) => (queue, binding))
            .FirstOrDefault(pair => !exchanges.ContainsKey(pair.binding.Exchange));

        if (missingExchange.queue is not null)
            throw new InvalidOperationException(
                $"Queue '{missingExchange.queue.LogicalName}' is bound to exchange '{missingExchange.binding.Exchange}', which is not declared.");

        return new TopologyRegistry([.. exchanges.Values], builtQueues);
    }
}
