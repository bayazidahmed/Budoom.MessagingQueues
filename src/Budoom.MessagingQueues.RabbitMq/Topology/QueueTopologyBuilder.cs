namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// Fluent configuration for one queue. Obtained from <see cref="TopologyBuilder.Queue"/> or
/// <see cref="TopologyBuilder.TransientQueue"/>.
/// </summary>
public sealed class QueueTopologyBuilder
{
    private readonly List<BindingDefinition> bindings = [];
    private readonly bool transient;

    private QueueType type;
    private int? prefetchCount;
    private IReadOnlyList<TimeSpan>? retryDelays;
    private bool retryDisabled;

    internal QueueTopologyBuilder(string logicalName, bool transient)
    {
        LogicalName = logicalName;
        this.transient = transient;
        type = transient ? QueueType.Classic : QueueType.Quorum;
        retryDisabled = transient;
    }

    internal string LogicalName { get; }

    /// <summary>Binds the queue to an exchange. A queue with no binding is only reachable by name through <c>SendAsync</c>.</summary>
    public QueueTopologyBuilder BindTo(string exchange, string routingKey)
    {
        bindings.Add(new BindingDefinition(exchange, routingKey));

        return this;
    }

    /// <summary>Overrides the default prefetch, which caps how many deliveries a consumer holds unacknowledged.</summary>
    public QueueTopologyBuilder WithPrefetch(int prefetchCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(prefetchCount, 1);

        this.prefetchCount = prefetchCount;

        return this;
    }

    /// <summary>Overrides the configured default backoff schedule. One retry queue is declared per distinct delay.</summary>
    public QueueTopologyBuilder WithRetry(params TimeSpan[] delays)
    {
        ArgumentNullException.ThrowIfNull(delays);
        ArgumentOutOfRangeException.ThrowIfZero(delays.Length);

        foreach (var delay in delays)
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(delay, TimeSpan.Zero, nameof(delays));

        if (transient)
            throw new InvalidOperationException($"Queue '{LogicalName}' is transient, so it cannot have durable retry queues.");

        retryDelays = delays;
        retryDisabled = false;

        return this;
    }

    /// <summary>Opts out of delayed retry. <c>RetryAsync</c> then throws for messages from this queue.</summary>
    public QueueTopologyBuilder WithoutRetry()
    {
        retryDelays = null;
        retryDisabled = true;

        return this;
    }

    /// <summary>Declares a classic queue instead of a quorum queue. Durable work queues should stay quorum.</summary>
    public QueueTopologyBuilder AsClassic()
    {
        type = QueueType.Classic;

        return this;
    }

    internal QueueDefinition Build(RabbitMqOptions option, string instanceId)
    {
        RetryPolicy? retry = null;

        if (!retryDisabled)
        {
            var delays = retryDelays ?? option.EffectiveRetryDelays;

            if (delays.Count > 0)
                retry = new RetryPolicy([.. delays.Distinct().OrderBy(d => d)]);
        }

        return new QueueDefinition
        {
            LogicalName = LogicalName,
            Name = transient ? $"{LogicalName}_{instanceId}" : LogicalName,
            Type = type,
            Durable = !transient,
            Exclusive = transient,
            AutoDelete = transient,
            Bindings = bindings,
            PrefetchCount = prefetchCount,
            Retry = retry
        };
    }
}
