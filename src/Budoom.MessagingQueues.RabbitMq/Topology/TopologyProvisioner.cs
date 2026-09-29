using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// Declares the registered topology on the broker: exchanges, queues, their bindings, and the
/// retry and error queues that back delayed retry.
/// </summary>
/// <remarks>
/// Retry works without any broker plugin. Each configured delay gets its own queue that no one
/// consumes, holding messages for <c>x-message-ttl</c> and dead-lettering them back to the original
/// queue when they expire. So a retry is: republish onto the delay queue, wait, get redelivered.
/// </remarks>
internal sealed class TopologyProvisioner(
    ConnectionProvider connectionProvider,
    TopologyRegistry topologyRegistry,
    ILogger<TopologyProvisioner> logger)
{
    private readonly TaskCompletionSource provisioned = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>True once the topology has been declared on the broker since startup.</summary>
    public bool IsProvisioned => provisioned.Task.IsCompleted;

    /// <summary>
    /// Completes once the topology has been declared, so consumers do not subscribe to queues that do
    /// not exist yet and fail noisily on every startup.
    /// </summary>
    public Task WaitUntilProvisionedAsync(CancellationToken cancellationToken) => provisioned.Task.WaitAsync(cancellationToken);

    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await connectionProvider.GetPublisherConnectionAsync(cancellationToken);

        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken);

        foreach (var exchange in topologyRegistry.Exchanges)
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchange.Name,
                type: exchange.Type,
                durable: exchange.Durable,
                autoDelete: exchange.AutoDelete,
                cancellationToken: cancellationToken);
        }

        // Exclusive queues belong to the connection that declares them, so they are left to the
        // consumer to declare on its own channel when it subscribes.
        foreach (var queue in topologyRegistry.Queues.Where(queue => !queue.Exclusive))
        {
            await DeclareQueueAsync(channel, queue, cancellationToken);
        }

        provisioned.TrySetResult();

        RabbitMqLog.TopologyProvisioned(logger, topologyRegistry.Exchanges.Count, topologyRegistry.Queues.Count);
    }

    public static async Task DeclareQueueAsync(IChannel channel, QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        var arguments = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            [Headers.XQueueType] = QueueTypeArgument(queue.Type)
        };

        if (queue.ErrorQueueName is { } errorQueueName)
        {
            await channel.QueueDeclareAsync(
                queue: errorQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object?>(StringComparer.Ordinal) { [Headers.XQueueType] = QueueTypeArgument(queue.Type) },
                cancellationToken: cancellationToken);

            // Anything rejected outright, or dropped by the queue's own delivery limit, is kept here
            // rather than lost. Routing through the default exchange needs no extra exchange.
            arguments[Headers.XDeadLetterExchange] = MessagePublisher.DefaultExchange;
            arguments[Headers.XDeadLetterRoutingKey] = errorQueueName;
        }

        await channel.QueueDeclareAsync(
            queue: queue.Name,
            durable: queue.Durable,
            exclusive: queue.Exclusive,
            autoDelete: queue.AutoDelete,
            arguments: arguments,
            cancellationToken: cancellationToken);

        if (queue.Retry is { } retry)
        {
            foreach (var delay in retry.Delays)
            {
                await channel.QueueDeclareAsync(
                    queue: queue.RetryQueueName(delay),
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        [Headers.XQueueType] = QueueTypeArgument(queue.Type),
                        [Headers.XMessageTTL] = (int)delay.TotalMilliseconds,
                        [Headers.XDeadLetterExchange] = MessagePublisher.DefaultExchange,
                        [Headers.XDeadLetterRoutingKey] = queue.Name
                    },
                    cancellationToken: cancellationToken);
            }
        }

        foreach (var binding in queue.Bindings)
        {
            await channel.QueueBindAsync(
                queue: queue.Name,
                exchange: binding.Exchange,
                routingKey: binding.RoutingKey,
                cancellationToken: cancellationToken);
        }
    }

    private static string QueueTypeArgument(QueueType queueType) => queueType switch
    {
        QueueType.Quorum => "quorum",
        QueueType.Classic => "classic",
        _ => throw new ArgumentOutOfRangeException(nameof(queueType), queueType, "Unknown queue type.")
    };
}
