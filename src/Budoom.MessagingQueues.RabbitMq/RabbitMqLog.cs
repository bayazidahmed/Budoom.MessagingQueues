namespace Budoom.MessagingQueues.RabbitMq;

internal static partial class RabbitMqLog
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Opened RabbitMQ {Role} connection to {Host}:{Port} (virtual host '{VirtualHost}')")]
    public static partial void ConnectionOpened(ILogger logger, string role, string host, int port, string virtualHost);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ {Role} connection closed: {Reason}")]
    public static partial void ConnectionClosed(ILogger logger, string role, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Closed RabbitMQ {Role} connection")]
    public static partial void ConnectionClosedByApplication(ILogger logger, string role);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ {Role} connection is blocked by the broker: {Reason}")]
    public static partial void ConnectionBlocked(ILogger logger, string role, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "RabbitMQ {Role} connection is unblocked")]
    public static partial void ConnectionUnblocked(ILogger logger, string role);

    [LoggerMessage(Level = LogLevel.Information, Message = "Provisioned RabbitMQ topology: {ExchangeCount} exchange(s), {QueueCount} queue(s)")]
    public static partial void TopologyProvisioned(ILogger logger, int exchangeCount, int queueCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to provision RabbitMQ topology, retrying in {Delay}")]
    public static partial void TopologyProvisioningFailed(ILogger logger, TimeSpan delay, Exception exception);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Published message to exchange '{Exchange}' with routing key '{RoutingKey}'")]
    public static partial void Published(ILogger logger, string exchange, string routingKey);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Publishing is disabled, dropped message for exchange '{Exchange}' with routing key '{RoutingKey}'")]
    public static partial void PublishSkipped(ILogger logger, string exchange, string routingKey);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Failed to dispose a publishing channel")]
    public static partial void ChannelDisposeFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message returned as unroutable from exchange '{Exchange}' with routing key '{RoutingKey}': {ReplyCode} {ReplyText}")]
    public static partial void Returned(ILogger logger, string exchange, string routingKey, int replyCode, string replyText);

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming queue '{Queue}' with prefetch {PrefetchCount}")]
    public static partial void ConsumerStarted(ILogger logger, string queue, int prefetchCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Consumer for queue '{Queue}' stopped, reconnecting in {Delay}")]
    public static partial void ConsumerInterrupted(ILogger logger, string queue, TimeSpan delay, Exception? exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to deserialize a message from queue '{Queue}' into {MessageType}")]
    public static partial void DeserializationFailed(ILogger logger, string queue, string messageType, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Scheduled retry {Attempt} of {MaxAttempts} for a message from queue '{Queue}' in {Delay}: {Reason}")]
    public static partial void RetryScheduled(ILogger logger, string queue, int attempt, int maxAttempts, TimeSpan delay, string? reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Exhausted {MaxAttempts} attempts for a message from queue '{Queue}', moved it to '{ErrorQueue}': {Reason}")]
    public static partial void DeadLettered(ILogger logger, string queue, int maxAttempts, string errorQueue, string? reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not settle a delivery from queue '{Queue}'; the connection most likely dropped and the message will be redelivered")]
    public static partial void SettlementFailed(ILogger logger, string queue, Exception exception);
}
