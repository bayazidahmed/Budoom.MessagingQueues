using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Owns the long-lived connections. Publishing and consuming get separate connections on purpose:
/// a broker that blocks a connection under memory or disk pressure blocks publishers, and sharing
/// one connection would stall consumers that are trying to drain the very queues causing it.
/// </summary>
internal sealed class ConnectionProvider(IOptions<RabbitMqOptions> options, ILogger<ConnectionProvider> logger) : IAsyncDisposable
{
    private const string PublisherRole = "publisher";
    private const string ConsumerRole = "consumer";

    private readonly SemaphoreSlim gate = new(1, 1);

    private IConnection? publisherConnection;
    private IConnection? consumerConnection;
    private volatile bool publisherBlocked;
    private volatile bool consumerBlocked;
    private bool disposed;

    public bool IsEnabled => options.Value.Enabled;

    /// <summary>True while the broker is refusing publishes on either connection (memory or disk alarm).</summary>
    public bool IsBlocked => publisherBlocked || consumerBlocked;

    public ValueTask<IConnection> GetPublisherConnectionAsync(CancellationToken cancellationToken = default) =>
        GetConnectionAsync(PublisherRole, cancellationToken);

    public ValueTask<IConnection> GetConsumerConnectionAsync(CancellationToken cancellationToken = default) =>
        GetConnectionAsync(ConsumerRole, cancellationToken);

    private async ValueTask<IConnection> GetConnectionAsync(string role, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (!options.Value.Enabled)
            throw new InvalidOperationException($"RabbitMQ is disabled. Set {RabbitMqOptions.SectionName}:{nameof(RabbitMqOptions.Enabled)} to true to connect.");

        if (Current(role) is { IsOpen: true } open)
            return open;

        await gate.WaitAsync(cancellationToken);

        try
        {
            if (Current(role) is { IsOpen: true } opened)
                return opened;

            if (Current(role) is { } stale)
                await SafeDisposeAsync(stale);

            var connection = await CreateConnectionAsync(role, cancellationToken);

            if (role == PublisherRole)
                publisherConnection = connection;
            else
                consumerConnection = connection;

            return connection;
        }
        finally
        {
            gate.Release();
        }
    }

    private IConnection? Current(string role) => role == PublisherRole ? publisherConnection : consumerConnection;

    private async Task<IConnection> CreateConnectionAsync(string role, CancellationToken cancellationToken)
    {
        var option = options.Value;

        var connectionFactory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = true,
            // Consumers are re-established by MessageConsumer itself after a drop. Letting the client
            // also restore them would risk two consumers on the same queue from one process.
            TopologyRecoveryEnabled = role == PublisherRole,
            NetworkRecoveryInterval = option.NetworkRecoveryInterval
        };

        if (!string.IsNullOrWhiteSpace(option.ConnectionString))
        {
            // Sets host, port, virtual host and credentials, and turns TLS on for amqps.
            connectionFactory.Uri = new Uri(option.ConnectionString);
        }
        else
        {
            connectionFactory.HostName = option.Host;
            connectionFactory.VirtualHost = option.VirtualHost;
            connectionFactory.UserName = option.UserName;
            connectionFactory.Password = option.Password;

            if (option.UseTls)
            {
                connectionFactory.Ssl = new SslOption
                {
                    Enabled = true,
                    ServerName = option.Host
                };
            }

            // Left at the client's default otherwise, which picks 5671 for TLS and 5672 without.
            if (option.Port is { } port)
                connectionFactory.Port = port;
        }

        var clientProvidedName = $"{option.ClientProvidedName}:{role}:{Environment.ProcessId}";

        var connection = await connectionFactory.CreateConnectionAsync(clientProvidedName, cancellationToken);

        connection.ConnectionShutdownAsync += (_, shutdownEventArgs) =>
        {
            // Closing it ourselves on shutdown is expected; anything else is worth a warning.
            if (shutdownEventArgs.Initiator == ShutdownInitiator.Application)
                RabbitMqLog.ConnectionClosedByApplication(logger, role);
            else
                RabbitMqLog.ConnectionClosed(logger, role, shutdownEventArgs.ReplyText);

            return Task.CompletedTask;
        };

        connection.ConnectionBlockedAsync += (_, blockedEventArgs) =>
        {
            SetBlocked(role, true);

            RabbitMqLog.ConnectionBlocked(logger, role, blockedEventArgs.Reason);

            return Task.CompletedTask;
        };

        connection.ConnectionUnblockedAsync += (_, _) =>
        {
            SetBlocked(role, false);

            RabbitMqLog.ConnectionUnblocked(logger, role);

            return Task.CompletedTask;
        };

        RabbitMqLog.ConnectionOpened(logger, role, connection.Endpoint.HostName, connection.Endpoint.Port, connectionFactory.VirtualHost);

        return connection;
    }

    private void SetBlocked(string role, bool blocked)
    {
        if (role == PublisherRole)
            publisherBlocked = blocked;
        else
            consumerBlocked = blocked;
    }

    private async Task SafeDisposeAsync(IConnection connection)
    {
        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception exception)
        {
            RabbitMqLog.ConnectionClosed(logger, "stale", exception.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;

        if (publisherConnection is not null)
            await SafeDisposeAsync(publisherConnection);

        if (consumerConnection is not null)
            await SafeDisposeAsync(consumerConnection);

        gate.Dispose();
    }
}
