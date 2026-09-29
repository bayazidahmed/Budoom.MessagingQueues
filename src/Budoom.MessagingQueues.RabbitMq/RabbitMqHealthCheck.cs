using Budoom.MessagingQueues.RabbitMq.Topology;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Healthy when the publishing connection is open and the topology has been declared; degraded
/// while the topology is still pending or the broker is blocking the connection; unhealthy when the
/// broker cannot be reached. Healthy when RabbitMQ is disabled, since nothing is expected to connect.
/// </summary>
internal sealed class RabbitMqHealthCheck(ConnectionProvider connectionProvider, TopologyProvisioner topologyProvisioner) : IHealthCheck
{
    public const string Name = "rabbitmq";

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!connectionProvider.IsEnabled)
            return HealthCheckResult.Healthy("RabbitMQ is disabled.");

        try
        {
            var connection = await connectionProvider.GetPublisherConnectionAsync(cancellationToken);

            if (!connection.IsOpen)
                return new HealthCheckResult(context.Registration.FailureStatus, "The RabbitMQ connection is closed.");
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, "Cannot connect to RabbitMQ.", exception);
        }

        if (connectionProvider.IsBlocked)
            return HealthCheckResult.Degraded("The broker is blocking the connection (memory or disk alarm).");

        if (!topologyProvisioner.IsProvisioned)
            return HealthCheckResult.Degraded("Connected, but the topology has not been declared yet.");

        return HealthCheckResult.Healthy();
    }
}
