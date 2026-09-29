using Budoom.MessagingQueues.RabbitMq.Topology;
using Microsoft.Extensions.Options;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Connects and declares the topology at startup. It retries instead of failing the host: a broker
/// that is briefly unavailable should not stop the web app from serving, and consumers retry on the
/// same interval until their queues exist.
/// </summary>
internal sealed class RabbitMqHostedService(
    ConnectionProvider connectionProvider,
    TopologyProvisioner topologyProvisioner,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!connectionProvider.IsEnabled)
            return;

        var delay = options.Value.ConsumerRecoveryInterval;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await topologyProvisioner.ProvisionAsync(stoppingToken);

                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                RabbitMqLog.TopologyProvisioningFailed(logger, delay, exception);
            }

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
