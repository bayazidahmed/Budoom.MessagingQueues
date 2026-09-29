using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq.Tests;

/// <summary>
/// End-to-end against a real broker. Skipped unless RABBITMQ_TEST_CONNECTION is set to an AMQP URI,
/// e.g. <c>amqp://guest:guest@localhost:5672/</c> (<c>docker run -p 5672:5672 rabbitmq:4</c>).
/// </summary>
public sealed class BrokerIntegrationTests
{
    private sealed record WorkMessage(int Id, string Text);

    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable("RABBITMQ_TEST_CONNECTION");

    [Fact]
    public async Task Publish_consume_retry_and_dead_letter_round_trip()
    {
        Assert.SkipWhen(ConnectionString is null, "RABBITMQ_TEST_CONNECTION is not set.");

        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var exchange = $"it-events-{suffix}";
        var workQueue = $"it_work_{suffix}";

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessagingQueues:RabbitMq:ConnectionString"] = ConnectionString,
            ["MessagingQueues:RabbitMq:ConsumerRecoveryInterval"] = "00:00:01"
        });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();
        builder.AddRabbitMqTopology(topology =>
        {
            topology.Exchange(exchange, ExchangeType.Topic);
            topology.Queue(workQueue).BindTo(exchange, "work.#").WithRetry(TimeSpan.FromSeconds(1));
            topology.TransientQueue("it_replies");
        });

        using var host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            await WaitForHealthyAsync(host, cancellationToken);

            var publisher = host.Services.GetRequiredService<IMessagePublisher>();
            var consumer = host.Services.GetRequiredService<IMessageConsumer>();

            await publisher.PublishAsync(exchange, "work.created", new WorkMessage(7, "hello"), new Dictionary<string, string> { ["tenant"] = "t1" }, cancellationToken);

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));

            var deliveries = 0;

            await foreach (var message in consumer.ConsumeAsync<WorkMessage>(workQueue, timeout.Token))
            {
                deliveries++;

                Assert.Equal(new WorkMessage(7, "hello"), message.Message);
                Assert.Equal("t1", message.Headers["tenant"]);
                Assert.Equal(deliveries, message.Attempt);
                Assert.Equal(2, message.MaxAttempts);

                var outcome = await message.RetryAsync(reason: "testing", cancellationToken: cancellationToken);

                if (deliveries == 1)
                {
                    Assert.Equal(RetryOutcome.Scheduled, outcome);
                    continue;
                }

                Assert.Equal(RetryOutcome.DeadLettered, outcome);
                break;
            }

            Assert.Equal(2, deliveries);

            // Reply to this instance's transient queue through its broker name.
            var replyTo = host.Services.GetRequiredService<IQueueNameResolver>().GetBrokerName("it_replies");
            var replies = consumer.ConsumeAsync<WorkMessage>("it_replies", timeout.Token).GetAsyncEnumerator(timeout.Token);
            var next = replies.MoveNextAsync();

            // The transient queue is declared when the consumer subscribes, so wait for it before sending.
            await WaitForQueueAsync(replyTo, cancellationToken);
            await publisher.SendAsync(replyTo, new WorkMessage(8, "reply"), cancellationToken: cancellationToken);

            Assert.True(await next);
            Assert.Equal(8, replies.Current.Message!.Id);
            await replies.Current.AckAsync(cancellationToken);
            await replies.DisposeAsync();

            Assert.Equal(1u, await MessageCountAsync($"{workQueue}_error", cancellationToken));
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await DeleteAsync(exchange, [workQueue, $"{workQueue}_error", $"{workQueue}_retry_1s"]);
        }
    }

    private static async Task WaitForHealthyAsync(IHost host, CancellationToken cancellationToken)
    {
        var healthCheckService = host.Services.GetRequiredService<HealthCheckService>();

        for (var i = 0; i < 50; i++)
        {
            var report = await healthCheckService.CheckHealthAsync(cancellationToken);
            if (report.Status == HealthStatus.Healthy)
                return;

            await Task.Delay(200, cancellationToken);
        }

        Assert.Fail("The broker never became healthy.");
    }

    private static async Task WaitForQueueAsync(string queue, CancellationToken cancellationToken)
    {
        for (var i = 0; i < 50; i++)
        {
            try
            {
                await MessageCountAsync(queue, cancellationToken);
                return;
            }
            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException)
            {
                await Task.Delay(200, cancellationToken);
            }
        }
    }

    private static async Task<uint> MessageCountAsync(string queue, CancellationToken cancellationToken)
    {
        await using var connection = await new ConnectionFactory { Uri = new Uri(ConnectionString!) }.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        return (await channel.QueueDeclarePassiveAsync(queue, cancellationToken)).MessageCount;
    }

    private static async Task DeleteAsync(string exchange, string[] queues)
    {
        await using var connection = await new ConnectionFactory { Uri = new Uri(ConnectionString!) }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        foreach (var queue in queues)
            await channel.QueueDeleteAsync(queue);

        await channel.ExchangeDeleteAsync(exchange);
    }
}
