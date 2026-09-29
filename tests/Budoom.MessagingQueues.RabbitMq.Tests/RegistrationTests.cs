using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text.Json;

namespace Budoom.MessagingQueues.RabbitMq.Tests;

public sealed class RegistrationTests
{
    private static HostApplicationBuilder CreateBuilder(Dictionary<string, string?> settings)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(settings);

        return builder;
    }

    [Fact]
    public void Options_have_defaults_and_bind_from_the_MessagingQueues_RabbitMq_section()
    {
        var builder = CreateBuilder(new()
        {
            ["MessagingQueues:RabbitMq:Host"] = "broker.example.com",
            ["MessagingQueues:RabbitMq:UseTls"] = "true",
            ["MessagingQueues:RabbitMq:RetryDelays:0"] = "00:00:10"
        });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();

        using var host = builder.Build();
        var options = host.Services.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal("broker.example.com", options.Host);
        Assert.True(options.UseTls);
        Assert.Equal("/", options.VirtualHost);
        Assert.Equal(8, options.PublisherChannelPoolSize);
        // Configured delays replace the defaults instead of being appended to them.
        Assert.Equal([TimeSpan.FromSeconds(10)], options.EffectiveRetryDelays);
    }

    [Fact]
    public void Configure_callback_runs_after_binding()
    {
        var json = new JsonSerializerOptions(JsonSerializerDefaults.General);
        var builder = CreateBuilder(new() { ["MessagingQueues:RabbitMq:Host"] = "from-config" });
        builder.AddMessagingQueueRabbitMqInfrastructureServices(options =>
        {
            options.Host = "from-code";
            options.JsonSerializerOptions = json;
        });

        using var host = builder.Build();
        var options = host.Services.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal("from-code", options.Host);
        Assert.Same(json, options.JsonSerializerOptions);
    }

    [Fact]
    public async Task Invalid_connection_string_fails_at_startup()
    {
        var builder = CreateBuilder(new() { ["MessagingQueues:RabbitMq:ConnectionString"] = "http://not-amqp" });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();

        using var host = builder.Build();

        await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Disabled_messaging_boots_without_a_broker_and_is_inert()
    {
        var builder = CreateBuilder(new() { ["MessagingQueues:RabbitMq:Enabled"] = "false" });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();
        // Topology can be declared before or after the call above.
        builder.AddRabbitMqTopology(topology =>
        {
            topology.Exchange("app-events", ExchangeType.Topic);
            topology.Queue("work").BindTo("app-events", "work.created");
            topology.TransientQueue("replies");
        });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();

        using var host = builder.Build();
        var cancellationToken = TestContext.Current.CancellationToken;
        await host.StartAsync(cancellationToken);

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("app-events", "work.created", new { Id = 1 }, cancellationToken: cancellationToken);
        await publisher.SendAsync("work", new { Id = 2 }, cancellationToken: cancellationToken);

        var consumer = host.Services.GetRequiredService<IMessageConsumer>();
        await foreach (var _ in consumer.ConsumeAsync<object>("work", cancellationToken))
            Assert.Fail("A disabled consumer should yield nothing.");

        var resolver = host.Services.GetRequiredService<IQueueNameResolver>();
        Assert.StartsWith("replies_", resolver.GetBrokerName("replies"), StringComparison.Ordinal);

        var health = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(cancellationToken);
        Assert.Equal(HealthStatus.Healthy, health.Entries["rabbitmq"].Status);

        // Registering twice must not add a second hosted service.
        Assert.Single(host.Services.GetServices<IHostedService>().OfType<RabbitMqHostedService>());

        await host.StopAsync(cancellationToken);
    }

    [Fact]
    public void Health_check_can_be_turned_off()
    {
        var builder = CreateBuilder(new() { ["MessagingQueues:RabbitMq:HealthChecks"] = "false" });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();

        using var host = builder.Build();
        var registrations = host.Services.GetService<IOptions<HealthCheckServiceOptions>>()?.Value.Registrations ?? [];

        Assert.DoesNotContain(registrations, registration => registration.Name == "rabbitmq");
    }

    [Fact]
    public async Task Unreachable_broker_reports_unhealthy()
    {
        var builder = CreateBuilder(new() { ["MessagingQueues:RabbitMq:ConnectionString"] = "amqp://guest:guest@127.0.0.1:1/" });
        builder.AddMessagingQueueRabbitMqInfrastructureServices();

        using var host = builder.Build();
        var health = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, health.Entries["rabbitmq"].Status);
    }
}
