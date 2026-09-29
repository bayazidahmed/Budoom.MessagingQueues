using Budoom.MessagingQueues;
using Budoom.MessagingQueues.RabbitMq;
using Budoom.MessagingQueues.RabbitMq.Topology;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

// Deliberately in the hosting namespace, like the Aspire client integrations, so Program.cs and
// feature registration code need no extra using.
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace Microsoft.Extensions.Hosting;
#pragma warning restore IDE0130

/// <summary>Registers the RabbitMQ implementation of <see cref="IMessagePublisher"/> and <see cref="IMessageConsumer"/>.</summary>
public static class RabbitMqHostApplicationBuilderExtensions
{
    /// <summary>
    /// Per-process suffix for transient queue names, so each instance gets its own broadcast queue
    /// rather than competing for one.
    /// </summary>
    private static readonly string InstanceId = CreateInstanceId();

    /// <summary>
    /// Adds RabbitMQ messaging: <see cref="IMessagePublisher"/>, <see cref="IMessageConsumer"/>,
    /// <see cref="IQueueNameResolver"/>, a hosted service that declares the topology at startup, and
    /// (unless turned off) a <c>rabbitmq</c> health check. Settings are bound from the
    /// <c>MessagingQueues:RabbitMq</c> configuration section; every setting has a default.
    /// </summary>
    /// <param name="builder">The host builder.</param>
    /// <param name="configure">
    /// Applied after configuration binding — the place to set things that do not come from
    /// configuration, such as <see cref="RabbitMqOptions.JsonSerializerOptions"/>.
    /// </param>
    /// <remarks>Calling this more than once is safe: later calls only add their <paramref name="configure"/> callback.</remarks>
    public static IHostApplicationBuilder AddMessagingQueueRabbitMqInfrastructureServices(
        this IHostApplicationBuilder builder,
        Action<RabbitMqOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var services = builder.Services;

        GetOrAddTopologyBuilder(services);

        if (services.Any(descriptor => descriptor.ServiceType == typeof(ConnectionProvider)))
        {
            if (configure is not null)
                services.Configure(configure);

            return builder;
        }

        services
            .AddOptions<RabbitMqOptions>()
            .BindConfiguration(RabbitMqOptions.SectionName)
            .ValidateDataAnnotations()
            .Validate(
                option => !option.Enabled || string.IsNullOrWhiteSpace(option.ConnectionString) || IsAmqpUri(option.ConnectionString),
                $"{RabbitMqOptions.SectionName}:{nameof(RabbitMqOptions.ConnectionString)} must be an absolute amqp:// or amqps:// URI.")
            .Validate(
                option => !option.Enabled || !string.IsNullOrWhiteSpace(option.ConnectionString) || !string.IsNullOrWhiteSpace(option.Host),
                $"{RabbitMqOptions.SectionName}:{nameof(RabbitMqOptions.Host)} is required when no {nameof(RabbitMqOptions.ConnectionString)} is set.")
            .Validate(
                option => option.RetryDelays is null || option.RetryDelays.All(delay => delay > TimeSpan.Zero),
                $"{RabbitMqOptions.SectionName}:{nameof(RabbitMqOptions.RetryDelays)} must only contain positive durations.")
            .Validate(
                option => option.ConsumerRecoveryInterval > TimeSpan.Zero && option.NetworkRecoveryInterval > TimeSpan.Zero,
                $"{RabbitMqOptions.SectionName}:{nameof(RabbitMqOptions.ConsumerRecoveryInterval)} and {nameof(RabbitMqOptions.NetworkRecoveryInterval)} must be positive.")
            .ValidateOnStart();

        // Registered after BindConfiguration so code wins over configuration.
        if (configure is not null)
            services.Configure(configure);

        services.TryAddSingleton<MessageSerializer>();

        services.TryAddSingleton<ConnectionProvider>();
        services.TryAddSingleton<PublisherChannelPool>();
        services.TryAddSingleton<TopologyProvisioner>();

        services.TryAddSingleton<MessagePublisher>();
        services.TryAddSingleton<IMessagePublisher>(serviceProvider => serviceProvider.GetRequiredService<MessagePublisher>());
        services.TryAddSingleton<IMessageConsumer, MessageConsumer>();

        services.TryAddSingleton(serviceProvider => serviceProvider
            .GetRequiredService<TopologyBuilder>()
            .Build(serviceProvider.GetRequiredService<IOptions<RabbitMqOptions>>().Value, InstanceId));
        services.TryAddSingleton<IQueueNameResolver>(serviceProvider => serviceProvider.GetRequiredService<TopologyRegistry>());

        services.AddHostedService<RabbitMqHostedService>();

        // Whether to register the health check has to be known now, before options are built, so the
        // setting is resolved eagerly the same way the options will be: configuration, then callback.
        var registration = new RabbitMqOptions();
        builder.Configuration.GetSection(RabbitMqOptions.SectionName).Bind(registration);
        configure?.Invoke(registration);

        if (registration.HealthChecks)
            services.AddHealthChecks().AddCheck<RabbitMqHealthCheck>(RabbitMqHealthCheck.Name, tags: ["messaging", "rabbitmq"]);

        return builder;
    }

    /// <summary>
    /// Declares the exchanges and queues a feature owns. Call it from the feature's own registration
    /// code; every call adds to the same topology, in any order, before or after
    /// <see cref="AddMessagingQueueRabbitMqInfrastructureServices"/>.
    /// </summary>
    /// <param name="builder">The host builder.</param>
    /// <param name="configure">Declares exchanges and queues on the shared <see cref="TopologyBuilder"/>.</param>
    public static IHostApplicationBuilder AddRabbitMqTopology(this IHostApplicationBuilder builder, Action<TopologyBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        configure(GetOrAddTopologyBuilder(builder.Services));

        return builder;
    }

    private static TopologyBuilder GetOrAddTopologyBuilder(IServiceCollection services)
    {
        var registered = services.FirstOrDefault(descriptor => descriptor.ServiceType == typeof(TopologyBuilder));

        if (registered?.ImplementationInstance is TopologyBuilder existing)
            return existing;

        var topologyBuilder = new TopologyBuilder();

        services.AddSingleton(topologyBuilder);

        return topologyBuilder;
    }

    private static bool IsAmqpUri(string value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri)
        && (uri.Scheme.Equals("amqp", StringComparison.OrdinalIgnoreCase) || uri.Scheme.Equals("amqps", StringComparison.OrdinalIgnoreCase));

    private static string CreateInstanceId()
    {
        var raw = $"{Environment.MachineName}-{Environment.ProcessId}";

        var sanitized = string.Concat(raw.Where(character => char.IsAsciiLetterOrDigit(character) || character is '-'));

        return sanitized.ToLowerInvariant();
    }
}
