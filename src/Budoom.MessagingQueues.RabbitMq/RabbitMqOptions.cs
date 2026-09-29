using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Text.Json;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Bound from the <c>MessagingQueues:RabbitMq</c> configuration section. Every setting has a default,
/// so a local broker needs no configuration at all and a hosted one usually only needs
/// <see cref="ConnectionString"/> (or <see cref="Host"/>, <see cref="UserName"/> and <see cref="Password"/>).
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>The configuration section the options are bound from.</summary>
    public const string SectionName = "MessagingQueues:RabbitMq";

    /// <summary>Default backoff schedule when <see cref="RetryDelays"/> is not configured.</summary>
    public static IReadOnlyList<TimeSpan> DefaultRetryDelays { get; } =
        [TimeSpan.FromSeconds(15), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5)];

    /// <summary>
    /// When false nothing connects: the topology is not provisioned, consumers stay idle and
    /// publishing is a no-op. Lets the app boot on a machine with no broker. Defaults to true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// AMQP URI, e.g. <c>amqps://user:password@host:5671/vhost</c>. When set it takes precedence over
    /// <see cref="Host"/>, <see cref="Port"/>, <see cref="VirtualHost"/>, <see cref="UserName"/>,
    /// <see cref="Password"/> and <see cref="UseTls"/>; the <c>amqps</c> scheme turns TLS on.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Broker host name. Defaults to <c>localhost</c>.</summary>
    public string Host { get; set; } = "localhost";

    /// <summary>Broker port. Defaults to 5672, or 5671 when <see cref="UseTls"/> is on.</summary>
    [Range(1, 65535)]
    public int? Port { get; set; }

    /// <summary>Virtual host. Defaults to <c>/</c>.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>Defaults to <c>guest</c>, which RabbitMQ only accepts from localhost.</summary>
    public string UserName { get; set; } = "guest";

    /// <summary>Defaults to <c>guest</c>. Keep real passwords in user secrets or environment variables.</summary>
    public string Password { get; set; } = "guest";

    /// <summary>Connect over TLS (amqps). Defaults to false.</summary>
    public bool UseTls { get; set; }

    /// <summary>
    /// Shown in the RabbitMQ management UI, with the role and process id appended so instances are
    /// distinguishable. Defaults to the entry assembly name.
    /// </summary>
    [Required]
    public string ClientProvidedName { get; set; } = Assembly.GetEntryAssembly()?.GetName().Name ?? "Budoom.MessagingQueues";

    /// <summary>
    /// Number of channels kept for publishing. A channel must never be used by two concurrent
    /// publishes, so this is the real publish concurrency limit. Defaults to 8.
    /// </summary>
    [Range(1, 128)]
    public int PublisherChannelPoolSize { get; set; } = 8;

    /// <summary>Unacknowledged deliveries allowed per consumer when the queue does not set its own. Defaults to 16.</summary>
    [Range(1, 65535)]
    public int DefaultPrefetchCount { get; set; } = 16;

    /// <summary>
    /// Default backoff schedule used by <c>RetryAsync</c>, for queues that do not set their own.
    /// One retry queue is declared per distinct delay, so keep the list short. Also caps attempts:
    /// a message is dead-lettered once it has been delivered <c>RetryDelays.Length + 1</c> times.
    /// Defaults to <see cref="DefaultRetryDelays"/> (15 s, 1 min, 5 min) when not set.
    /// </summary>
    /// <remarks>
    /// Null rather than pre-filled on purpose: the configuration binder appends to an existing
    /// array instead of replacing it, so a pre-filled default could not be overridden.
    /// </remarks>
    public TimeSpan[]? RetryDelays { get; set; }

    /// <summary>How long to wait before re-establishing a consumer after the connection or channel drops. Defaults to 5 s.</summary>
    public TimeSpan ConsumerRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long the client waits between automatic connection recovery attempts. Defaults to 5 s.</summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Register the <c>rabbitmq</c> health check. Defaults to true.</summary>
    public bool HealthChecks { get; set; } = true;

    /// <summary>
    /// Serializer settings for message bodies. Not bound from configuration; set it in code through
    /// the <c>configure</c> callback. Defaults to <see cref="JsonSerializerOptions.Web"/>.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = JsonSerializerOptions.Web;

    /// <summary>The retry schedule actually applied: <see cref="RetryDelays"/> if set, otherwise <see cref="DefaultRetryDelays"/>.</summary>
    internal IReadOnlyList<TimeSpan> EffectiveRetryDelays => RetryDelays is { Length: > 0 } delays ? delays : DefaultRetryDelays;
}
