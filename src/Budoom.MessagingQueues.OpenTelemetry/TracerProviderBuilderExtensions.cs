using Budoom.MessagingQueues;
using Budoom.MessagingQueues.OpenTelemetry;

// Deliberately in the OpenTelemetry namespace, like the official instrumentation packages, so the
// tracing setup needs no extra using.
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace OpenTelemetry.Trace;
#pragma warning restore IDE0130

/// <summary>Registers Budoom.MessagingQueues tracing with OpenTelemetry.</summary>
public static class TracerProviderBuilderExtensions
{
    /// <summary>
    /// Records a span per handled message (started by <see cref="ReceivedMessage{TMessage}.StartActivity"/>),
    /// joined to the trace of whoever published it, plus the broker client's publish spans.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <param name="configure">Turns the broker client's publish and deliver spans on or off.</param>
    /// <returns>The same builder, for chaining.</returns>
    /// <example>
    /// <code>
    /// builder.Services.AddOpenTelemetry()
    ///     .WithTracing(tracing => tracing
    ///         .AddMessagingQueuesInstrumentation()
    ///         .AddOtlpExporter());
    /// </code>
    /// </example>
    public static TracerProviderBuilder AddMessagingQueuesInstrumentation(
        this TracerProviderBuilder builder,
        Action<MessagingQueuesInstrumentationOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new MessagingQueuesInstrumentationOptions();
        configure?.Invoke(options);

        builder.AddSource(MessagingDiagnostics.ActivitySourceName);

        if (options.IncludeBrokerPublishSpans)
            builder.AddSource(BrokerSourceNames.Publish);

        if (options.IncludeBrokerDeliverSpans)
            builder.AddSource(BrokerSourceNames.Deliver);

        return builder;
    }
}
