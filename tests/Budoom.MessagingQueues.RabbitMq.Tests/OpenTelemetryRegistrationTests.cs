using OpenTelemetry;
using OpenTelemetry.Trace;
using RabbitMQ.Client;
using System.Diagnostics;

namespace Budoom.MessagingQueues.RabbitMq.Tests;

public sealed class OpenTelemetryRegistrationTests
{
    [Fact]
    public void The_broker_source_names_match_RabbitMQ_Client()
    {
        Assert.Equal(RabbitMQActivitySource.PublisherSourceName, Budoom.MessagingQueues.OpenTelemetry.BrokerSourceNames.RabbitMqPublisher);
        Assert.Equal(RabbitMQActivitySource.SubscriberSourceName, Budoom.MessagingQueues.OpenTelemetry.BrokerSourceNames.RabbitMqSubscriber);
    }

    [Fact]
    public void By_default_process_and_publish_spans_are_recorded()
    {
        var recorded = new List<Activity>();

        using (var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddMessagingQueuesInstrumentation()
            .AddProcessor(new RecordingProcessor(recorded))
            .Build())
        {
            using var processSource = new ActivitySource(MessagingDiagnostics.ActivitySourceName);
            using var publishSource = new ActivitySource(RabbitMQActivitySource.PublisherSourceName);
            using var process = processSource.StartActivity("process");
            using var publish = publishSource.StartActivity("publish");
        }

        Assert.Contains(recorded, a => a.Source.Name == MessagingDiagnostics.ActivitySourceName);
        Assert.Contains(recorded, a => a.Source.Name == RabbitMQActivitySource.PublisherSourceName);
    }

    [Fact]
    public void Broker_spans_follow_the_options()
    {
        var recorded = new List<Activity>();

        using (var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddMessagingQueuesInstrumentation(options =>
            {
                options.IncludeBrokerPublishSpans = false;
                options.IncludeBrokerDeliverSpans = true;
            })
            .AddProcessor(new RecordingProcessor(recorded))
            .Build())
        {
            using var publishSource = new ActivitySource(RabbitMQActivitySource.PublisherSourceName);
            using var deliverSource = new ActivitySource(RabbitMQActivitySource.SubscriberSourceName);
            using var publish = publishSource.StartActivity("publish");
            using var deliver = deliverSource.StartActivity("deliver");
        }

        Assert.DoesNotContain(recorded, a => a.Source.Name == RabbitMQActivitySource.PublisherSourceName);
        Assert.Contains(recorded, a => a.Source.Name == RabbitMQActivitySource.SubscriberSourceName);
    }

    private sealed class RecordingProcessor(List<Activity> recorded) : BaseProcessor<Activity>
    {
        public override void OnEnd(Activity data)
        {
            lock (recorded)
                recorded.Add(data);
        }
    }
}
