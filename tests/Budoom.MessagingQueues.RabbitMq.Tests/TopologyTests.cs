using Budoom.MessagingQueues.RabbitMq.Topology;
using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq.Tests;

public sealed class TopologyTests
{
    private static readonly RabbitMqOptions Options = new();

    [Fact]
    public void Queue_uses_default_retry_delays_and_derives_retry_and_error_queue_names()
    {
        var topology = new TopologyBuilder();
        topology.Exchange("app-search", ExchangeType.Topic);
        topology.Queue("search_requested").BindTo("app-search", "search.requested");

        var queue = topology.Build(Options, "host-1").GetQueue("search_requested");

        Assert.Equal("search_requested", queue.Name);
        Assert.Equal(QueueType.Quorum, queue.Type);
        Assert.Equal(RabbitMqOptions.DefaultRetryDelays, queue.Retry!.Delays);
        Assert.Equal(4, queue.Retry.MaxAttempts);
        Assert.Equal("search_requested_error", queue.ErrorQueueName);
        Assert.Equal("search_requested_retry_15s", queue.RetryQueueName(TimeSpan.FromSeconds(15)));
    }

    [Fact]
    public void WithRetry_overrides_defaults_sorted_and_distinct()
    {
        var topology = new TopologyBuilder();
        topology.Queue("work").WithRetry(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1));

        var retry = topology.Build(Options, "host-1").GetQueue("work").Retry!;

        Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1)], retry.Delays);
    }

    [Fact]
    public void Transient_queue_gets_instance_suffix_and_no_retry_or_error_queue()
    {
        var topology = new TopologyBuilder();
        topology.TransientQueue("status");

        var registry = topology.Build(Options, "host-1");
        var queue = registry.GetQueue("status");

        Assert.Equal("status_host-1", queue.Name);
        Assert.Equal("status_host-1", registry.GetBrokerName("status"));
        Assert.True(queue.Exclusive);
        Assert.Equal(QueueType.Classic, queue.Type);
        Assert.Null(queue.Retry);
        Assert.Null(queue.ErrorQueueName);
    }

    [Fact]
    public void Transient_queue_rejects_retry()
    {
        var topology = new TopologyBuilder();

        Assert.Throws<InvalidOperationException>(() => topology.TransientQueue("status").WithRetry(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void Binding_to_undeclared_exchange_fails_the_build()
    {
        var topology = new TopologyBuilder();
        topology.Queue("work").BindTo("missing", "key");

        Assert.Throws<InvalidOperationException>(() => topology.Build(Options, "host-1"));
    }

    [Fact]
    public void Duplicate_queue_and_conflicting_exchange_are_rejected()
    {
        var topology = new TopologyBuilder();
        topology.Queue("work");
        topology.Exchange("events", ExchangeType.Topic);

        Assert.Throws<InvalidOperationException>(() => topology.Queue("work"));
        Assert.Throws<InvalidOperationException>(() => topology.Exchange("events", ExchangeType.Fanout));
        topology.Exchange("events", ExchangeType.Topic);
    }

    [Fact]
    public void Unknown_queue_names_pass_through_on_send_but_throw_on_lookup()
    {
        var registry = new TopologyBuilder().Build(Options, "host-1");

        Assert.Equal("someone_elses_queue", registry.ResolveQueueName("someone_elses_queue"));
        Assert.Throws<InvalidOperationException>(() => registry.GetBrokerName("someone_elses_queue"));
    }

    [Theory]
    [InlineData(2, 15)]
    [InlineData(3, 60)]
    [InlineData(4, 300)]
    [InlineData(9, 300)]
    public void RetryPolicy_picks_the_delay_for_the_next_attempt(int nextAttempt, int expectedSeconds)
    {
        var policy = new RetryPolicy(RabbitMqOptions.DefaultRetryDelays);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), policy.DelayFor(nextAttempt));
    }

    [Theory]
    [InlineData(1, 15)]
    [InlineData(15, 15)]
    [InlineData(16, 60)]
    [InlineData(3600, 300)]
    public void RetryPolicy_snaps_requested_delays_up_to_a_declared_one(int requestedSeconds, int expectedSeconds)
    {
        var policy = new RetryPolicy(RabbitMqOptions.DefaultRetryDelays);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), policy.Snap(TimeSpan.FromSeconds(requestedSeconds)));
    }
}
