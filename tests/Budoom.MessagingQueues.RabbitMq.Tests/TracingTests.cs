using System.Diagnostics;

namespace Budoom.MessagingQueues.RabbitMq.Tests;

public sealed class TracingTests
{
    internal const string TestSourceName = "Budoom.MessagingQueues.Tests";

    internal static readonly ActivitySource TestSource = new(TestSourceName);

    private const string TraceId = "0af7651916cd43dd8448eb211c80319c";
    private const string ParentSpanId = "b7ad6b7169203331";

    private sealed class FakeMessage(
        IReadOnlyDictionary<string, string>? headers = null,
        Exception? deserializationError = null,
        RetryOutcome retryOutcome = RetryOutcome.Scheduled)
        : ReceivedMessage<string>(
            "orders",
            "abc"u8.ToArray(),
            headers ?? new Dictionary<string, string>(),
            false,
            "m-1",
            null,
            null,
            "c-1",
            deserializationError is null ? "body" : null,
            deserializationError)
    {
        public override int MaxAttempts => 4;

        public override TimeSpan? NextRetryDelay => null;

        public override bool SupportsRetry => true;

        protected override string? MessagingSystem => "test";

        protected override Task AckCoreAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override Task NackCoreAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override Task RejectCoreAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        protected override Task<RetryOutcome> RetryCoreAsync(TimeSpan? delay, string? reason, CancellationToken cancellationToken) =>
            Task.FromResult(retryOutcome);
    }

    /// <summary>Records everything from the library's source and the tests' own source.</summary>
    internal static ActivityListener Listen()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is MessagingDiagnostics.ActivitySourceName or TestSourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };

        ActivitySource.AddActivityListener(listener);

        return listener;
    }

    private static Dictionary<string, string> TraceHeaders() =>
        new() { ["traceparent"] = "00-" + TraceId + "-" + ParentSpanId + "-01" };

    [Fact]
    public void The_span_continues_the_publishers_trace_from_the_headers()
    {
        using var listener = Listen();

        using var activity = new FakeMessage(TraceHeaders()).StartActivity();

        Assert.NotNull(activity);
        Assert.Same(activity, Activity.Current);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("process orders", activity.DisplayName);
        Assert.Equal(TraceId, activity.TraceId.ToHexString());
        Assert.Equal(ParentSpanId, activity.ParentSpanId.ToHexString());
        Assert.Equal("test", activity.GetTagItem("messaging.system"));
        Assert.Equal("process", activity.GetTagItem("messaging.operation.type"));
        Assert.Equal("orders", activity.GetTagItem("messaging.destination.name"));
        Assert.Equal("m-1", activity.GetTagItem("messaging.message.id"));
        Assert.Equal("c-1", activity.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(3, (int?)activity.GetTagItem("messaging.message.body.size"));
        Assert.Equal(1, (int?)activity.GetTagItem(MessagingDiagnostics.AttemptTagName));
    }

    [Fact]
    public void Without_trace_headers_the_span_joins_the_current_activity()
    {
        using var listener = Listen();
        using var parent = TestSource.StartActivity("host");

        using var activity = new FakeMessage().StartActivity();

        Assert.NotNull(parent);
        Assert.NotNull(activity);
        Assert.Equal(parent.TraceId, activity.TraceId);
        Assert.Equal(parent.SpanId, activity.ParentSpanId);
    }

    [Fact]
    public async Task Ack_records_the_outcome_without_failing_the_span()
    {
        using var listener = Listen();
        var message = new FakeMessage(TraceHeaders());
        using var activity = message.StartActivity();

        await message.AckAsync(TestContext.Current.CancellationToken);

        Assert.Equal("ack", activity!.GetTagItem(MessagingDiagnostics.OutcomeTagName));
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
    }

    [Fact]
    public async Task Reject_marks_the_span_as_failed()
    {
        using var listener = Listen();
        var message = new FakeMessage(TraceHeaders());
        using var activity = message.StartActivity();

        await message.RejectAsync(TestContext.Current.CancellationToken);

        Assert.Equal("reject", activity!.GetTagItem(MessagingDiagnostics.OutcomeTagName));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task A_scheduled_retry_is_recorded_but_not_a_failure()
    {
        using var listener = Listen();
        var message = new FakeMessage(TraceHeaders());
        using var activity = message.StartActivity();

        var outcome = await message.RetryAsync(reason: "db timeout", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RetryOutcome.Scheduled, outcome);
        Assert.Equal("retry", activity!.GetTagItem(MessagingDiagnostics.OutcomeTagName));
        Assert.Equal("db timeout", activity.GetTagItem(MessagingDiagnostics.ReasonTagName));
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
    }

    [Fact]
    public async Task Dead_lettering_marks_the_span_as_failed()
    {
        using var listener = Listen();
        var message = new FakeMessage(TraceHeaders(), retryOutcome: RetryOutcome.DeadLettered);
        using var activity = message.StartActivity();

        var outcome = await message.RetryAsync(reason: "db timeout", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RetryOutcome.DeadLettered, outcome);
        Assert.Equal("dead_letter", activity!.GetTagItem(MessagingDiagnostics.OutcomeTagName));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("db timeout", activity.StatusDescription);
    }

    [Fact]
    public void A_deserialization_error_is_recorded_on_the_span()
    {
        using var listener = Listen();

        using var activity = new FakeMessage(TraceHeaders(), new FormatException("bad json")).StartActivity();

        Assert.Equal(ActivityStatusCode.Error, activity!.Status);
        Assert.Contains(activity.Events, e => e.Name == "exception");
    }

    [Fact]
    public void The_span_can_only_be_started_once()
    {
        using var listener = Listen();
        var message = new FakeMessage();
        using var activity = message.StartActivity();

        Assert.Throws<InvalidOperationException>(() => message.StartActivity());
    }
}
