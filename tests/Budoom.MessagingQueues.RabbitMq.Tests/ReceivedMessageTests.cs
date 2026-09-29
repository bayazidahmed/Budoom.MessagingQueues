namespace Budoom.MessagingQueues.RabbitMq.Tests;

public sealed class ReceivedMessageTests
{
    private sealed class FakeMessage(bool supportsRetry, IReadOnlyDictionary<string, string>? headers = null)
        : ReceivedMessage<string>("queue", ReadOnlyMemory<byte>.Empty, headers ?? new Dictionary<string, string>(), false, null, null, null, null, "body", null)
    {
        public int Settlements { get; private set; }

        public override int MaxAttempts => supportsRetry ? 4 : 1;

        public override TimeSpan? NextRetryDelay => null;

        public override bool SupportsRetry => supportsRetry;

        protected override Task AckCoreAsync(CancellationToken cancellationToken) => Count();

        protected override Task NackCoreAsync(CancellationToken cancellationToken) => Count();

        protected override Task RejectCoreAsync(CancellationToken cancellationToken) => Count();

        protected override async Task<RetryOutcome> RetryCoreAsync(TimeSpan? delay, string? reason, CancellationToken cancellationToken)
        {
            await Count();

            return RetryOutcome.Scheduled;
        }

        private Task Count()
        {
            Settlements++;

            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task A_message_can_only_be_settled_once()
    {
        var message = new FakeMessage(supportsRetry: true);

        await message.AckAsync(TestContext.Current.CancellationToken);

        Assert.True(message.IsSettled);
        await Assert.ThrowsAsync<InvalidOperationException>(() => message.RejectAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, message.Settlements);
    }

    [Fact]
    public async Task Retry_without_a_policy_throws_and_leaves_the_message_unsettled()
    {
        var message = new FakeMessage(supportsRetry: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => message.RetryAsync(cancellationToken: TestContext.Current.CancellationToken));

        Assert.False(message.IsSettled);
        await message.NackAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, message.Settlements);
    }

    [Fact]
    public void Attempt_comes_from_the_header_and_defaults_to_one()
    {
        Assert.Equal(1, new FakeMessage(true).Attempt);
        Assert.Equal(3, new FakeMessage(true, new Dictionary<string, string> { [MessageHeaderNames.Attempt] = "3" }).Attempt);
    }
}
