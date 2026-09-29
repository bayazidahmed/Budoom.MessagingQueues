using RabbitMQ.Client;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Exclusive use of a pooled publishing channel. A channel must never be shared between concurrent
/// publishes — that interleaves frames and corrupts the protocol — so the lease is what guarantees
/// one publisher at a time. Always <c>await using</c> it.
/// </summary>
internal readonly struct PublisherChannelLease(PublisherChannelPool pool, IChannel channel) : IAsyncDisposable
{
    public IChannel Channel { get; } = channel;

    public ValueTask DisposeAsync() => pool.ReturnAsync(Channel);
}
