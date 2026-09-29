using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// A small pool of channels used for publishing, with publisher confirmations enabled: a publish
/// completes only once the broker has taken responsibility for the message. The pool size is the
/// publish concurrency limit, because a channel cannot be used by two publishes at once.
/// </summary>
internal sealed class PublisherChannelPool(
    ConnectionProvider connectionProvider,
    IOptions<RabbitMqOptions> options,
    ILogger<PublisherChannelPool> logger) : IAsyncDisposable
{
    private readonly SemaphoreSlim slots = new(options.Value.PublisherChannelPoolSize, options.Value.PublisherChannelPoolSize);
    private readonly ConcurrentBag<IChannel> idle = [];

    private bool disposed;

    public async ValueTask<PublisherChannelLease> RentAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        await slots.WaitAsync(cancellationToken);

        try
        {
            while (idle.TryTake(out var pooled))
            {
                if (pooled.IsOpen)
                    return new PublisherChannelLease(this, pooled);

                await SafeDisposeAsync(pooled);
            }

            return new PublisherChannelLease(this, await CreateChannelAsync(cancellationToken));
        }
        catch
        {
            slots.Release();

            throw;
        }
    }

    internal async ValueTask ReturnAsync(IChannel channel)
    {
        try
        {
            if (!disposed && channel.IsOpen)
                idle.Add(channel);
            else
                await SafeDisposeAsync(channel);
        }
        finally
        {
            slots.Release();
        }
    }

    private async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        var connection = await connectionProvider.GetPublisherConnectionAsync(cancellationToken);

        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken);

        // Published with mandatory set, so anything the broker cannot route comes back here instead
        // of vanishing. That is almost always a missing binding or a typo in a routing key.
        channel.BasicReturnAsync += (_, returnEventArgs) =>
        {
            RabbitMqLog.Returned(logger, returnEventArgs.Exchange, returnEventArgs.RoutingKey, returnEventArgs.ReplyCode, returnEventArgs.ReplyText);

            return Task.CompletedTask;
        };

        return channel;
    }

    private async Task SafeDisposeAsync(IChannel channel)
    {
        try
        {
            await channel.DisposeAsync();
        }
        catch (Exception exception)
        {
            RabbitMqLog.ChannelDisposeFailed(logger, exception);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;

        while (idle.TryTake(out var channel))
        {
            await SafeDisposeAsync(channel);
        }

        slots.Dispose();
    }
}
