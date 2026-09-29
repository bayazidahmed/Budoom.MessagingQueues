using RabbitMQ.Client;
using System.Threading.Channels;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// One live subscription: a dedicated AMQP channel plus the in-memory buffer deliveries are handed
/// to. Torn down and rebuilt whenever the connection drops.
/// </summary>
internal sealed class ConsumerSession<TMessage>(IChannel channel, string consumerTag, Channel<ReceivedMessage<TMessage>> buffer) : IAsyncDisposable
{
    public ChannelReader<ReceivedMessage<TMessage>> Reader => buffer.Reader;

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (channel.IsOpen)
                await channel.BasicCancelAsync(consumerTag, noWait: true);
        }
        catch
        {
            // The channel is already gone, which is the usual reason this session is being disposed.
        }

        buffer.Writer.TryComplete();

        await channel.DisposeAsync();
    }
}
