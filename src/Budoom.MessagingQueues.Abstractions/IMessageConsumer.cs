namespace Budoom.MessagingQueues;

/// <summary>
/// Reads messages off a queue. Inject it into a <c>BackgroundService</c>, which enumerates the
/// stream and settles each message itself.
/// </summary>
public interface IMessageConsumer
{
    /// <summary>
    /// Streams messages from a queue until the token is cancelled. The stream survives connection
    /// drops: the subscription is re-established and enumeration continues, so the caller does not
    /// need its own reconnect loop.
    /// </summary>
    /// <param name="queue">Logical queue name as declared in the topology.</param>
    /// <param name="cancellationToken">Ends the stream.</param>
    /// <typeparam name="TMessage">Type the JSON body is deserialized into.</typeparam>
    IAsyncEnumerable<ReceivedMessage<TMessage>> ConsumeAsync<TMessage>(string queue, CancellationToken cancellationToken = default);
}
