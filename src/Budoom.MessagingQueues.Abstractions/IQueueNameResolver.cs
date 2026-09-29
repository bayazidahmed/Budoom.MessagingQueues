namespace Budoom.MessagingQueues;

/// <summary>
/// Maps a logical queue name to the name that exists on the broker. The two differ for per-instance
/// (transient) queues, whose broker name carries an instance suffix — so this is how a caller learns
/// what to put in a <c>ReplyTo</c> field.
/// </summary>
public interface IQueueNameResolver
{
    /// <summary>Returns the broker name of a declared queue.</summary>
    /// <param name="queue">Logical queue name as declared in the topology.</param>
    /// <exception cref="InvalidOperationException">The queue is not declared.</exception>
    string GetBrokerName(string queue);
}
