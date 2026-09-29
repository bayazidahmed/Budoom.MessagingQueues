namespace Budoom.MessagingQueues.RabbitMq.Topology;

internal enum QueueType
{
    /// <summary>
    /// Replicated, durable, and the default for work queues. Cannot be exclusive or auto-delete,
    /// and carries an <c>x-delivery-limit</c> (20 by default) after which redelivered messages are
    /// dead-lettered — which is what stops a requeue loop from spinning forever.
    /// </summary>
    Quorum = 0,

    /// <summary>
    /// Single-node classic queue. Required for exclusive or auto-delete queues, such as the
    /// per-instance broadcast queues used for fan-out.
    /// </summary>
    Classic = 1
}
