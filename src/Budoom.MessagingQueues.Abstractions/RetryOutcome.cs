namespace Budoom.MessagingQueues;

/// <summary>What <see cref="ReceivedMessage{TMessage}.RetryAsync"/> did with the message.</summary>
public enum RetryOutcome
{
    /// <summary>The message was parked and will come back once the delay expires.</summary>
    Scheduled = 0,

    /// <summary>Attempts were exhausted, so the message was moved to the error queue and will not come back.</summary>
    DeadLettered = 1
}
