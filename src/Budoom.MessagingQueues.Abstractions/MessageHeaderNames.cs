namespace Budoom.MessagingQueues;

/// <summary>Names of the headers the messaging packages set on messages.</summary>
public static class MessageHeaderNames
{
    /// <summary>How many times this message has been delivered to its queue. First delivery is 1.</summary>
    public const string Attempt = "x-attempt";

    /// <summary>The queue a retried or dead-lettered message was originally consumed from.</summary>
    public const string OriginalQueue = "x-original-queue";

    /// <summary>Why the message was retried or dead-lettered, when the caller supplied a reason.</summary>
    public const string Reason = "x-reason";

    /// <summary>UTC timestamp (round-trip format) of the failure that caused the retry or dead-letter.</summary>
    public const string FailedAt = "x-failed-at";

    /// <summary>The CLR type the message was serialized from. Informational: consumers deserialize into their own type.</summary>
    public const string MessageType = "x-message-type";
}
