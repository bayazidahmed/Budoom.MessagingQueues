namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <summary>
/// Delayed-retry settings for one queue. Each delay gets its own retry queue, which holds the
/// message for <c>x-message-ttl</c> and then dead-letters it back to the original queue.
/// </summary>
/// <param name="Delays">Backoff schedule. The Nth retry waits <c>Delays[N - 1]</c>, clamped to the last entry.</param>
internal sealed record RetryPolicy(IReadOnlyList<TimeSpan> Delays)
{
    /// <summary>
    /// Deliveries allowed before the message is dead-lettered to the error queue: the first
    /// delivery plus one per configured delay.
    /// </summary>
    public int MaxAttempts => Delays.Count + 1;

    /// <summary>Delay for the retry that produces delivery number <paramref name="nextAttempt"/>.</summary>
    public TimeSpan DelayFor(int nextAttempt)
    {
        var index = Math.Clamp(nextAttempt - 2, 0, Delays.Count - 1);

        return Delays[index];
    }

    /// <summary>
    /// Snaps a caller-supplied delay to a declared one, since only the declared delays have a
    /// retry queue. Picks the shortest declared delay that is at least as long as requested.
    /// </summary>
    public TimeSpan Snap(TimeSpan requested)
    {
        foreach (var delay in Delays.OrderBy(d => d))
        {
            if (delay >= requested)
                return delay;
        }

        return Delays.Max();
    }
}
