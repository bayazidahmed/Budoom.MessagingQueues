namespace Budoom.MessagingQueues.RabbitMq.Topology;

/// <param name="Name">Exchange name.</param>
/// <param name="Type">One of the <see cref="RabbitMQ.Client.ExchangeType"/> constants.</param>
/// <param name="Durable">Survives a broker restart.</param>
/// <param name="AutoDelete">Deleted once the last binding is removed.</param>
/// <param name="RequiresBoundQueue">
/// Publishes are mandatory, so a message no queue receives fails. False when declared with
/// <see cref="ExchangeTopologyBuilder.WithoutBoundQueue"/>.
/// </param>
internal sealed record ExchangeDefinition(string Name, string Type, bool Durable = true, bool AutoDelete = false, bool RequiresBoundQueue = true);
