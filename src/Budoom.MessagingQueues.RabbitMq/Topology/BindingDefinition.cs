namespace Budoom.MessagingQueues.RabbitMq.Topology;

internal sealed record BindingDefinition(string Exchange, string RoutingKey);
