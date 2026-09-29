# Budoom.MessagingQueues

Simple, reliable messaging for .NET 10 applications. Your code talks to broker-neutral interfaces — `IMessagePublisher` and `IMessageConsumer` — and a broker package plugs in the implementation.

| Package | What it gives you |
|---|---|
| `Budoom.MessagingQueues.Abstractions` | The interfaces and types your application code uses: `IMessagePublisher`, `IMessageConsumer`, `ReceivedMessage<T>`, `IQueueNameResolver`, `RetryOutcome`, `MessageHeaderNames`. |
| `Budoom.MessagingQueues.RabbitMq` | The RabbitMQ implementation, built on the official `RabbitMQ.Client` 7 (fully async). |

Support for Azure Service Bus and Kafka is planned behind the same interfaces.

## Features

- **Topology declared in code.** Exchanges, queues and bindings are declared once at startup, so publishing and consuming just use names.
- **Confirmed, persistent publishing.** A publish completes only after the broker has accepted the message. Unroutable messages are logged rather than silently dropped.
- **Self-healing consumers.** A consumer keeps running through connection drops — no reconnect loop needed in your code.
- **Delayed retry with backoff**, with no broker plugin required, plus an error queue for messages that can't be processed.
- **Health check** and **configuration binding** out of the box.

## Installation

```shell
dotnet add package Budoom.MessagingQueues.RabbitMq
```

The Abstractions package comes along automatically. Class libraries that only publish or consume can reference `Budoom.MessagingQueues.Abstractions` on its own.

## Getting started

### 1. Configure the connection

Add a `MessagingQueues:RabbitMq` section to `appsettings.json`:

```json
{
  "MessagingQueues": {
    "RabbitMq": {
      "ConnectionString": "amqps://user:password@broker.example.com:5671/vhost"
    }
  }
}
```

Every setting has a default, so for a RabbitMQ running on `localhost` with the default `guest` account you can leave the section out entirely. Keep real credentials in user secrets or environment variables (for example `MessagingQueues__RabbitMq__ConnectionString`).

### 2. Register the services

In `Program.cs`:

```csharp
builder.AddMessagingQueueRabbitMqInfrastructureServices();
```

No extra `using` is needed — the extension methods live in the `Microsoft.Extensions.Hosting` namespace.

### 3. Declare your topology

Each feature of your application declares the exchanges and queues it owns. You can call `AddRabbitMqTopology` as many times as you like, in any order, before or after the call above:

```csharp
using RabbitMQ.Client; // for ExchangeType

builder.AddRabbitMqTopology(topology =>
{
    topology.Exchange("app-orders", ExchangeType.Topic);

    topology
        .Queue("order_placed")
        .BindTo("app-orders", "order.placed")
        .WithPrefetch(4)
        .WithRetry(TimeSpan.FromSeconds(15), TimeSpan.FromMinutes(1));
});
```

Everything is declared on the broker when the application starts.

### 4. Publish messages

Inject `IMessagePublisher` wherever you need to send something. Messages are serialized as JSON.

```csharp
using Budoom.MessagingQueues;

public sealed class OrderService(IMessagePublisher publisher)
{
    public async Task PlaceOrderAsync(Guid orderId, CancellationToken cancellationToken)
    {
        // Publish through an exchange; its bindings decide which queues receive it.
        await publisher.PublishAsync("app-orders", "order.placed", new OrderPlaced(orderId), cancellationToken: cancellationToken);

        // Or send straight to a single queue.
        await publisher.SendAsync("order_placed", new OrderPlaced(orderId), cancellationToken: cancellationToken);
    }
}
```

Both methods accept optional headers (`IReadOnlyDictionary<string, string>`).

### 5. Consume messages

Inject `IMessageConsumer` into a `BackgroundService` and enumerate the queue:

```csharp
using Budoom.MessagingQueues;

public sealed class OrderPlacedHandler(IMessageConsumer consumer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var message in consumer.ConsumeAsync<OrderPlaced>("order_placed", stoppingToken))
        {
            if (message.DeserializationError is not null)
            {
                await message.RejectAsync(stoppingToken); // unreadable: straight to the error queue
                continue;
            }

            try
            {
                await HandleAsync(message.Message!, stoppingToken);
                await message.AckAsync(stoppingToken);
            }
            catch (Exception exception)
            {
                // Tries again later; goes to the error queue once attempts run out.
                await message.RetryAsync(reason: exception.Message, cancellationToken: stoppingToken);
            }
        }
    }
}

// Register it:
builder.Services.AddHostedService<OrderPlacedHandler>();
```

## Settling messages

Every message you receive must be settled **exactly once**, using one of these methods:

| Method | What happens |
|---|---|
| `AckAsync` | Success. The message is removed from the queue. |
| `NackAsync` | Put back for immediate redelivery. Use it for a momentary glitch — it is not a backoff. |
| `RejectAsync` | Moved to the error queue (`<queue>_error`) without retrying, where you can inspect it. |
| `RetryAsync` | Held for the next delay in the retry schedule, then redelivered. Once attempts run out it is moved to the error queue. Returns `RetryOutcome.Scheduled` or `RetryOutcome.DeadLettered`. |

Useful properties on a received message:

- `Message` — the deserialized body (check `DeserializationError` first).
- `Body`, `Headers`, `MessageId`, `CorrelationId`, `Redelivered`.
- `Attempt` / `MaxAttempts` — which delivery this is, and how many are allowed.
- `NextRetryDelay` and `SupportsRetry`.

Delivery is **at-least-once**: in rare failure cases a message can arrive twice, so make your handlers idempotent.

## Retry and error queues

Durable queues get delayed retry by default. Each distinct delay gets its own holding queue (`<queue>_retry_15s`, `<queue>_retry_60s`, …) that returns the message to the original queue once the delay expires — no RabbitMQ plugin needed. Messages that are rejected, or that run out of attempts, land in `<queue>_error`.

- The default schedule is **15 seconds, 1 minute, 5 minutes**, which allows 4 deliveries in total (the first one plus one per delay).
- Change the default for all queues with the `RetryDelays` setting, or per queue with `.WithRetry(...)`.
- Turn retry off for a queue with `.WithoutRetry()`; calling `RetryAsync` on its messages then throws.
- `RetryAsync(delay: ...)` can override the delay for one message; it is rounded up to the nearest configured delay.

## Queue types

| Declaration | Behaviour |
|---|---|
| `topology.Queue(name)` | Durable quorum queue shared by all running instances of your app. Each message is handled by exactly one instance. Supports retry. |
| `topology.TransientQueue(name)` | A private queue per running instance, deleted when the instance stops. A message routed to it reaches **every** instance — useful for broadcasting cache invalidations or receiving replies. No retry. |

Other queue options: `.BindTo(exchange, routingKey)`, `.WithPrefetch(n)`, `.AsClassic()` (classic instead of quorum queue).

A transient queue's name on the broker carries an instance suffix. To tell another service where to reply, get the real name from `IQueueNameResolver`:

```csharp
var replyTo = queueNameResolver.GetBrokerName("order_replies");
```

### Naming tips

- Exchanges in `kebab-case`, queues in `snake_case`, routing keys `dotted` (topic wildcards match dot-separated words).
- Renaming a queue or exchange later leaves the old one on the broker; delete it yourself.
- Changing a queue's settings after it exists (for example quorum ↔ classic) makes startup fail with `PRECONDITION_FAILED`. Delete the queue or choose a new name.

## Configuration reference

All settings live under `MessagingQueues:RabbitMq`:

| Setting | Default | Description |
|---|---|---|
| `Enabled` | `true` | Set to `false` to run without a broker: nothing connects, consumers stay idle and publishing does nothing (a warning is logged). |
| `ConnectionString` | — | An `amqp://` or `amqps://` URI. When set, it takes precedence over the connection settings below. |
| `Host` | `localhost` | |
| `Port` | `5672` (`5671` with TLS) | |
| `VirtualHost` | `/` | |
| `UserName` / `Password` | `guest` / `guest` | RabbitMQ only accepts `guest` from localhost. |
| `UseTls` | `false` | |
| `ClientProvidedName` | entry assembly name | Connection name shown in the RabbitMQ management UI. |
| `PublisherChannelPoolSize` | `8` | Maximum number of publishes in flight at once. |
| `DefaultPrefetchCount` | `16` | Unacknowledged messages per consumer, unless the queue sets `WithPrefetch`. |
| `RetryDelays` | `00:00:15`, `00:01:00`, `00:05:00` | Default retry schedule. A configured list replaces the default. |
| `ConsumerRecoveryInterval` | `00:00:05` | Wait before a consumer reconnects after a drop. |
| `NetworkRecoveryInterval` | `00:00:05` | Wait between connection recovery attempts. |
| `HealthChecks` | `true` | Registers the `rabbitmq` health check. |

Settings that can't come from configuration, such as the JSON serializer options, are set in code. The callback runs after configuration is bound, so it wins:

```csharp
builder.AddMessagingQueueRabbitMqInfrastructureServices(options =>
    options.JsonSerializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
    {
        // your settings
    });
```

Settings are validated at startup, so a bad connection string or retry schedule fails fast.

## Health check

A health check named `rabbitmq` (tags `messaging`, `rabbitmq`) is registered automatically:

- **Healthy** — connected, and the topology is declared.
- **Degraded** — still declaring the topology, or the broker is temporarily blocking the connection.
- **Unhealthy** — the broker can't be reached.

Expose it the usual way:

```csharp
app.MapHealthChecks("/health");
```

## License

MIT
