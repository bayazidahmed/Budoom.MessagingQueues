using Budoom.MessagingQueues.RabbitMq.Topology;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Budoom.MessagingQueues.RabbitMq;

internal sealed class MessageConsumer(
    ConnectionProvider connectionProvider,
    TopologyProvisioner topologyProvisioner,
    TopologyRegistry topologyRegistry,
    MessagePublisher messagePublisher,
    MessageSerializer messageSerializer,
    IOptions<RabbitMqOptions> options,
    ILogger<MessageConsumer> logger) : IMessageConsumer
{
    public async IAsyncEnumerable<ReceivedMessage<TMessage>> ConsumeAsync<TMessage>(
        string queue,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var definition = topologyRegistry.GetQueue(queue);

        if (!connectionProvider.IsEnabled)
            yield break;

        var recoveryInterval = options.Value.ConsumerRecoveryInterval;

        if (!await WaitUntilProvisionedAsync(cancellationToken))
            yield break;

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumerSession<TMessage>? session = null;
            Exception? failure = null;

            try
            {
                session = await CreateSessionAsync<TMessage>(definition, cancellationToken);
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            if (session is not null)
            {
                await using (session)
                {
                    while (true)
                    {
                        var message = await ReadAsync(session, cancellationToken);

                        if (message is null)
                            break;

                        yield return message;
                    }
                }
            }

            if (cancellationToken.IsCancellationRequested)
                yield break;

            // Either the subscription could not be created or it ended: the broker closed the
            // channel, or the connection dropped. Back off and build a fresh one.
            RabbitMqLog.ConsumerInterrupted(logger, definition.LogicalName, recoveryInterval, failure);

            if (!await DelayAsync(recoveryInterval, cancellationToken))
                yield break;
        }
    }

    private static async Task<ReceivedMessage<TMessage>?> ReadAsync<TMessage>(ConsumerSession<TMessage> session, CancellationToken cancellationToken)
    {
        try
        {
            return await session.Reader.ReadAsync(cancellationToken);
        }
        catch (Exception exception) when (exception is ChannelClosedException or OperationCanceledException)
        {
            return null;
        }
    }

    private async Task<bool> WaitUntilProvisionedAsync(CancellationToken cancellationToken)
    {
        try
        {
            await topologyProvisioner.WaitUntilProvisionedAsync(cancellationToken);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<ConsumerSession<TMessage>> CreateSessionAsync<TMessage>(QueueDefinition definition, CancellationToken cancellationToken)
    {
        var connection = await connectionProvider.GetConsumerConnectionAsync(cancellationToken);

        var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: 1),
            cancellationToken);

        try
        {
            // A transient queue is exclusive to the connection that declares it, so it cannot be
            // provisioned at startup on the publishing connection — it is declared here instead,
            // and redeclared every time the subscription is rebuilt.
            if (definition.Exclusive)
                await TopologyProvisioner.DeclareQueueAsync(channel, definition, cancellationToken);

            var prefetchCount = definition.PrefetchCount ?? options.Value.DefaultPrefetchCount;

            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: (ushort)prefetchCount, global: false, cancellationToken);

            // Bounded to the prefetch so the buffer cannot outgrow what the broker has handed out:
            // the writer blocks the delivery dispatcher, which is the backpressure.
            var buffer = Channel.CreateBounded<ReceivedMessage<TMessage>>(new BoundedChannelOptions(prefetchCount)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });

            var context = new DeliveryContext(channel, definition, messagePublisher, logger);

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (_, deliverEventArgs) =>
            {
                var message = Materialize<TMessage>(context, definition, deliverEventArgs);

                try
                {
                    await buffer.Writer.WriteAsync(message, cancellationToken);
                }
                catch (Exception exception) when (exception is ChannelClosedException or OperationCanceledException)
                {
                    // Shutting down. The delivery was never acknowledged, so the broker keeps it.
                }
            };

            channel.ChannelShutdownAsync += (_, _) =>
            {
                buffer.Writer.TryComplete();

                return Task.CompletedTask;
            };

            var consumerTag = await channel.BasicConsumeAsync(
                queue: definition.Name,
                autoAck: false,
                consumer: consumer,
                cancellationToken: cancellationToken);

            RabbitMqLog.ConsumerStarted(logger, definition.LogicalName, prefetchCount);

            return new ConsumerSession<TMessage>(channel, consumerTag, buffer);
        }
        catch
        {
            await channel.DisposeAsync();

            throw;
        }
    }

    private RabbitMqReceivedMessage<TMessage> Materialize<TMessage>(
        DeliveryContext context,
        QueueDefinition definition,
        BasicDeliverEventArgs deliverEventArgs)
    {
        // The client reuses its delivery buffer as soon as this handler returns, so the body has to
        // be copied out now rather than referenced.
        var body = deliverEventArgs.Body.ToArray();

        var properties = deliverEventArgs.BasicProperties;
        var headers = AmqpHeaders.FromAmqp(properties.Headers);

        TMessage? message = default;
        Exception? deserializationError = null;

        try
        {
            message = messageSerializer.Deserialize<TMessage>(body);
        }
        catch (Exception exception)
        {
            deserializationError = exception;

            RabbitMqLog.DeserializationFailed(logger, definition.LogicalName, typeof(TMessage).Name, exception);
        }

        return new RabbitMqReceivedMessage<TMessage>(
            context,
            deliverEventArgs.DeliveryTag,
            body,
            headers,
            deliverEventArgs.Redelivered,
            properties.MessageId,
            properties.Type,
            properties.ContentType,
            properties.CorrelationId,
            message,
            deserializationError);
    }
}
