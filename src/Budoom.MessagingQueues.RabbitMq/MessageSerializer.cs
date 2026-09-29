using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// JSON body serialization. The wire format is plain UTF-8 JSON with
/// <c>application/json</c> as the content type; the CLR type travels in the AMQP
/// <c>type</c> property and the <see cref="MessageHeaderNames.MessageType"/> header for diagnostics
/// only — consuming deserializes into whatever type the caller asked for.
/// </summary>
internal sealed class MessageSerializer(IOptions<RabbitMqOptions> options)
{
    public const string ContentType = "application/json";

    private readonly JsonSerializerOptions jsonSerializerOptions = options.Value.JsonSerializerOptions ?? JsonSerializerOptions.Web;

    public ReadOnlyMemory<byte> Serialize<TMessage>(TMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, jsonSerializerOptions);

    public TMessage? Deserialize<TMessage>(ReadOnlyMemory<byte> body) =>
        JsonSerializer.Deserialize<TMessage>(body.Span, jsonSerializerOptions);

    public static string TypeNameOf<TMessage>() => typeof(TMessage).FullName ?? typeof(TMessage).Name;
}
