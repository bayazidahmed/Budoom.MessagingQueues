using System.Globalization;
using System.Text;

namespace Budoom.MessagingQueues.RabbitMq;

/// <summary>
/// Conversions between the AMQP field table (where values arrive as <see cref="byte"/> arrays) and
/// the string dictionary callers see. Header names are in <see cref="MessageHeaderNames"/>.
/// </summary>
internal static class AmqpHeaders
{
    public static Dictionary<string, object?> ToAmqp(IReadOnlyDictionary<string, string>? headers)
    {
        var amqp = new Dictionary<string, object?>(headers?.Count ?? 0, StringComparer.Ordinal);

        if (headers is null)
            return amqp;

        foreach (var (key, value) in headers)
        {
            amqp[key] = value;
        }

        return amqp;
    }

    public static Dictionary<string, string> FromAmqp(IDictionary<string, object?>? headers)
    {
        var converted = new Dictionary<string, string>(headers?.Count ?? 0, StringComparer.Ordinal);

        if (headers is null)
            return converted;

        foreach (var (key, value) in headers)
        {
            var text = ToText(value);
            if (text is not null)
                converted[key] = text;
        }

        return converted;
    }

    /// <summary>
    /// AMQP field table values are loosely typed: RabbitMQ hands back long strings as byte arrays,
    /// and nested tables and arrays for things like x-death.
    /// </summary>
    private static string? ToText(object? value) => value switch
    {
        null => null,
        string text => text,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        IEnumerable<object?> items => string.Join(",", items.Select(ToText)),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString()
    };
}
