using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TextMediation.Worker.Models;
using TextMediation.Worker.Options;

namespace TextMediation.Worker.Services;

public sealed class ModerationProducer : IDisposable
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<ModerationProducer> _log;

    public ModerationProducer(IOptions<KafkaOptions> options, ILogger<ModerationProducer> log)
    {
        var opts = options.Value;
        _topic = opts.OutputTopic;
        _log = log;

        var config = new ProducerConfig
        {
            BootstrapServers = opts.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = opts.LingerMs,
            CompressionType = CompressionType.Snappy,
            MessageSendMaxRetries = int.MaxValue,
            ClientId = $"{opts.GroupId}-producer",
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _log.LogError("Kafka producer error: {Reason}", e.Reason))
            .Build();
    }

    public async Task ProduceAsync(string key, ModeratedComment payload, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload, JsonOpts);
        var msg = new Message<string, string> { Key = key, Value = json };
        try
        {
            await _producer.ProduceAsync(_topic, msg, ct).ConfigureAwait(false);
        }
        catch (ProduceException<string, string> ex)
        {
            _log.LogError(ex, "Failed to produce to {Topic} key={Key}", _topic, key);
            throw;
        }
    }

    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(5)); }
        catch (Exception ex) { _log.LogWarning(ex, "Producer flush on dispose failed"); }
        _producer.Dispose();
    }
}
