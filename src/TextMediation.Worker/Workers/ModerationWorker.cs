using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TextMediation.Worker.Models;
using TextMediation.Worker.Options;
using TextMediation.Worker.Services;

namespace TextMediation.Worker.Workers;

public sealed class ModerationWorker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private readonly KafkaOptions _kafka;
    private readonly IModerationService _moderation;
    private readonly ModerationProducer _producer;
    private readonly LanguageDetector _langDetector;
    private readonly ITranslator _translator;
    private readonly ILogger<ModerationWorker> _log;
    private readonly IHostApplicationLifetime _lifetime;

    public ModerationWorker(
        IOptions<KafkaOptions> kafka,
        IModerationService moderation,
        ModerationProducer producer,
        LanguageDetector langDetector,
        ITranslator translator,
        IHostApplicationLifetime lifetime,
        ILogger<ModerationWorker> log)
    {
        _kafka = kafka.Value;
        _moderation = moderation;
        _producer = producer;
        _langDetector = langDetector;
        _translator = translator;
        _lifetime = lifetime;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_kafka.AutoOffsetReset, ignoreCase: true, out var r)
                ? r
                : AutoOffsetReset.Earliest,
            SessionTimeoutMs = _kafka.SessionTimeoutMs,
            EnablePartitionEof = false,
            AllowAutoCreateTopics = true,
            ClientId = $"{_kafka.GroupId}-consumer",
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _log.LogError("Kafka consumer error: {Reason} (fatal={Fatal})", e.Reason, e.IsFatal))
            .SetLogHandler((_, m) => _log.LogDebug("rdkafka: {Message}", m.Message))
            .Build();

        try
        {
            consumer.Subscribe(_kafka.InputTopic);
            _log.LogInformation(
                "Consumer subscribed to '{Topic}' (group={Group}, brokers={Brokers}), waiting messages...",
                _kafka.InputTopic, _kafka.GroupId, _kafka.BootstrapServers);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? cr = null;
                try
                {
                    cr = consumer.Consume(stoppingToken);
                    if (cr?.Message is null) continue;

                    await ProcessAsync(cr, stoppingToken).ConfigureAwait(false);

                    try { consumer.Commit(cr); }
                    catch (KafkaException ex)
                    {
                        _log.LogWarning(ex, "Commit failed for offset {Offset}", cr.TopicPartitionOffset);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex) when (ex.Error.IsFatal)
                {
                    _log.LogCritical(ex, "Fatal Kafka consume error — stopping host");
                    _lifetime.StopApplication();
                    break;
                }
                catch (ConsumeException ex)
                {
                    _log.LogError(ex, "Transient consume error — continuing");
                }
                catch (JsonException ex)
                {
                    _log.LogError(ex, "Malformed JSON at offset {Offset} — skipping", cr?.TopicPartitionOffset);
                    if (cr is not null)
                    {
                        try { consumer.Commit(cr); } catch { /* best effort */ }
                    }
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Unexpected processing error — message will be retried on next poll");
                }
            }
        }
        finally
        {
            _log.LogInformation("Closing consumer...");
            try { consumer.Close(); }
            catch (Exception ex) { _log.LogWarning(ex, "consumer.Close() threw"); }
        }
    }

    private async Task ProcessAsync(ConsumeResult<string, string> cr, CancellationToken ct)
    {
        var incoming = JsonSerializer.Deserialize<IncomingComment>(cr.Message.Value, JsonOpts);
        if (incoming is null || string.IsNullOrWhiteSpace(incoming.Text))
        {
            _log.LogWarning("Empty/invalid payload at {Offset} — skipping", cr.TopicPartitionOffset);
            return;
        }

        var id = string.IsNullOrWhiteSpace(incoming.Id)
            ? (cr.Message.Key ?? Guid.NewGuid().ToString("N"))
            : incoming.Id;

        string textForModel = incoming.Text;

        if (_langDetector.IsGreek(incoming.Text))
        {
            _log.LogWarning("Greek text detected (id={Id}) — invoking translator sidecar", id);
            var translated = await _translator.TranslateToEnglishAsync(incoming.Text, ct).ConfigureAwait(false);

            if (string.IsNullOrWhiteSpace(translated))
            {
                _log.LogWarning("Translator unavailable/failed for id={Id} — emitting 'unknown' verdict (fail-open)", id);

                var fallback = new ModeratedComment
                {
                    CommentId = id,
                    OriginalText = incoming.Text,
                    Label = "unknown",
                    Score = 0f,
                    InferenceMs = 0,
                    ModeratedAt = DateTimeOffset.UtcNow
                };

                await _producer.ProduceAsync(id, fallback, ct).ConfigureAwait(false);
                return;
            }

            textForModel = translated;
        }

        var result = _moderation.Classify(id, textForModel);

        // Preserve the caller's original text in the output regardless of translation.
        result.OriginalText = incoming.Text;

        await _producer.ProduceAsync(id, result, ct).ConfigureAwait(false);

        _log.LogInformation(
            "Moderated id={Id} label={Label} score={Score:F3} inference={Ms:F1}ms",
            id, result.Label, result.Score, result.InferenceMs);
    }
}
