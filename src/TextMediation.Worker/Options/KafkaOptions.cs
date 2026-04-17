namespace TextMediation.Worker.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "moderation-worker";
    public string InputTopic { get; set; } = "comments.incoming";
    public string OutputTopic { get; set; } = "comments.moderated";
    public string AutoOffsetReset { get; set; } = "Earliest";
    public int SessionTimeoutMs { get; set; } = 10000;
    public int LingerMs { get; set; } = 5;
}
