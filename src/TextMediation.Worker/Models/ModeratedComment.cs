using System.Text.Json.Serialization;

namespace TextMediation.Worker.Models;

public sealed class ModeratedComment
{
    [JsonPropertyName("commentId")]
    public string CommentId { get; set; } = string.Empty;

    [JsonPropertyName("originalText")]
    public string OriginalText { get; set; } = string.Empty;

    [JsonPropertyName("label")]
    public string Label { get; set; } = string.Empty;

    [JsonPropertyName("score")]
    public float Score { get; set; }

    [JsonPropertyName("allScores")]
    public IReadOnlyList<LabelScore> AllScores { get; set; } = Array.Empty<LabelScore>();

    [JsonPropertyName("inferenceMs")]
    public double InferenceMs { get; set; }

    [JsonPropertyName("moderatedAt")]
    public DateTimeOffset ModeratedAt { get; set; } = DateTimeOffset.UtcNow;
}
