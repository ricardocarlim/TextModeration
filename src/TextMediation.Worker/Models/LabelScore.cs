using System.Text.Json.Serialization;

namespace TextMediation.Worker.Models;

public readonly record struct LabelScore(
    [property: JsonPropertyName("label")] string Label,
    [property: JsonPropertyName("score")] float Score);
