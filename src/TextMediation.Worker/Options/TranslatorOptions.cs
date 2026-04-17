namespace TextMediation.Worker.Options;

public sealed class TranslatorOptions
{
    public const string SectionName = "Translator";

    public string BaseUrl { get; set; } = "http://translator:8000";
    public int TimeoutSeconds { get; set; } = 15;
    public double GreekRatioThreshold { get; set; } = 0.30;
}
