namespace TextMediation.Worker.Options;

public sealed class ModelOptions
{
    public const string SectionName = "Model";

    public string ModelPath { get; set; } = "/models/model.onnx";
    public string SentencePiecePath { get; set; } = "/models/sentencepiece.bpe.model";
    public int MaxSequenceLength { get; set; } = 128;
    public int IntraOpNumThreads { get; set; } = 1;

    /// <summary>
    /// Label order MUST match the model's id2label configuration exported from HuggingFace.
    /// </summary>
    public string[] Labels { get; set; } = new[] { "non-toxic", "toxic" };
    public float ConfidenceThreshold { get; set; } = 0.50f;
}
