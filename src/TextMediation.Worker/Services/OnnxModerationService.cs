using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;
using System.Diagnostics;
using TextMediation.Worker.Models;
using TextMediation.Worker.Options;

namespace TextMediation.Worker.Services;

public sealed class OnnxModerationService : IModerationService, IDisposable
{
    private readonly InferenceSession _session;
    private readonly XlmrTokenizer _tokenizer;
    private readonly ModelOptions _opts;
    private readonly ILogger<OnnxModerationService> _log;
    private readonly string _inputIdsName;
    private readonly string _attentionMaskName;
    private readonly string? _tokenTypeIdsName;
    private readonly string _logitsName;

    public OnnxModerationService(
        IOptions<ModelOptions> options,
        XlmrTokenizer tokenizer,
        ILogger<OnnxModerationService> log)
    {
        _opts = options.Value;
        _tokenizer = tokenizer;
        _log = log;

        var so = new SessionOptions { GraphOptimizationLevel = GraphOptimizationLevel.ORT_ENABLE_ALL };
        _session = new InferenceSession(_opts.ModelPath, so);

        _inputIdsName = _session.InputMetadata.Keys.FirstOrDefault(k => k.Contains("input_ids")) ?? "input_ids";
        _attentionMaskName = _session.InputMetadata.Keys.FirstOrDefault(k => k.Contains("attention_mask")) ?? "attention_mask";
        _tokenTypeIdsName = _session.InputMetadata.Keys.FirstOrDefault(k => k.Contains("token_type_ids"));
        _logitsName = _session.OutputMetadata.Keys.FirstOrDefault(k => k.Contains("logit")) ?? _session.OutputMetadata.Keys.First();

        _log.LogInformation("ONNX Moderation Service iniciado. Threshold Configurável: {T}", _opts.ConfidenceThreshold);
    }

    public ModeratedComment Classify(string commentId, string text)
    {
        var sw = Stopwatch.StartNew();

        var (ids, mask) = _tokenizer.Encode(text);
        int seqLen = ids.Length;

        var inputs = new List<NamedOnnxValue>
        {
            NamedOnnxValue.CreateFromTensor(_inputIdsName, new DenseTensor<long>(ids, new[] { 1, seqLen })),
            NamedOnnxValue.CreateFromTensor(_attentionMaskName, new DenseTensor<long>(mask, new[] { 1, seqLen }))
        };

        if (!string.IsNullOrEmpty(_tokenTypeIdsName))
        {
            inputs.Add(NamedOnnxValue.CreateFromTensor(_tokenTypeIdsName, new DenseTensor<long>(new long[seqLen], new[] { 1, seqLen })));
        }

        using var results = _session.Run(inputs);
        var logits = (results.FirstOrDefault(r => r.Name == _logitsName) ?? results.First()).AsTensor<float>().ToArray();

        float safeScore;
        float toxicScore;

        if (logits.Length == 1)
        {
            toxicScore = Sigmoid(logits[0]);
            safeScore = 1f - toxicScore;
        }
        else
        {
            var probs = Softmax(logits);
            safeScore = probs[0];
            toxicScore = probs[1];
        }

        int argmax;

        if (safeScore < _opts.ConfidenceThreshold || toxicScore > safeScore)
        {
            argmax = 1;
        }
        else
        {
            argmax = 0;
        }

        string label = (argmax < _opts.Labels.Length) ? _opts.Labels[argmax] : "unknown";
        float finalScore = argmax == 1 ? toxicScore : safeScore;

        _log.LogInformation("Moderação ID {Id}: {Label} (Safe: {S:P1}, Toxic: {T:P1})",
            commentId, label, safeScore, toxicScore);

        sw.Stop();
        return new ModeratedComment
        {
            CommentId = commentId,
            OriginalText = text,
            Label = label,
            Score = finalScore,
            InferenceMs = sw.Elapsed.TotalMilliseconds,
            ModeratedAt = DateTimeOffset.UtcNow
        };
    }

    private static float Sigmoid(float x) => 1f / (1f + MathF.Exp(-x));

    private static float[] Softmax(ReadOnlySpan<float> logits)
    {
        var result = new float[logits.Length];
        float max = float.NegativeInfinity;
        foreach (var l in logits) if (l > max) max = l;

        float sum = 0f;
        for (int i = 0; i < logits.Length; i++)
        {
            result[i] = MathF.Exp(logits[i] - max);
            sum += result[i];
        }

        for (int i = 0; i < result.Length; i++) result[i] /= sum;
        return result;
    }

    public void Dispose() => _session.Dispose();
}