using Microsoft.Extensions.Options;
using TextMediation.Worker.Options;

namespace TextMediation.Worker.Services;

public sealed class LanguageDetector
{
    private readonly double _threshold;

    public LanguageDetector(IOptions<TranslatorOptions> opts)
    {
        _threshold = opts.Value.GreekRatioThreshold;
    }

    public bool IsGreek(string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return false;

        int greek = 0;
        int total = 0;

        foreach (var c in text)
        {
            if (c <= ' ') continue;
            total++;

            if ((c >= '\u0370' && c <= '\u03FF') || (c >= '\u1F00' && c <= '\u1FFF'))
                greek++;
        }

        if (total == 0) return false;
        return (double)greek / total >= _threshold;
    }
}
