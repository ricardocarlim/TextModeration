namespace TextMediation.Worker.Services;

public interface ITranslator
{
    Task<string?> TranslateToEnglishAsync(string text, CancellationToken ct);
}
