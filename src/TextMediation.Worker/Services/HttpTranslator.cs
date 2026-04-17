using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace TextMediation.Worker.Services;

public sealed class HttpTranslator : ITranslator
{
    private readonly HttpClient _http;
    private readonly ILogger<HttpTranslator> _log;

    public HttpTranslator(HttpClient http, ILogger<HttpTranslator> log)
    {
        _http = http;
        _log = log;
    }

    public async Task<string?> TranslateToEnglishAsync(string text, CancellationToken ct)
    {
        try
        {
            using var resp = await _http.PostAsJsonAsync("/translate", new TranslateRequest(text), ct)
                .ConfigureAwait(false);

            if (!resp.IsSuccessStatusCode)
            {
                _log.LogWarning("Translator returned HTTP {Status} — failing open", (int)resp.StatusCode);
                return null;
            }

            var payload = await resp.Content.ReadFromJsonAsync<TranslateResponse>(cancellationToken: ct)
                .ConfigureAwait(false);

            if (payload is null || string.IsNullOrWhiteSpace(payload.Translation))
            {
                _log.LogWarning("Translator returned empty payload — failing open");
                return null;
            }

            return payload.Translation;
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Translator call failed — failing open");
            return null;
        }
    }

    private sealed record TranslateRequest([property: JsonPropertyName("text")] string Text);

    private sealed record TranslateResponse([property: JsonPropertyName("translation")] string Translation);
}
