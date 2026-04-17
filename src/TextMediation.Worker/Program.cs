using Microsoft.Extensions.Options;
using TextMediation.Worker.Options;
using TextMediation.Worker.Services;
using TextMediation.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .ValidateOnStart();

builder.Services
    .AddOptions<ModelOptions>()
    .Bind(builder.Configuration.GetSection(ModelOptions.SectionName))
    .ValidateOnStart();

builder.Services
    .AddOptions<TranslatorOptions>()
    .Bind(builder.Configuration.GetSection(TranslatorOptions.SectionName))
    .ValidateOnStart();

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
    o.IncludeScopes = false;
});

builder.Services.AddSingleton<XlmrTokenizer>(sp =>
{
    var opts = sp.GetRequiredService<IOptions<ModelOptions>>().Value;
    return new XlmrTokenizer(opts.SentencePiecePath, opts.MaxSequenceLength);
});
builder.Services.AddSingleton<IModerationService, OnnxModerationService>();
builder.Services.AddSingleton<ModerationProducer>();

builder.Services.AddSingleton<LanguageDetector>();
builder.Services.AddHttpClient<ITranslator, HttpTranslator>((sp, http) =>
{
    var opts = sp.GetRequiredService<IOptions<TranslatorOptions>>().Value;
    http.BaseAddress = new Uri(opts.BaseUrl);
    http.Timeout = TimeSpan.FromSeconds(opts.TimeoutSeconds);
});

builder.Services.AddHostedService<ModerationWorker>();

var host = builder.Build();
await host.RunAsync();
