# Serviço Reativo de Moderação de Texto — .NET 8 + Kafka + ONNX

## Context

O repositório atual (`TextMediation.sln`) contém apenas um esqueleto de Web API (`src/TextMediation.API`) gerado pelo template padrão do Visual Studio (Controllers, WeatherForecast, Swagger). O objetivo é transformar isso em um serviço **reativo** que:

1. Consome mensagens do tópico Kafka `comments.incoming`.
2. Classifica o texto via **DistilXLM-R multilíngue** rodando offline com **ONNX Runtime**.
3. Publica o veredito em `comments.moderated`.
4. Roda totalmente local via `docker-compose` (Zookeeper + Kafka + serviço .NET).

Como o projeto é praticamente greenfield, o plano substitui o projeto Web API atual por um **Worker Service** (`Microsoft.NET.Sdk.Worker`), que é o SDK apropriado para um `BackgroundService` de longa duração sem exposição HTTP. O nome do projeto passa a ser `TextMediation.Worker`.

---

## Arquitetura (visão geral)

```
┌─────────────────┐     ┌──────────────────────────────────────┐     ┌────────────────────┐
│ Kafka topic:    │ ──▶ │ TextMediation.Worker (BackgroundSvc) │ ──▶ │ Kafka topic:       │
│ comments.       │     │  ┌──────────┐  ┌──────────────────┐  │     │ comments.moderated │
│ incoming        │     │  │ Consumer │─▶│ XlmrTokenizer    │  │     │                    │
└─────────────────┘     │  └──────────┘  └────────┬─────────┘  │     └────────────────────┘
                        │                         ▼            │
                        │                 ┌────────────────┐   │
                        │                 │ OnnxInference  │   │
                        │                 │ (InferenceSession, │
                        │                 │  reutilizada)     │
                        │                 └────────┬─────────┘  │
                        │                          ▼            │
                        │                 ┌────────────────┐   │
                        │                 │ Producer       │   │
                        │                 └────────────────┘   │
                        └──────────────────────────────────────┘
```

Princípios-chave de baixa latência:
- **InferenceSession singleton** (construção cara, thread-safe para `Run`).
- **Tokenizer singleton** (carrega `sentencepiece.bpe.model` uma única vez).
- **Producer singleton** com `linger.ms` baixo.
- Commit manual no consumer (após produce + flush) → **at-least-once** end-to-end.
- Buffers `Int64` reutilizados por mensagem (tamanho `maxSeqLen = 128`).

---

## Estrutura de arquivos final

```
TextMediation/
├── docker-compose.yaml                          (NOVO — raiz)
├── TextMediation.sln                            (atualizar referência)
├── models/                                      (NOVO — volume montado no container)
│   ├── model.onnx
│   ├── sentencepiece.bpe.model
│   └── config.json                              (labels do classificador)
├── src/
│   └── TextMediation.Worker/                    (renomear de TextMediation.API)
│       ├── TextMediation.Worker.csproj          (reescrever como Worker SDK)
│       ├── Program.cs                           (reescrever — host genérico)
│       ├── Dockerfile                           (reescrever — multi-stage + libgomp1)
│       ├── appsettings.json                     (Kafka + Model)
│       ├── Workers/
│       │   └── ModerationWorker.cs              (BackgroundService — loop consume/produce)
│       ├── Services/
│       │   ├── IModerationService.cs
│       │   ├── OnnxModerationService.cs         (InferenceSession + softmax)
│       │   ├── XlmrTokenizer.cs                 (SentencePiece + offsets XLM-R)
│       │   └── ModerationProducer.cs            (wrapper Producer)
│       ├── Models/
│       │   ├── IncomingComment.cs
│       │   ├── ModeratedComment.cs
│       │   └── LabelScore.cs
│       └── Options/
│           ├── KafkaOptions.cs
│           └── ModelOptions.cs
└── docs/
    └── EXPORT_MODEL.md                          (NOVO — instruções HuggingFace → ONNX)
```

Arquivos a remover do projeto atual: `Controllers/`, `WeatherForecast.cs`, `TextMediation.API.http`, `Properties/launchSettings.json` (ou adaptar).

---

## Especificação dos arquivos

### 1. `src/TextMediation.Worker/TextMediation.Worker.csproj`

- SDK: `Microsoft.NET.Sdk.Worker`
- `TargetFramework`: `net8.0`
- `Nullable`: enable ; `ImplicitUsings`: enable
- PackageReferences:
  - `Confluent.Kafka` 2.5.x
  - `Microsoft.ML.OnnxRuntime` 1.19.x (CPU; trocar para `.Gpu` se quiser)
  - `Microsoft.ML.Tokenizers` 0.22.x (tem `SentencePieceTokenizer`)
  - `Microsoft.Extensions.Hosting` 8.0.x
  - `Microsoft.Extensions.Options.ConfigurationExtensions` 8.0.x
- Sem `DockerfileContext` inherited do VS; manter limpo.

### 2. `Program.cs`

- `Host.CreateApplicationBuilder(args)` (modelo .NET 8 genérico).
- Binding: `KafkaOptions` ← `Kafka`, `ModelOptions` ← `Model`.
- DI singletons: `XlmrTokenizer`, `IModerationService → OnnxModerationService`, `ModerationProducer`.
- `builder.Services.AddHostedService<ModerationWorker>()`.
- Logging console + request id scopes.

### 3. `Options/KafkaOptions.cs`

Campos: `BootstrapServers`, `GroupId`, `InputTopic`, `OutputTopic`, `AutoOffsetReset` (default Earliest), `SessionTimeoutMs`.

### 4. `Options/ModelOptions.cs`

Campos: `ModelPath` (default `/models/model.onnx`), `SentencePiecePath` (default `/models/sentencepiece.bpe.model`), `MaxSequenceLength` (128), `Labels` (array: `["non-toxic", "toxic"]` ou qualquer número de classes), `IntraOpNumThreads`.

### 5. `Services/XlmrTokenizer.cs`

Responsável por:
- Carregar `sentencepiece.bpe.model` via `SentencePieceTokenizer.Create(File.OpenRead(path))` (Microsoft.ML.Tokenizers 0.22+).
- Aplicar o **offset do XLM-R**: os IDs do SentencePiece são deslocados +1 no vocabulário do XLM-R (reservando 0–3 para `<s>, <pad>, </s>, <unk>`). Confirmar com `tokenizer_config.json` no export.
- Envelopar com tokens especiais: `[<s>] + tokens + [</s>]` (IDs 0 e 2).
- Truncar para `MaxSequenceLength`, pad com ID 1, devolver `input_ids` (`long[]`) e `attention_mask` (`long[]`).
- Método: `(long[] ids, long[] mask) Encode(string text)`.

Observação crítica: se a biblioteca `Microsoft.ML.Tokenizers` 0.22 não expuser `SentencePieceTokenizer` diretamente, usar `LlamaTokenizer` (também baseado em SentencePiece BPE) ou ler o `tokenizer.json` (fast tokenizer) com `BpeTokenizer.Create`. Decisão final feita na implementação — o plano permite fallback.

### 6. `Services/OnnxModerationService.cs`

- Singleton. Mantém `InferenceSession` criada no construtor com:
  - `SessionOptions { IntraOpNumThreads = opts.IntraOpNumThreads, GraphOptimizationLevel = ORT_ENABLE_ALL }`.
- Método `ModerationResult Classify(string text)`:
  1. `(ids, mask) = tokenizer.Encode(text)`.
  2. Monta `DenseTensor<long>` shape `[1, seqLen]` para `input_ids` e `attention_mask`.
  3. `session.Run(inputs)` com `NamedOnnxValue`s.
  4. Lê tensor `logits` shape `[1, nLabels]`, aplica softmax numericamente estável, mapeia para `Labels[i]`.
  5. Retorna `ModeratedComment { CommentId, OriginalText, Label, Score, AllScores, ModeratedAt }`.
- `Classify` é puro e thread-safe; `InferenceSession.Run` é thread-safe.

### 7. `Services/ModerationProducer.cs`

- `IProducer<string, string>` criado com `ProducerConfig { BootstrapServers, EnableIdempotence = true, Acks = All, LingerMs = 5, CompressionType = Snappy }`.
- Método `Task ProduceAsync(string key, ModeratedComment msg, CancellationToken ct)` — usa `System.Text.Json`.
- Implementa `IDisposable` (`Flush` + `Dispose`).

### 8. `Workers/ModerationWorker.cs`

Loop robusto:

```
ConsumerConfig { BootstrapServers, GroupId, EnableAutoCommit = false,
                 AutoOffsetReset = Earliest, EnablePartitionEof = false }

while (!stoppingToken.IsCancellationRequested) {
    try {
        var cr = consumer.Consume(stoppingToken);         // bloqueia
        if (cr?.Message is null) continue;
        var incoming = JsonSerializer.Deserialize<IncomingComment>(cr.Message.Value);
        var result   = moderation.Classify(incoming.Text);
        result.CommentId = incoming.Id;
        await producer.ProduceAsync(incoming.Id, result, stoppingToken);
        consumer.Commit(cr);                              // commit só após produce
    }
    catch (ConsumeException ex) when (ex.Error.IsFatal) { throw; }
    catch (ConsumeException ex) { log.Error(ex, "consume error — continuando"); }
    catch (OperationCanceledException) { break; }
    catch (Exception ex) {
        log.Error(ex, "unexpected — skip offset / ou DLQ");
        // Opcional: produzir em comments.moderated.dlq
    }
}
consumer.Close();
```

- Consumer subscribe no `OnStartAsync` (ou primeiro iter).
- `StopAsync` chama `Close()` com timeout.

### 9. `Dockerfile` (substituir o atual)

Multi-stage otimizado:

```dockerfile
# ---------- build ----------
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/TextMediation.Worker/TextMediation.Worker.csproj src/TextMediation.Worker/
RUN dotnet restore src/TextMediation.Worker/TextMediation.Worker.csproj
COPY . .
RUN dotnet publish src/TextMediation.Worker/TextMediation.Worker.csproj \
    -c Release -o /app/publish /p:UseAppHost=false

# ---------- runtime ----------
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
# libgomp1 é requerida pelo OpenMP dentro do onnxruntime nativo
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=publish /app/publish .
ENV DOTNET_EnableDiagnostics=0
ENTRYPOINT ["dotnet", "TextMediation.Worker.dll"]
```

Usar `dotnet/runtime` (não `aspnet`) porque é Worker puro.

### 10. `docker-compose.yaml` (raiz do repo)

- **zookeeper**: `confluentinc/cp-zookeeper:7.6.1`, porta 2181 interna.
- **kafka**: `confluentinc/cp-kafka:7.6.1`, 2 listeners:
  - `INTERNAL://kafka:9092` (containers)
  - `HOST://localhost:9094` (dev no host)
  - `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` (ou `kafka-init` separado).
  - `healthcheck` com `kafka-topics --bootstrap-server localhost:9092 --list`.
- **moderation**: build `.` com `dockerfile: src/TextMediation.Worker/Dockerfile`, env:
  - `Kafka__BootstrapServers=kafka:9092`
  - `Kafka__GroupId=moderation-worker`
  - `Kafka__InputTopic=comments.incoming`
  - `Kafka__OutputTopic=comments.moderated`
  - `Model__ModelPath=/models/model.onnx`
  - `Model__SentencePiecePath=/models/sentencepiece.bpe.model`
  - `depends_on: kafka: { condition: service_healthy }`
  - `volumes: ./models:/models:ro`
- Rede compartilhada default.

### 11. `docs/EXPORT_MODEL.md`

Instruções passo a passo:

```bash
# Requer Python 3.10+
pip install --upgrade "optimum[exporters]" transformers sentencepiece onnx

# Exporta DistilXLM-R classificador (ex: textdetox/xlmr-large-toxicity-classifier
# ou qualquer fine-tune multilíngue compatível) para ./models
optimum-cli export onnx \
  --model <hf-repo-id> \
  --task text-classification \
  --opset 17 \
  ./models

# Copiar o SentencePiece BPE para o nome esperado:
cp ./models/sentencepiece.bpe.model ./models/sentencepiece.bpe.model

# Validar que existem: model.onnx, sentencepiece.bpe.model, config.json
ls ./models
```

Notas:
- A pasta `./models` é montada como volume read-only no container (`/models`).
- Ajustar `Model.Labels` em `appsettings.json` conforme `id2label` do `config.json`.
- Para rodar com GPU, trocar o pacote NuGet para `Microsoft.ML.OnnxRuntime.Gpu` e adicionar runtime CUDA ao Dockerfile.

---

## Arquivos críticos a modificar

- `TextMediation.sln` — atualizar referência para `TextMediation.Worker`.
- `src/TextMediation.Worker/TextMediation.Worker.csproj` — reescrever.
- `src/TextMediation.Worker/Program.cs` — reescrever.
- `src/TextMediation.Worker/Dockerfile` — reescrever (multi-stage, libgomp1, runtime).
- `.dockerignore` — adicionar `models/`, `bin/`, `obj/`, `.vs/`.
- Criar todos os arquivos de `Services/`, `Workers/`, `Models/`, `Options/`.
- Criar `docker-compose.yaml` na raiz.
- Criar `docs/EXPORT_MODEL.md`.

Arquivos a deletar: `Controllers/`, `WeatherForecast.cs`, `TextMediation.API.http`, `appsettings.Development.json` se ficar redundante.

Funções/utilitários existentes reutilizáveis: **nenhuma** — template padrão do VS, tudo será substituído.

---

## Verificação end-to-end

1. **Exportar o modelo**: seguir `docs/EXPORT_MODEL.md` e confirmar `./models/model.onnx` + `./models/sentencepiece.bpe.model`.
2. **Subir a stack**: `docker compose up --build`. Observar logs do serviço `moderation` ficar em *"Consumer subscribed, waiting messages..."*.
3. **Teste manual** — em outro terminal, entrar no container do kafka e publicar uma mensagem:
   ```bash
   docker compose exec kafka kafka-console-producer \
     --bootstrap-server localhost:9092 --topic comments.incoming \
     --property parse.key=true --property key.separator=:
   # digitar: c1:{"id":"c1","text":"eu te odeio"}
   ```
4. **Consumir o resultado**:
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 --topic comments.moderated \
     --from-beginning --property print.key=true
   ```
   Esperado: JSON `{"commentId":"c1","label":"toxic","score":0.97,...}`.
5. **Teste de latência**: enviar 1000 mensagens via `kafka-producer-perf-test` e medir p50/p95 nos logs (`Classify` tempo).
6. **Resiliência**: `docker compose stop kafka && sleep 5 && docker compose start kafka` — o worker deve reconectar sem reiniciar o container graças ao loop try/catch + `ConsumeException` não-fatal.
7. **Shutdown limpo**: `docker compose down`; confirmar que o worker loga `"Closing consumer..."` antes de sair.

---

## Observações finais

- O plano mantém **singletons** para tokenizer, InferenceSession e Producer — eliminando alocações por mensagem e garantindo baixa latência.
- **At-least-once** end-to-end: `EnableAutoCommit=false` + commit manual pós-produce; com `EnableIdempotence=true` no producer evita duplicatas dentro de uma sessão.
- O tratamento de erro não bloqueia o loop em falhas transientes de consumo, mas propaga `IsFatal`.
- A escolha de `SentencePieceTokenizer` vs `LlamaTokenizer` em Microsoft.ML.Tokenizers será validada na implementação — ambas são suficientes para XLM-R com os offsets corretos.
