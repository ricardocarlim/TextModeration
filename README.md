# TextMediation

Real-time multilingual comment moderation pipeline built on Apache Kafka, ONNX Runtime, and XLM-RoBERTa. Detects toxicity in Portuguese, English, and Greek using a fine-tuned transformer model, with automatic Greek→English translation via a FastAPI sidecar.

---

## Architecture

```
                      ┌─────────────────────────────────────────┐
                      │           tm-moderation (.NET 8)         │
                      │                                          │
Kafka                 │  ModerationWorker                        │         Kafka
comments.incoming ───►│    │                                     │───►  comments.moderated
                      │    ├─ IsGreek? ──► HttpTranslator ──►    │
                      │    │               (tm-translator)        │
                      │    └─ OnnxModerationService              │
                      │         XlmrTokenizer + ONNX Runtime     │
                      └─────────────────────────────────────────┘
                                          ▲
                      ┌───────────────────┘
                      │  tm-translator (Python / FastAPI)
                      │  Helsinki-NLP/opus-mt-grk-en
                      └─────────────────────────────────────────
```

### Flow

1. `ModerationWorker` consumes a message from `comments.incoming`
2. `LanguageDetector` checks if the text is Greek (Unicode ratio ≥ 30%)
3. If Greek: `HttpTranslator` calls the Python sidecar to translate EL → EN
   - On sidecar failure the message is emitted with `label = "unknown"` (fail-open)
4. `OnnxModerationService` tokenizes with `XlmrTokenizer` and runs ONNX inference
5. `ModerationProducer` publishes the verdict to `comments.moderated`
6. Kafka offset is committed only after the produce is acknowledged

---

## Services

| Container | Image | Role |
|---|---|---|
| `tm-zookeeper` | `confluentinc/cp-zookeeper:7.6.1` | Kafka coordination |
| `tm-kafka` | `confluentinc/cp-kafka:7.6.1` | Message broker |
| `tm-translator` | Python 3.11 / FastAPI | Greek → English translation |
| `tm-moderation` | .NET 8 Worker | Toxicity classification |

---

## ML Models

### Toxicity Classifier

[`textdetox/xlmr-large-toxicity-classifier`](https://huggingface.co/textdetox/xlmr-large-toxicity-classifier) — XLM-RoBERTa Large fine-tuned for multilingual toxicity detection. Exported to ONNX via [Optimum](https://huggingface.co/docs/optimum/index).

Supported languages (native): EN, PT, RU, UK, DE, ES, AR, HI, ZH, AM.

Greek is handled via translation (see below).

**Output format:** 2-class softmax `[non-toxic, toxic]` or single-logit sigmoid, both supported automatically.

### Greek Translator

[`Helsinki-NLP/opus-mt-grk-en`](https://huggingface.co/Helsinki-NLP/opus-mt-grk-en) — MarianMT model for Greek → English translation. Pre-downloaded at image build time. Runs as a FastAPI sidecar with beam search decoding (`num_beams=4`, `early_stopping=True`).

### Tokenizer

XLM-RoBERTa SentencePiece tokenizer (`sentencepiece.bpe.model`) loaded via `Microsoft.ML.Tokenizers`. Applies fairseq ID offset remapping (`+1`) to align internal SentencePiece IDs with the ONNX model's expected token IDs.

---

## Project Structure

```
TextMediation/
├── docker-compose.yaml
├── models/                          # Mount point for ONNX + SentencePiece files (not committed)
│   ├── model.onnx
│   └── sentencepiece.bpe.model
└── src/
    ├── TextMediation.Translator/    # Python sidecar
    │   ├── Dockerfile
    │   ├── main.py
    │   └── requirements.txt
    └── TextMediation.Worker/        # .NET 8 Worker Service
        ├── Dockerfile
        ├── Program.cs
        ├── appsettings.json
        ├── Models/
        │   ├── IncomingComment.cs
        │   ├── LabelScore.cs
        │   └── ModeratedComment.cs
        ├── Options/
        │   ├── KafkaOptions.cs
        │   ├── ModelOptions.cs
        │   └── TranslatorOptions.cs
        ├── Services/
        │   ├── HttpTranslator.cs
        │   ├── IModerationService.cs
        │   ├── ITranslator.cs
        │   ├── LanguageDetector.cs
        │   ├── ModerationProducer.cs
        │   ├── OnnxModerationService.cs
        │   └── XlmrTokenizer.cs
        └── Workers/
            └── ModerationWorker.cs
```

---

## Kafka Topics

| Topic | Direction | Schema |
|---|---|---|
| `comments.incoming` | Input | `IncomingComment` JSON |
| `comments.moderated` | Output | `ModeratedComment` JSON |

### IncomingComment

```json
{
  "id": "c-001",
  "text": "Your comment text here",
  "author": "optional-user-id",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### ModeratedComment

```json
{
  "commentId": "c-001",
  "originalText": "Your comment text here",
  "label": "non-toxic",
  "score": 0.997,
  "allScores": [],
  "inferenceMs": 94.5,
  "moderatedAt": "2024-01-01T00:00:00Z"
}
```

`label` is one of: `non-toxic`, `toxic`, `unknown` (translation failure).

---

## Configuration

### Worker (`appsettings.json` / environment variables)

| Key | Default | Description |
|---|---|---|
| `Kafka__BootstrapServers` | `kafka:9092` | Kafka broker address |
| `Kafka__GroupId` | `moderation-worker` | Consumer group |
| `Kafka__InputTopic` | `comments.incoming` | Source topic |
| `Kafka__OutputTopic` | `comments.moderated` | Destination topic |
| `Kafka__AutoOffsetReset` | `Earliest` | Offset reset policy |
| `Model__ModelPath` | `/models/model.onnx` | ONNX model file |
| `Model__SentencePiecePath` | `/models/sentencepiece.bpe.model` | Tokenizer model file |
| `Model__MaxSequenceLength` | `128` | Max token sequence length |
| `Model__IntraOpNumThreads` | `1` | ONNX intra-op threads |
| `Model__Labels` | `["non-toxic","toxic"]` | Label order (must match model `id2label`) |
| `Model__ConfidenceThreshold` | `0.50` | Min safe score to classify as non-toxic |
| `Translator__BaseUrl` | `http://translator:8000` | Translator sidecar URL |
| `Translator__TimeoutSeconds` | `15` | HTTP timeout for translation calls |
| `Translator__GreekRatioThreshold` | `0.30` | Min Greek char ratio to trigger translation |

### Translator (`docker-compose.yaml` environment)

| Variable | Default | Description |
|---|---|---|
| `TRANSLATOR_MODEL` | `Helsinki-NLP/opus-mt-grk-en` | HuggingFace model ID |
| `TRANSLATOR_MAX_LENGTH` | `256` | Max tokens for generation |

---

## Getting Started

### Prerequisites

- Docker Desktop with Compose V2
- ONNX model file and SentencePiece tokenizer placed in `./models/`

### Obtaining the models

Export the toxicity classifier to ONNX using [Optimum](https://huggingface.co/docs/optimum/exporters/onnx/usage_guides/export_a_model):

```bash
docker run --rm -it \
  -v "$(pwd)/models:/out" \
  python:3.11-slim bash -c "
    pip install -q optimum[exporters] transformers sentencepiece &&
    optimum-cli export onnx \
      --model textdetox/xlmr-large-toxicity-classifier \
      --task text-classification \
      /out/xlmr-toxicity
  "
```

Then copy the exported files:

```
models/model.onnx                  ← from /out/xlmr-toxicity/model.onnx
models/sentencepiece.bpe.model     ← from /out/xlmr-toxicity/sentencepiece.bpe.model
```

### Running

```bash
docker compose up -d
docker logs -f tm-translator   # wait for "Translator ready."
docker logs -f tm-moderation   # wait for "Consumer subscribed to 'comments.incoming'"
```

### Sending a test message (PowerShell)

```powershell
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

@"
c-en-01:{"id":"c-en-01","text":"You are an idiot and understand nothing"}
c-pt-01:{"id":"c-pt-01","text":"Você é um idiota e não entende nada"}
c-el-01:{"id":"c-el-01","text":"Είσαι ένα τόσο ηλίθιο άτομο"}
c-ok-01:{"id":"c-ok-01","text":"Thank you for your help today"}
"@ | docker exec -i tm-kafka kafka-console-producer `
  --bootstrap-server localhost:9092 `
  --topic comments.incoming `
  --property "key.separator=:" `
  --property "parse.key=true"
```

### Reading output

```bash
docker exec tm-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic comments.moderated \
  --from-beginning
```

---

## Dependencies

### .NET Worker

| Package | Version | Purpose |
|---|---|---|
| `Confluent.Kafka` | 2.5.3 | Kafka producer/consumer |
| `Microsoft.ML.OnnxRuntime` | 1.19.2 | ONNX model inference |
| `Microsoft.ML.Tokenizers` | 3.0.0-preview | SentencePiece tokenization |
| `Microsoft.Extensions.Hosting` | 8.0.1 | Worker Service host |
| `Microsoft.Extensions.Http` | 8.0.1 | Typed HTTP client factory |
| `Microsoft.Extensions.Options.ConfigurationExtensions` | 8.0.0 | Options binding |

### Python Sidecar

| Package | Version | Purpose |
|---|---|---|
| `fastapi` | 0.115.0 | REST API framework |
| `uvicorn[standard]` | 0.30.6 | ASGI server |
| `transformers` | 4.44.2 | MarianMT model loading |
| `torch` | 2.4.1 | PyTorch inference backend |
| `sentencepiece` | 0.2.0 | Tokenizer for MarianMT |
| `sacremoses` | 0.1.1 | Text preprocessing |

---

## Design Decisions

**Fail-open on translation failure** — if the translator sidecar is unavailable, the worker emits `label = "unknown"` rather than blocking or retrying indefinitely. This prevents a translator outage from stalling the entire moderation pipeline.

**Manual Kafka commit** — offsets are committed only after the moderated verdict is successfully produced to `comments.moderated`. This guarantees at-least-once delivery with no silent message loss.

**ONNX export over direct inference** — running the HuggingFace model through ONNX Runtime instead of PyTorch in the .NET worker eliminates the Python runtime dependency from the critical path, reduces memory footprint, and enables graph-level optimizations (`ORT_ENABLE_ALL`).

**SentencePiece binary model** — `Microsoft.ML.Tokenizers` requires the binary `.model` protobuf format, not the `tokenizer.json` used by the HuggingFace tokenizer library. The SentencePiece IDs are remapped with a `+1` fairseq offset to match the model's expected vocabulary indices.

**Greek detection without Unicode APIs** — `LanguageDetector` uses direct codepoint range comparisons (`U+0370–U+03FF`, `U+1F00–U+1FFF`) rather than `char.IsLetter()`, which behaves inconsistently under `InvariantGlobalization=true` in containerized .NET deployments.
