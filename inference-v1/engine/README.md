# Observer.ai inference engine

Decides which LLM agent traces are worth keeping for review. For each trace it
asks a fine-tuned decision model (Kev) whether the final answer is likely
wrong, applies a keep/drop policy, and exports the kept traces over OTLP to
any number of trace back ends: [Arize Phoenix](https://github.com/Arize-ai/phoenix),
[Langfuse](https://langfuse.com), an OpenTelemetry Collector, or anything
else that accepts OTLP/HTTP traces.

Traces arrive as OpenTelemetry spans (OTLP over gRPC or HTTP) or over an HTTP
JSON API, and wait in a queue: in NATS JetStream when `queue.url` is set, or
in the engine's memory when it is not. A worker pool in the same binary
pulls them off the queue and does the rest.

```
OTLP gRPC :4317 ─┐
OTLP HTTP :4318 ─┴─> spans stored by trace ID ──> assembled once the trace goes quiet ─┐
                     (stream OBSERVER_SPANS)                                            │
                                                                                        v
POST /v1/traces ──> validate ───────────────────────────────> JetStream stream OBSERVER_TRACES
                                                                                        │
                                                                                        v
                                             worker pool ──> slice ──> Kev ──> policy ──> exporters (kept only)
                                                  │
                               system error, after max_deliver attempts
                                                  v
                                    JetStream stream OBSERVER_TRACES_DLQ

POST /v1/traces:score ──> validate ──> slice ──> Kev ──> policy ──> exporters   (no queue)
```

## Setup

Requirements:

- Docker, to run Phoenix, NATS (optional, see [Where the queues live](#where-the-queues-live)) and the engine itself
- Go 1.26 or newer, to build or test outside Docker
- Python 3, only for regenerating the golden slice files and sending samples
- A Kev server (not needed for tests). Phoenix runs from the compose file.

### With Docker Compose

```sh
make up       # docker compose up --build -d: the engine, NATS JetStream and Phoenix
make logs     # follow the engine's log
make down     # stop all three; queued traces, dead letters and Phoenix's data stay in volumes
```

Compose starts three services:

| Service | Host ports | Notes |
| --- | --- | --- |
| `observer` | 8080 (JSON API), 4318 (OTLP HTTP), **14317** (OTLP gRPC) | The engine: receivers, API and worker pool |
| `nats` | 4222 (clients), 8222 (monitoring), both on loopback only | JetStream, stored in the `nats-data` volume; configured by `nats.conf` |
| `phoenix` | 6006 (UI and OTLP HTTP), 4317 (OTLP gRPC) | Phoenix, stored in the `phoenix-data` volume. Open http://localhost:6006 |

Inside the container the engine listens for OTLP gRPC on 4317. Compose
publishes it on host port 14317 because Phoenix takes 4317 for its own OTLP
gRPC receiver.

If a Phoenix is already running on the host (`phoenix serve`), it holds 6006
and 4317 and `make up` fails with "port is already allocated". Either stop
it, or move the compose one with `PHOENIX_PORT` and `PHOENIX_GRPC_PORT`, or
keep using the host one:

```sh
OBSERVER_EXPORT_ENDPOINT=http://host.docker.internal:6006/v1/traces \
  docker compose up --build -d --no-deps nats observer
```

Kev is not in the compose file. By default the engine container reaches it
on the Docker host at port 8010. Override with environment variables when
running compose:

| Variable | Default |
| --- | --- |
| `OBSERVER_SCORER_ENDPOINT` | `http://host.docker.internal:8010/v1/systemone` |
| `OBSERVER_EXPORT_ENDPOINT` | `http://phoenix:6006/v1/traces` (the first exporter) |
| `PHOENIX_PORT` | `6006` |
| `PHOENIX_GRPC_PORT` | `4317` |
| `OBSERVER_MANIFEST_FILE` | `../../finetune/notebooks/runs/observer-kev-0.8b-ft/manifest.json` (mounted into the container) |
| `OBSERVER_PORT` | `8080` |
| `OBSERVER_OTLP_GRPC_PORT` | `14317` |
| `OBSERVER_OTLP_HTTP_PORT` | `4318` |

On Linux, a Kev that listens only on `127.0.0.1` is not reachable from a
container; bind it to `0.0.0.0` or point the variable elsewhere.
Docker Desktop on macOS and Windows forwards to host loopback, so the
defaults work there.

### On the host

```sh
make phoenix  # start only Phoenix in Docker
make run      # build and start the engine with observer.yaml, queues in memory
make test     # go vet + go test -race, fully offline

# Or with the queues in NATS JetStream, as in production:
make nats
OBSERVER_NATS_URL=nats://localhost:4222 make run
```

`make run` binds 8080, 4317 and 4318. Phoenix, from `make phoenix` or on the
host, already holds 4317, and the engine exits with an "address already in
use" error; start it with `OBSERVER_OTLP_GRPC_ADDR=:14317 make run` instead.

### Try it

```sh
curl -X POST localhost:8080/v1/traces:score -d @testdata/traces/wrong_river.json
```

```json
{"trace_id":"wrong-river-001","decision":"keep","p_keep":0.626,"p_keep_raw":0.6087,"reason":"kev","severity":"normal","model":"kev-latest","latency_ms":78.5,"exported":true}
```

The trace then appears in the Phoenix project `observer-ai` (the default
exporter) with the `eval.*` attributes on its root span.

To send a batch of real traces from the labeled evaluation dataset
(`finetune/notebooks/data/eval/labeled_traces.jsonl`):

```sh
python3 scripts/send_samples.py                # 30 held-out traces through the queue
python3 scripts/send_samples.py -n 60 --sync   # print each decision next to its label
python3 scripts/send_samples.py --otlp         # the same traces as OTLP spans, to :4318
```

It samples equally across the `wrong`, `partial` and `correct` labels and
sends only the fields the API accepts. With `--otlp` each trace is sent as an
AGENT root span and one TOOL span per tool call.

To point an instrumented agent at the engine, give its OTLP exporter the
engine's endpoint instead of Phoenix's. For the test agent in
`finetune/testagents`:

```sh
python scripts/smoke.py --exporting
```

Flags: `-config PATH` (default `observer.yaml`) and `-log-level debug|info|warn|error`.

## Configuration

`observer.yaml` documents every key with its default. Only `scorer.manifest`
is required. [`../examples/`](../examples/) has ready-to-run variations,
including scoring with TypeSafe's hosted Jev instead of Kev. [observer.schema.yaml](observer.schema.yaml) is the full
reference: a JSON Schema with each key's type, default, limits and the rules
that tie keys together. [architecture.md](architecture.md) explains how the
file is loaded.

| Key | Default | Meaning |
| --- | --- | --- |
| `server.addr` | `:8080` | Listen address |
| `server.workers` | `8` | Workers pulling traces from the queue |
| `server.shutdown_timeout` | `30s` | Time for in-flight traces to finish on shutdown |
| `otlp.grpc_addr` | `:4317` | OTLP gRPC listen address; `""` turns gRPC off |
| `otlp.http_addr` | `:4318` | OTLP HTTP listen address (`POST /v1/traces`); `""` turns it off |
| `otlp.idle_timeout` | `10s` | A trace with no new spans for this long is assembled and scored |
| `otlp.max_wait` | `2m` | A trace is assembled this long after its first span even if spans keep arriving |
| `otlp.assemblers` | `4` | Goroutines assembling traces |
| `otlp.span_stream`, `otlp.span_subject` | `OBSERVER_SPANS`, `observer.spans` | Where spans wait, on subject `observer.spans.<trace id>` |
| `otlp.span_max_age` | `1h` | Spans never assembled are removed after this |
| `otlp.pending_stream`, `otlp.pending_subject` | `OBSERVER_PENDING`, `observer.pending` | One marker per trace awaiting assembly |
| `queue.url` | empty | NATS server; empty keeps every queue in memory. See [Where the queues live](#where-the-queues-live) |
| `queue.connect_timeout` | `30s` | How long to wait for NATS at start-up before exiting |
| `queue.stream` | `OBSERVER_TRACES` | Work-queue stream |
| `queue.subject` | `observer.traces` | Subject traces are published to |
| `queue.consumer` | `observer-workers` | Durable consumer; engines sharing the name share the work |
| `queue.max_messages` | `100000` | Queued traces before `POST /v1/traces` returns 503 |
| `queue.ack_wait` | `2m` | Time a worker may hold a trace before it is redelivered |
| `queue.max_deliver` | `5` | Delivery attempts before a trace is dead-lettered |
| `queue.redelivery_backoff` | `[5s, 30s, 2m, 5m]` | Delay before each redelivery; the last value repeats |
| `queue.dlq_stream` | `OBSERVER_TRACES_DLQ` | Dead-letter stream |
| `queue.dlq_subject` | `observer.traces.dlq` | Dead-letter subject |
| `queue.dlq_max_age` | `168h` | How long dead letters (traces and exports) are kept |
| `queue.export_stream`, `queue.export_subject` | `OBSERVER_EXPORTS`, `observer.exports` | Each exporter's queue on NATS: stream `OBSERVER_EXPORTS_<NAME>`, subject `observer.exports.<name>` |
| `queue.export_dlq_stream`, `queue.export_dlq_subject` | `OBSERVER_EXPORTS_DLQ`, `observer.exports_dlq` | Where exports given up on go, on NATS |
| `scorer.kind` | `kev` | `kev` for a Kev server, `jev` for TypeSafe's hosted Jev |
| `scorer.api_key_env` | `JEV_API_KEY` for `jev`, unset for `kev` | Environment variable holding the scorer's API key, sent as a bearer token. Start-up fails if it names an unset variable |
| `scorer.endpoint` | `http://localhost:8010/v1/systemone` | Kev endpoint |
| `scorer.model` | `kev-latest` | Model name sent to Kev |
| `scorer.manifest` | (required) | Path to the fine-tuning `manifest.json` |
| `scorer.decision_question` | `answer_wrong` | Manifest question that drives the decision; must be `noul` |
| `scorer.timeout` | `10s` | Timeout per attempt |
| `scorer.max_retries` | `3` | In-process retries after the first attempt |
| `scorer.backoff` | `[500ms, 2s, 5s]` | Base delay before each retry; the last value repeats |
| `agents.attributes` | `[agent.name, service.name]` | Span attributes naming a trace's agent, tried on the root span then its resource. See [One model per agent](#one-model-per-agent) |
| `agents.models` | none | Agent name to its model: any `scorer` keys, the rest inherited from `scorer` |
| `policy.keep_threshold` | `0.5` | Keep when calibrated `p_keep` reaches this |
| `policy.random_floor` | `0.02` | Share of below-threshold traces kept at random |
| `alert.severity_high` | `0.8` | `p_keep` at which a kept trace is `high` severity |
| `observability.error_sinks` | `[{kind: stdout}]` | Where the engine's own errors are written, one JSON object per line: `stdout`, and/or `file` with a `path` |
| `observability.metrics.endpoint` | empty (off) | OTLP/HTTP metrics URL for the engine's own metrics. See [Metrics](#metrics) |
| `observability.metrics.headers`, `headers_env` | none | Headers for the metrics endpoint, as for an exporter |
| `observability.metrics.interval` | `15s` | How often metrics are sent |
| `exporters` | one Phoenix at `http://localhost:6006/v1/traces` | The trace back ends kept traces go to; see [Exporters](#exporters). An empty list exports nothing |
| `exporters[].name` | (required) | Names the back end in logs, errors and its NATS stream: letters, digits, `-`, `_`; unique |
| `exporters[].endpoint` | (required) | OTLP/HTTP traces URL, including the path |
| `exporters[].headers` | none | Headers sent as written. Keep secrets out of the file |
| `exporters[].headers_env` | none | Environment variable holding more headers, as `key=value,key2=value2`. Start-up fails if it is unset |
| `exporters[].resource_attributes` | none | Resource attributes added to every span sent to this back end |
| `exporters[].queue_size` | `1024` | Kept traces waiting for this back end; past it a trace is skipped for it and the skip reported |
| `exporters[].concurrency` | `4` | Traces sent to this back end at once |
| `exporters[].max_deliver` | `3` | Tries before the exporter gives up on a trace |
| `exporters[].backoff` | `[1s, 5s, 30s]` | Wait before each retry; the last value repeats |

Environment overrides:

| Variable | Overrides |
| --- | --- |
| `OBSERVER_NATS_URL` | `queue.url` |
| `OBSERVER_SCORER_ENDPOINT` | `scorer.endpoint` |
| `OBSERVER_EXPORT_ENDPOINT` | `exporters[0].endpoint` |
| `OBSERVER_MANIFEST` | `scorer.manifest` |
| `OBSERVER_OTLP_GRPC_ADDR` | `otlp.grpc_addr` |
| `OBSERVER_OTLP_HTTP_ADDR` | `otlp.http_addr` |

The standard `OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_EXPORTER_OTLP_TRACES_HEADERS`
are not read: each exporter sends only its own headers, so one back end's
credentials never reach another. Use `headers_env` instead.

The engine validates the config at start-up and exits with a clear error if:

- a threshold is outside [0, 1], a duration does not parse, or a key is unknown
- the decision question is missing from the manifest or is not `noul`
- `queue.ack_wait` is not longer than the worst-case scoring time of the
  slowest model (every attempt timing out, plus backoff: 47.5s with the
  defaults), since a shorter value would hand a trace to a second worker
  while the first is still on it

### The manifest

The manifest is the JSON the fine-tuning notebook writes:

```json
{"model": "...", "questions": {"answer_wrong": {"type": "noul", "instructions": "..."}}, "temperatures": {"answer_wrong": 0.86}}
```

- `questions` is sent to Kev unchanged, all questions, in the manifest's order,
  because that is how the model was trained. Only the decision question's
  answer is used.
- `temperatures[decision_question]` calibrates the score (1.0 if absent).
- A relative `scorer.manifest` path resolves against the config file's
  directory. The example config points at
  `../../finetune/notebooks/runs/observer-kev-0.8b-ft/manifest.json`.

### One model per agent

Each agent can have its own model, fine-tuned on that agent's traces. The
`scorer` section is the default model; `agents.models` adds one per agent,
stating only what differs:

```yaml
scorer:                                # the default: any agent not listed below
  endpoint: http://kev:8010/v1/systemone
  manifest: runs/default/manifest.json

agents:
  attributes: [agent.name, service.name]
  models:
    support-bot:
      model: kev-support                # same Kev server, another model
      manifest: runs/support-bot/manifest.json
    research-agent:
      endpoint: http://kev-gpu-2:8010/v1/systemone
      model: kev-research
      manifest: runs/research-agent/manifest.json
```

- **Which agent.** For OTLP traces, the first of `agents.attributes` set on
  the root span, else on its resource: OpenInference's `agent.name`, then the
  resource's `service.name` by default. For the JSON API, the trace's
  `agent` field.
- **Which model.** The agent's entry under `agents.models`, else the
  `scorer` section. A trace that names no agent, or an agent not listed, is
  scored by the default.
- **Inheritance.** Each entry takes any `scorer` key (`kind`, `endpoint`,
  `model`, `api_key_env`, `manifest`, `decision_question`, `timeout`,
  `max_retries`, `backoff`) and inherits the others from `scorer` as written
  in the file. `OBSERVER_SCORER_ENDPOINT` and `OBSERVER_MANIFEST` change only
  the `scorer` section.
- **Calibration.** Each model is calibrated with its own manifest's
  temperature.
- **Everywhere.** The decision log, the `/v1/traces:score` response, the
  `eval.agent` span attribute and the metrics all carry the agent, and
  `eval.model` the model. `/readyz` probes the default model only, so one
  agent's model being down does not make the whole engine unready.

[`../examples/agents.yaml`](../examples/agents.yaml) is a complete example.

## OTLP ingestion

The engine runs the standard OTLP trace receivers, so an agent's exporter can
point at it as if it were a collector:

| Protocol | Address | Details |
| --- | --- | --- |
| gRPC | `:4317` | `opentelemetry.proto.collector.trace.v1.TraceService/Export`, gzip accepted |
| HTTP | `:4318`, `POST /v1/traces` | `application/x-protobuf` or `application/json`, gzip accepted, 16 MiB limit |

An export is answered with success as soon as the spans are stored (in NATS,
once it has confirmed them, typically within a few milliseconds). If they
cannot be stored the answer is gRPC `UNAVAILABLE` or HTTP 503, which OTLP
exporters retry.

### Connecting an agent or service

Point the OTLP exporter at the engine instead of at Phoenix. Nothing else in
the agent changes, and no API key or header is needed.

For anything using an OpenTelemetry SDK, the standard variables are enough:

```sh
# HTTP (the SDK appends /v1/traces)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

# or gRPC (host port 14317 under Docker Compose, 4317 inside the network)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:14317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

Or set it in code. In Python, with an OpenInference instrumentor:

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
)
trace.set_tracer_provider(provider)
# then instrument as usual, e.g. GoogleADKInstrumentor().instrument(tracer_provider=provider)
```

The engine needs one root span carrying the question in `input.value` and the
answer in `output.value`; tool calls are spans with
`openinference.span.kind` = `TOOL`. The instrumentors emit these already. To
send a trace by hand:

```sh
curl -i http://localhost:4318/v1/traces -H 'Content-Type: application/json' -d '{
  "resourceSpans": [{"scopeSpans": [{"spans": [{
    "traceId": "5b8efff798038103d269b633813fc60c",
    "spanId": "eee19b7ec3c1b174",
    "name": "agent",
    "startTimeUnixNano": "1760000000000000000",
    "endTimeUnixNano": "1760000001000000000",
    "attributes": [
      {"key": "openinference.span.kind", "value": {"stringValue": "AGENT"}},
      {"key": "input.value", "value": {"stringValue": "Which river flows through the capital of France?"}},
      {"key": "output.value", "value": {"stringValue": "The Rhône"}}
    ]
  }]}]}]
}'
```

It answers `200 {}`. The decision appears in the engine's log about
`otlp.idle_timeout` (10s) later, and the trace shows up in your exporters
(Phoenix by default) if it was kept.

### From spans to a decision

1. **Store.** Each export is split by trace ID and published to
   `observer.spans.<trace id>` in the `OBSERVER_SPANS` stream.
2. **Wait.** A trace's spans usually arrive in several batches, with the root
   span last. The trace is considered complete when no span has arrived for
   `otlp.idle_timeout`, or `otlp.max_wait` after its first span.
3. **Assemble.** The spans are merged (a span sent twice counts once) and
   turned into the same record the JSON API takes; see the mapping below.
   Spans that do not describe an agent run are discarded here, before they
   cost a Kev call.
4. **Queue, score, decide.** The record goes to `OBSERVER_TRACES` and from
   there everything is as for a JSON trace, including redelivery and the
   dead-letter stream.
5. **Forward.** A kept trace is sent to every exporter as its original spans,
   with the original trace and span IDs, the exporter's
   `resource_attributes`, and the `eval.*` attributes added to the root
   span. A dropped trace is sent nowhere and its spans are deleted.

Because of step 2, a decision arrives at least `otlp.idle_timeout` after the
trace's last span.

### How spans are read

The engine reads the [OpenInference](https://github.com/Arize-ai/openinference)
attributes that Phoenix's instrumentors emit.

| Trace field | Taken from |
| --- | --- |
| Root span | The span whose parent is not in the trace, preferring `openinference.span.kind` = `AGENT`, then `CHAIN` |
| `agent` | The first of `agents.attributes` set on the root span, else on its resource |
| `question` | Root span `input.value` |
| `final_answer` | Root span `output.value` |
| `tool_calls` | Spans with kind `TOOL`, ordered by start time |
| tool `name` | `tool.name`, else the span name |
| tool `args` | `input.value` if it is JSON, else `tool.parameters`, else `input.value` as a string |
| tool `status` | `observer.tool.status` if set; otherwise `error` when the span status is ERROR, else `ok` |
| `evidence` | First of: `observer.evidence.<i>` on the root span; documents on `RETRIEVER` spans; the paragraphs (split on blank lines) of successful tool outputs |

When `input.value` or `output.value` holds a JSON object, the text is taken
from a `question`/`input`/`query`/`prompt` key (or `answer`/`output`/`result`
for outputs) when there is one. A tool output shaped as a function response,
`{"response": {"result": "..."}}`, is unwrapped.

A message recorded by the Google ADK instrumentor,
`{"new_message": {"parts": [{"text": "..."}]}}` on the way in and
`{"content": {"parts": [{"text": "..."}]}}` on the way out, gives the text of
its parts, so an ADK agent's own root span works without extra attributes.

A trace is **not scorable**, and is discarded with a `spans discarded` log
line, when its root span has no `input.value` or no `output.value`. That
covers ordinary service spans sent to the engine by mistake.

An agent run that ended without an answer is scored like any other, as long
as its root span sets `output.value`, even to an empty string. The model then
sees an empty final answer.

### Matching what the model was trained on

The model was trained on evidence as bare paragraph text and on the agent's
own tool statuses (`ok`, `not_found`, `bad_args`, ...). Standard spans carry
neither exactly: a tool span holds the tool's rendered output, and its status
is only OK or ERROR. So for a trace read from plain instrumentor spans, the
slice differs a little from the training data. For the test agent, evidence
items keep their `[Title] ` prefix and a search that found nothing shows as
`ok` with its message as evidence.

An agent can remove the difference by setting two attributes itself:

| Attribute | On | Value |
| --- | --- | --- |
| `observer.evidence.0`, `observer.evidence.1`, ... | the root span | the evidence items, in order |
| `observer.tool.status` | each `TOOL` span | the tool call's status |

With both set, a trace sent as spans yields byte for byte the slice the same
trace yields through the JSON API. A test checks this for every fixture, and
`scripts/send_samples.py --otlp` sends spans this way.

## API

### `POST /v1/traces`

Stores a trace in the queue. On NATS the 202 is sent only after NATS has
confirmed the trace, so an accepted trace survives a restart of the engine.
In memory it survives only as long as the process.

```json
{
  "trace_id": "7f3a91c2e8",
  "agent": "support-bot",
  "question": "Which river flows through the capital of France?",
  "tool_calls": [{"name": "search", "args": {"query": "capital of France"}, "status": "ok"}],
  "evidence": ["Paris is the capital of France.", "The Seine flows through Paris."],
  "final_answer": "The Seine"
}
```

- `question` and `final_answer` are required. `final_answer` may be an empty
  string for a run that ended without an answer.
- `trace_id` is optional; the engine generates 32 hex characters if it is missing.
- `agent` is optional and picks the model (see [One model per agent](#one-model-per-agent)).
- `args` may be any JSON value. `status` defaults to `ok`.
- This is the engine's own JSON API on port 8080. The OTLP receiver also
  serves a `/v1/traces` path, on port 4318, with a different body.

| Status | Body | When |
| --- | --- | --- |
| 202 | `{"trace_id": "..."}` | Stored in the queue |
| 400 | `{"error": "..."}` | Invalid JSON or a missing required field |
| 413 | `{"error": "..."}` | Body larger than 8 MiB |
| 503 + `Retry-After` | `{"error": "..."}` | Queue at `queue.max_messages`, or NATS unreachable |

### `POST /v1/traces:score`

Same input, processed synchronously without the queue. Meant for testing and
demos. It returns the decision and flushes the export, so the trace is in
every exporter's back end when the response arrives.

```json
{
  "trace_id": "wrong-river-001",
  "agent": "",
  "decision": "keep",
  "p_keep": 0.626,
  "p_keep_raw": 0.6087,
  "reason": "kev",
  "severity": "normal",
  "model": "kev-latest",
  "latency_ms": 78.5,
  "exported": true
}
```

| Field | Values |
| --- | --- |
| `decision` | `keep` or `drop` |
| `p_keep_raw` | Kev's `noul` for the decision question; `null` on fallback |
| `p_keep` | `p_keep_raw` after temperature calibration; `null` on fallback |
| `reason` | `kev`, `random_floor`, `below_threshold` or `fallback` |
| `severity` | `high`, `normal` or `none` |
| `latency_ms` | Kev's reported latency; the measured time if Kev reports none or on fallback |
| `exported` | The trace was kept, queued to at least one exporter, and the flush of every exporter did not fail |

### API docs: `GET /docs` and `GET /openapi.yaml`

`/openapi.yaml` serves the OpenAPI 3.0 description of this API. `/docs` renders
it with Swagger UI, where you can also send requests. The UI's scripts load
from a CDN, so `/docs` needs internet access in the browser; the spec does not.

The spec lives in `internal/server/openapi.yaml` and is embedded in the binary.
Update it when you change a handler; a test fails if a route, a documented
status code or a field of the decision response goes missing from it.

### `GET /healthz` and `GET /readyz`

`/healthz` returns 200 whenever the process is up. `/readyz` returns 200 only
if the queue is up (the NATS connection, when NATS is used) and the default
model (the `scorer` section) answers `GET /v1/models` on its host within 2
seconds, and 503 otherwise. An agent's own model is not checked: when it is
down, only that agent's traces are affected (retried, then dead-lettered),
so it does not take the engine out of a load balancer; watch
`observer.scorer.duration` with `error.type` per `observer.model` for it.

## How a decision is made

1. **Route.** The trace's agent picks its model (see
   [One model per agent](#one-model-per-agent)).
2. **Slice.** `trace.BuildState` renders the trace into the text the model was
   trained on (see [The slice](#the-slice-and-its-golden-files)).
3. **Score.** The slice and the model's manifest questions go to its Kev. Timeouts,
   connection errors and 5xx responses are retried in-process with jittered
   backoff (between half and all of the configured delay).
4. **Calibrate.** `p_keep = sigmoid(logit(p_raw) / T)`, with the model's `T`, with `p_raw` clamped
   to [1e-6, 1 - 1e-6].
5. **Decide.**
   - `p_keep >= keep_threshold`: keep, reason `kev`.
   - Otherwise keep with probability `random_floor`, reason `random_floor`.
   - Otherwise drop, reason `below_threshold`.
6. **Severity.** `high` if kept and `p_keep >= alert.severity_high`, `normal`
   for other keeps, `none` for drops. This version only records severity; it
   sends no alerts.
7. **Export.** Kept traces are queued to every exporter. Each decision produces one JSON log
   line with `trace_id`, `agent`, `decision`, `p_keep`, `reason`, `model` and `latency_ms`.

### When Kev cannot score a trace

What happens depends on whose fault it is and which path the trace took.

| Failure | Queued (`POST /v1/traces`) | Sync (`:score`) |
| --- | --- | --- |
| **System error**: Kev unreachable, timing out, 5xx, 401/403/404/429, or an unparseable response | Redelivered, then dead-lettered. No decision is made. | Random fallback decision |
| **Kev rejects the trace**: 400, 413 or 422 | Random fallback decision | Random fallback decision |

The random fallback keeps the trace with probability `random_floor` and drops
it otherwise, with reason `fallback` either way.

## The queue

### Where the queues live

The engine has three kinds of queue: the trace work queue with its
dead-letter stream, the store where OTLP spans wait to be assembled, and one
export queue per exporter. `queue.url` decides where all of them live:

| `queue.url` | Broker | Survives a restart | Shared by replicas | Dead letters |
| --- | --- | --- | --- | --- |
| empty (the default) | in process | no | no, each engine works alone | reported to the error sinks, not stored |
| `nats://...` or `tls://...` (`OBSERVER_NATS_URL`) | NATS JetStream | yes | yes, through the durable consumers | stored in the DLQ streams, replayable |

Both follow the same rules: a bounded queue (`queue.max_messages`,
`exporters[].queue_size`), redelivery with backoff up to a delivery limit,
then a dead letter; spans assembled after `otlp.idle_timeout` of quiet or
`otlp.max_wait`. Memory suits trying the engine and single-instance setups
that can lose a few in-flight traces; use NATS for anything that must not.
Docker Compose sets `OBSERVER_NATS_URL`, so the compose stack uses NATS.

The rest of this section describes NATS.

- **Stream** `OBSERVER_TRACES` uses work-queue retention on file storage: a
  trace is removed once a worker acknowledges it. A full stream refuses new
  traces rather than evicting unscored ones.
- **Workers** pull one trace at a time through the durable consumer
  `observer-workers`. Run more engine replicas with the same consumer name to
  share the work.
- **Delivery is at least once.** If a worker dies after exporting a trace but
  before acknowledging it, the trace is scored and exported again.
- **Payload** is the trace JSON, the same shape the API accepts, so another
  producer can publish straight to `observer.traces`. A trace published that
  way without a `trace_id` gets one derived from its stream sequence.
- A trace assembled from OTLP spans also carries those spans in an
  `otlp_spans` field (base64 protobuf), so a kept trace can be forwarded as
  the originals. `nats.conf` raises the NATS message limit to 8 MB for this.
  A trace still too big is queued without its spans and, if kept, exported
  as rebuilt spans instead.

### Streams

| Stream | Holds | Removed when |
| --- | --- | --- |
| `OBSERVER_SPANS` | OTLP spans, one subject per trace ID | The trace is assembled, or after `otlp.span_max_age` |
| `OBSERVER_PENDING` | One marker per trace awaiting assembly | The trace is assembled |
| `OBSERVER_TRACES` | Traces waiting to be scored | A worker acknowledges the trace |
| `OBSERVER_TRACES_DLQ` | Traces that hit system errors | After `queue.dlq_max_age` |
| `OBSERVER_EXPORTS_<NAME>` | Kept traces waiting for one exporter (its name upper-cased, `-` as `_`) | The exporter sends the trace, or gives up on it |
| `OBSERVER_EXPORTS_DLQ` | Kept traces an exporter gave up on, with headers `Observer-Exporter`, `Observer-Dead-Error`, `Observer-Deliveries` | After `queue.dlq_max_age` |

### Redelivery and the dead-letter stream

When a worker hits a system error, the in-process retries have already run.
The worker then returns the trace to the queue with a delay from
`queue.redelivery_backoff`. After `queue.max_deliver` deliveries it copies the
trace to `OBSERVER_TRACES_DLQ` and removes it from the work queue. With the
defaults that is 5 deliveries over about 8 minutes.

A message that is not a valid trace is dead-lettered on first sight.

A dead letter's body is the original trace. Its headers say what happened:

| Header | Example |
| --- | --- |
| `Observer-Trace-Id` | `wrong-river-001` |
| `Observer-Dead-Reason` | `system_error` or `invalid_payload` |
| `Observer-Dead-Error` | `scorer: giving up after 4 attempts: kev returned HTTP 503: ...` |
| `Observer-Deliveries` | `5` |
| `Observer-Dead-At` | `2026-10-12T08:42:48Z` |
| `Observer-Source-Stream`, `Observer-Source-Sequence` | `OBSERVER_TRACES`, `17` |

Each dead-lettered trace also produces an `ERROR` log line, `trace dead-lettered`.

Inspecting and replaying, with the compose stack up:

```sh
make dlq      # how many dead letters there are

# The NATS CLI, run from a container on the compose network:
alias nats='docker run --rm -i --network observer_default natsio/nats-box:latest nats -s nats://nats:4222'

nats stream get OBSERVER_TRACES_DLQ 1          # one dead letter, with headers

# Replay it once Kev is back: post its body to the API again.
nats stream get OBSERVER_TRACES_DLQ 1 --json | jq -r .data | base64 -d \
  | curl -X POST localhost:8080/v1/traces -d @-

nats stream purge OBSERVER_TRACES_DLQ          # clear the dead letters
```

NATS monitoring is at http://localhost:8222 (`/jsz?streams=true` for stream
and consumer counts).

## Exporters

Kept traces go to every back end in `exporters`, over OTLP/HTTP. The default
is the local Phoenix:

```yaml
exporters:
  - name: phoenix
    endpoint: http://localhost:6006/v1/traces
    resource_attributes:
      openinference.project.name: observer-ai   # the Phoenix project
```

Add more entries to send to more back ends. Langfuse, for example, takes
Basic auth built from its public and secret keys
([`../examples/langfuse.yaml`](../examples/langfuse.yaml)):

```yaml
  - name: langfuse
    endpoint: https://cloud.langfuse.com/api/public/otel/v1/traces
    headers_env: LANGFUSE_OTLP_HEADERS
```

```sh
export LANGFUSE_OTLP_HEADERS="Authorization=Basic%20$(printf '%s' "pk-lf-...:sk-lf-..." | base64)"
```

**Headers.** An exporter's headers are its `headers`, plus those parsed from
the variable `headers_env` names, which win on a name clash. The variable
uses the OpenTelemetry format: `key=value` pairs separated by commas, values
percent-encoded where needed (`%20` for a space). The engine logs exporter
names and endpoints, never header values, and a header that fails to parse
is reported without its value.

**Asynchronous.** Exporting never holds up scoring. Each exporter has its own
bounded queue (`queue_size`) and its own workers (`concurrency`); a kept
trace is queued to every exporter and the worker moves on. The queue is in
NATS JetStream when `queue.url` is set (stream `OBSERVER_EXPORTS_<NAME>`, so
queued exports survive a restart), else in memory; see
[Where the queues live](#where-the-queues-live). A slow or unreachable back
end only fills its own queue: past `queue_size`, traces are skipped for that
back end alone and each skip is reported to the error sinks (`kind=full`,
`exporter=<name>`). The decision log's `exported` field says whether at least
one exporter queued the trace.

**Retries.** A failed send is retried after `backoff` (default 1s, 5s, 30s),
up to `max_deliver` tries (default 3); each retry is reported. Then the
exporter gives up: the trace is reported as a severe error and, on NATS,
moved to `OBSERVER_EXPORTS_DLQ` with the exporter's name, from where it can be
replayed. An error a retry cannot fix (corrupt stored spans) is given up at
once. Export failures never send the trace to the trace dead-letter stream:
it was scored, only its delivery failed.

**Shutdown.** In memory, the engine sends what is queued, within 10 seconds.
On NATS it stops after the sends in flight; the rest stays queued for the
next start.

`/v1/traces:score` waits until every exporter has sent its trace before it
answers, so its `exported` field covers all of them.

### What the back ends receive

The engine writes [OpenInference](https://github.com/Arize-ai/openinference)
attributes, which Phoenix reads natively and Langfuse and other OTLP back
ends show as span attributes, plus its own `eval.*` and `observer.*`
attributes.

**A kept trace that arrived over OTLP** is forwarded as its own spans: same
trace ID, span IDs, names, timings and attributes. Two things change: the
exporter's `resource_attributes` are set on every resource (replacing
attributes of the same name), and the root span gains the decision
attributes below. The agent's own `service.name` is kept.

**A kept trace from the JSON API** has no spans of its own, so the engine
builds them, as a new trace with a generated trace ID. Resource:
`service.name=observer-engine` plus the exporter's `resource_attributes`.
A root span `agent_run`, and one child span per tool call, named after the
tool.

Root span attributes:

| Attribute | Type | Set on | Value |
| --- | --- | --- | --- |
| `openinference.span.kind` | string | rebuilt | `AGENT` |
| `input.value` | string | rebuilt | The question |
| `output.value` | string | rebuilt | The final answer |
| `observer.source_trace_id` | string | rebuilt | The client's `trace_id` |
| `observer.evidence.<i>` | string | rebuilt | Each evidence item, 0-based, untruncated |
| `eval.agent` | string | both | The trace's agent; empty if it named none |
| `eval.decision` | string | both | `keep` (dropped traces are not exported) |
| `eval.reason` | string | both | Why: `kev` (the model), `random_floor`, or `fallback` (the model could not score it) |
| `eval.severity` | string | both | `high` or `normal` |
| `eval.model` | string | both | The scorer model that decided |
| `eval.latency_ms` | double | both | Scoring time in milliseconds |
| `eval.p_keep` | double | both | Calibrated probability the answer is wrong; omitted on a fallback keep |
| `eval.p_keep_raw` | double | both | The model's probability before calibration; omitted on a fallback keep |

Tool span attributes (rebuilt traces only; forwarded tool spans are not
changed):

| Attribute | Type | Value |
| --- | --- | --- |
| `openinference.span.kind` | string | `TOOL` |
| `tool.name` | string | The tool's name |
| `input.value` | string | Its arguments, as compact JSON; omitted when there are none |
| `output.value` | string | Its status |

Resource attributes worth setting per back end:

| Attribute | Back end | Effect |
| --- | --- | --- |
| `openinference.project.name` | Phoenix | The project the traces are filed under |
| `deployment.environment` | any | Lets you filter production from staging |
| `service.version` | any | The agent version the traces came from |

Rebuilt spans go through a batch span processor per exporter; forwarded spans
are sent at once, with a 10-second timeout per attempt.

## Metrics

With `observability.metrics.endpoint` set, the engine sends its own
OpenTelemetry metrics over OTLP/HTTP every `interval`, to a Collector or any
back end that takes OTLP metrics:

```yaml
observability:
  metrics:
    endpoint: http://otel-collector:4318/v1/metrics
    headers_env: METRICS_OTLP_HEADERS     # optional, "key=value,..."
    resource_attributes:
      deployment.environment: production
```

| Instrument | Kind | Attributes | Measures |
| --- | --- | --- | --- |
| `http.server.request.duration` | histogram (s) | `observer.server` (`api`, `otlp`), `http.request.method`, `http.route`, `http.response.status_code` | JSON API and OTLP/HTTP requests |
| `rpc.server.call.duration` | histogram (s) | `observer.server`, `rpc.system`, `rpc.method`, `rpc.grpc.status_code` | OTLP/gRPC calls |
| `observer.scorer.duration` | histogram (s) | `observer.agent`, `observer.model` | Model calls, retries included |
| `observer.export.duration` | histogram (s) | `observer.exporter` | Sends to a trace back end |
| `observer.decisions` | counter | `observer.agent`, `observer.model`, `observer.decision`, `observer.reason` | Decided traces |
| `observer.errors` | counter | `observer.component`, `error.type` | Errors reported to the error sinks |

A failed request or call also carries `error.type`: the status code (5xx for
HTTP, any non-OK gRPC code), or the error kind (`unavailable`, `timeout`,
`rejected`, ...) for model calls and sends. An error rate is the count with
`error.type` over the total count; a latency percentile comes from the
histogram. The resource has `service.name=observer-engine` plus
`resource_attributes`. As with exporters, the global
`OTEL_EXPORTER_OTLP_HEADERS` is not read. The last metrics are sent on
shutdown.

## The slice and its golden files

The model was trained on the output of the Python `build_state`, so
`BuildState` must reproduce it byte for byte. That includes Python's `repr()`
for tool arguments (`{'query': 'x'}`, `True`, `None`, `1e+16`, `'\xa0'`) and
evidence truncated to 1500 Unicode code points rather than bytes.

- `testdata/traces/*.json`: input fixtures
- `testdata/golden/*.txt`: what Python produces for each
- `testdata/gen_golden.py`: holds the Python `build_state` and writes the golden files

`TestBuildStateGolden` fails if Go and Python disagree on any fixture. A
second test sends every fixture through the queue and checks the worker
builds the same slice.

**To regenerate the golden files** after adding a fixture or changing the
training code's `build_state` (update the copy in `gen_golden.py` first):

```sh
make golden   # python3 testdata/gen_golden.py
make test
```

For a broader check, `make fuzz-golden` compares Go and Python over about
4,000 randomized traces, including every Unicode code point, random floats and
random nested arguments. It needs Python and is not part of `make test`.

Known differences from Python, all at the edges:

- Lone UTF-16 surrogates (`"\ud800"`) become U+FFFD in Go; Python keeps them.
- `NaN` and `Infinity` literals are rejected as invalid JSON; Python accepts them.
- Arguments nested more than 200 levels deep are rejected with a 400.
- Which characters `repr()` escapes depends on the Unicode version. Go 1.26 and
  Python 3.12 agree on every code point; re-run `make fuzz-golden` after
  changing either.

## Known limitations

These are open problems in the current version, not design choices to rely on.

- **Dead letters are not replayed automatically.** A Kev outage longer than
  the redelivery window (about 8 minutes with the defaults) sends every queued
  trace to `OBSERVER_TRACES_DLQ`, and they stay there until someone replays
  them by hand (see [Redelivery and the dead-letter stream](#redelivery-and-the-dead-letter-stream)).
  Nothing alerts on the dead-letter count either; watch for `trace dead-lettered`
  log lines or run `make dlq`.
- **A Kev that rejects everything causes silent random decisions.** A 400, 413
  or 422 is treated as that trace's problem and gets the random fallback. If
  Kev starts answering that way for every request (for example after a change
  to its request schema), every trace is decided at random, about 98% dropped
  with the default `random_floor`, and only `WARN` log lines show it.
- **The engine cannot tell which model Kev is serving.** Kev answers 200 for
  any model name, so a Kev serving a different model than the manifest
  describes goes unnoticed, and the manifest's temperature is applied to it
  anyway.
- **An exporter can lose kept traces.** A trace skipped because the
  exporter's queue was full is not sent to that back end. One it gave up on
  after `max_deliver` tries is lost in memory mode; on NATS it waits in
  `OBSERVER_EXPORTS_DLQ`.
- **In memory mode, a crash loses queued work.** Traces waiting to be scored,
  spans waiting to be assembled and traces waiting to be exported live only
  in the process. Use NATS (`queue.url`) where that matters.
- **Plain instrumentor spans do not reproduce the training slice exactly.**
  See [Matching what the model was trained on](#matching-what-the-model-was-trained-on).
  The test agent does not set the `observer.*` attributes yet, so its traces
  are scored on a slightly different slice than the model saw in training.
  On the one run compared, the raw score moved from 0.259 to 0.266.
- **Trace completeness is a guess.** A trace is assembled after
  `otlp.idle_timeout` of silence. Spans that arrive later are assembled as a
  separate, partial trace, which is normally unscorable and discarded; the
  first decision is not revisited. An agent run that pauses for longer than
  the idle timeout between spans is split this way.
- **The OTLP receivers have no authentication or TLS.** Anyone who can reach
  4317 or 4318 can submit spans.
- **A trace can be scored and exported twice.** Delivery is at least once and
  nothing deduplicates: a worker that dies between exporting and acknowledging
  leaves a duplicate in the back ends. The same happens if a client retries a
  `POST /v1/traces` that had in fact succeeded.
- **NATS has no authentication.** Anyone who can reach port 4222 can read,
  publish or delete traces. The compose file binds it to loopback, which is
  enough locally and nowhere else.

## Shutdown

On SIGINT or SIGTERM the engine stops accepting requests and OTLP exports,
stops pulling from the queue, and lets in-flight traces finish for up to
`server.shutdown_timeout`. Spans not yet assembled and traces still queued
stay in JetStream for the next start. If the timeout runs out, in-flight Kev calls are cancelled and those
traces go back to the queue; they are not dead-lettered and get no fallback
decision. Last, the exporters send the traces still queued for them and
shut down (up to 10 seconds in all).

## Tests

`make test` needs no network and no Docker. The queue tests run a real NATS
server in-process on a loopback port (`pkg/queue/queuetest`), so
redelivery, dead-lettering and stream limits are tested against JetStream
itself, not a fake.

## Layout

```
cmd/observer/      flags, config load, wiring, graceful shutdown
pkg/               the reusable engine, importable by other Go programs:
  pipeline/        the core: domain types, interfaces, model routing, calibration, the Engine
  trace/           trace types (with the agent), BuildState, Python repr
  fault/           the error type: kind, component, operation, trace
  observe/         the error pipeline and its sinks (stdout, file)
  metrics/         the engine's OpenTelemetry metrics
  config/          settings and manifests: loaders, compiler, validation, defaults
  scorer/          Kev client, its factory, the per-agent model builder
  policy/          keep/drop decision, random floor, severity
  broker/          where the queues live: NATS or in memory
  queue/           the trace queue and span store: JetStream or in memory
  export/          exporters: the async dispatcher, its queues, the OTLP/HTTP back end
internal/          this binary's own pieces:
  server/          HTTP handlers and the OpenAPI spec
  otlp/            OTLP gRPC and HTTP receivers, span-to-trace mapping
  common/          shared helpers: NATS connection and pull loop, worker pool, retry policy, request metrics
testdata/          trace fixtures, golden slices, fake Kev responses
scripts/           send_samples.py: post dataset traces to a running engine
Dockerfile         multi-stage build of the engine image
docker-compose.yml the engine, NATS JetStream and Phoenix
observer.yaml      example config with every default
observer.schema.yaml  JSON Schema for observer.yaml: every key, its type, default and limits
architecture.md    stack, package structure, design patterns, how the config is loaded
nats.conf          NATS server settings (JetStream, 8 MB message limit)
```

## Make targets

| Target | Does |
| --- | --- |
| `make build` | Build `bin/observer` |
| `make test` | `go vet ./...` and `go test -race ./...` |
| `make run` | Build and run on the host with `observer.yaml` (`CONFIG=path` to override) |
| `make nats` | Start only NATS in Docker, for `OBSERVER_NATS_URL=nats://localhost:4222 make run` |
| `make up` / `make down` | Start or stop the engine and NATS in Docker |
| `make logs` | Follow the engine container's log |
| `make dlq` | Show the dead-letter stream's state |
| `make lint` | gofmt check and `go vet` |
| `make golden` | Regenerate golden slice files |
| `make fuzz-golden` | Randomized Go-versus-Python slice comparison |
