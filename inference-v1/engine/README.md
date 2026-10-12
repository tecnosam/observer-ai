# Observer.ai inference engine

Decides which LLM agent traces are worth keeping for review. For each trace it
asks a fine-tuned decision model (Kev) whether the final answer is likely
wrong, applies a keep/drop policy, and exports the kept traces to
[Arize Phoenix](https://github.com/Arize-ai/phoenix).

Traces arrive as OpenTelemetry spans (OTLP over gRPC or HTTP) or over an HTTP
JSON API, and wait in a NATS JetStream queue. A worker pool in the same binary
pulls them off the queue and does the rest.

```
OTLP gRPC :4317 ─┐
OTLP HTTP :4318 ─┴─> spans stored by trace ID ──> assembled once the trace goes quiet ─┐
                     (stream OBSERVER_SPANS)                                            │
                                                                                        v
POST /v1/traces ──> validate ───────────────────────────────> JetStream stream OBSERVER_TRACES
                                                                                        │
                                                                                        v
                                             worker pool ──> slice ──> Kev ──> policy ──> Phoenix (kept only)
                                                  │
                               system error, after max_deliver attempts
                                                  v
                                    JetStream stream OBSERVER_TRACES_DLQ

POST /v1/traces:score ──> validate ──> slice ──> Kev ──> policy ──> Phoenix   (no queue)
```

## Setup

Requirements:

- Docker, to run NATS and (optionally) the engine itself
- Go 1.26 or newer, to build or test outside Docker
- Python 3, only for regenerating the golden slice files and sending samples
- A Kev server and a Phoenix instance (not needed for tests)

### With Docker Compose

```sh
make up       # docker compose up --build -d: the engine and NATS JetStream
make logs     # follow the engine's log
make down     # stop both; queued traces and dead letters stay in a volume
```

Compose starts two services:

| Service | Host ports | Notes |
| --- | --- | --- |
| `observer` | 8080 (JSON API), 4318 (OTLP HTTP), **14317** (OTLP gRPC) | The engine: receivers, API and worker pool |
| `nats` | 4222 (clients), 8222 (monitoring), both on loopback only | JetStream, stored in the `nats-data` volume; configured by `nats.conf` |

Inside the container the engine listens for OTLP gRPC on 4317. Compose
publishes it on host port 14317 because Phoenix, which usually runs on the
same host, already takes 4317 for its own OTLP gRPC receiver. Set
`OBSERVER_OTLP_GRPC_PORT=4317` if that port is free on your host.

Kev and Phoenix are not in the compose file. By default the engine container
reaches them on the Docker host at ports 8010 and 6006. Override with
environment variables when running compose:

| Variable | Default |
| --- | --- |
| `OBSERVER_SCORER_ENDPOINT` | `http://host.docker.internal:8010/v1/systemone` |
| `OBSERVER_PHOENIX_ENDPOINT` | `http://host.docker.internal:6006/v1/traces` |
| `OBSERVER_MANIFEST_FILE` | `../../finetune/notebooks/runs/observer-kev-0.8b-ft/manifest.json` (mounted into the container) |
| `OBSERVER_PORT` | `8080` |
| `OBSERVER_OTLP_GRPC_PORT` | `14317` |
| `OBSERVER_OTLP_HTTP_PORT` | `4318` |

On Linux, a Kev or Phoenix that listens only on `127.0.0.1` is not reachable
from a container; bind it to `0.0.0.0` or point the variables elsewhere.
Docker Desktop on macOS and Windows forwards to host loopback, so the
defaults work there.

### On the host

```sh
make nats     # start only NATS in Docker
make run      # build and start the engine with observer.yaml
make test     # go vet + go test -race, fully offline
```

`make run` binds 8080, 4317 and 4318. If Phoenix is running on the same host
it already holds 4317, and the engine exits with an "address already in use"
error; start it with `OBSERVER_OTLP_GRPC_ADDR=:14317 make run` instead.

### Try it

```sh
curl -X POST localhost:8080/v1/traces:score -d @testdata/traces/wrong_river.json
```

```json
{"trace_id":"wrong-river-001","decision":"keep","p_keep":0.626,"p_keep_raw":0.6087,"reason":"kev","severity":"normal","model":"kev-latest","latency_ms":78.5,"exported":true}
```

The trace then appears in the Phoenix project `observer-ai` with the `eval.*`
attributes on its root span.

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
python scripts/smoke.py --otel-endpoint http://localhost:4318/v1/traces
```

Flags: `-config PATH` (default `observer.yaml`) and `-log-level debug|info|warn|error`.

## Configuration

`observer.yaml` documents every key with its default. Only `scorer.manifest`
is required.

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
| `queue.url` | `nats://localhost:4222` | NATS server |
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
| `queue.dlq_max_age` | `168h` | How long dead letters are kept |
| `scorer.endpoint` | `http://localhost:8010/v1/systemone` | Kev endpoint |
| `scorer.model` | `kev-latest` | Model name sent to Kev |
| `scorer.manifest` | (required) | Path to the fine-tuning `manifest.json` |
| `scorer.decision_question` | `answer_wrong` | Manifest question that drives the decision; must be `noul` |
| `scorer.timeout` | `10s` | Timeout per attempt |
| `scorer.max_retries` | `3` | In-process retries after the first attempt |
| `scorer.backoff` | `[500ms, 2s, 5s]` | Base delay before each retry; the last value repeats |
| `policy.keep_threshold` | `0.5` | Keep when calibrated `p_keep` reaches this |
| `policy.random_floor` | `0.02` | Share of below-threshold traces kept at random |
| `alert.severity_high` | `0.8` | `p_keep` at which a kept trace is `high` severity |
| `export.phoenix_endpoint` | `http://localhost:6006/v1/traces` | Phoenix OTLP/HTTP URL, including the path |
| `export.project` | `observer-ai` | Phoenix project |

Environment overrides:

| Variable | Overrides |
| --- | --- |
| `OBSERVER_NATS_URL` | `queue.url` |
| `OBSERVER_SCORER_ENDPOINT` | `scorer.endpoint` |
| `OBSERVER_PHOENIX_ENDPOINT` | `export.phoenix_endpoint` |
| `OBSERVER_MANIFEST` | `scorer.manifest` |
| `OBSERVER_OTLP_GRPC_ADDR` | `otlp.grpc_addr` |
| `OBSERVER_OTLP_HTTP_ADDR` | `otlp.http_addr` |

The standard OpenTelemetry variables also apply to the exporter, for example
`OTEL_EXPORTER_OTLP_HEADERS` for a Phoenix API key.

The engine validates the config at start-up and exits with a clear error if:

- a threshold is outside [0, 1], a duration does not parse, or a key is unknown
- the decision question is missing from the manifest or is not `noul`
- `queue.ack_wait` is not longer than the worst-case scoring time (every Kev
  attempt timing out, plus backoff: 47.5s with the defaults), since a shorter
  value would hand a trace to a second worker while the first is still on it

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

## OTLP ingestion

The engine runs the standard OTLP trace receivers, so an agent's exporter can
point at it as if it were a collector:

| Protocol | Address | Details |
| --- | --- | --- |
| gRPC | `:4317` | `opentelemetry.proto.collector.trace.v1.TraceService/Export`, gzip accepted |
| HTTP | `:4318`, `POST /v1/traces` | `application/x-protobuf` or `application/json`, gzip accepted, 16 MiB limit |

An export is answered with success as soon as NATS has confirmed the spans
are stored, typically within a few milliseconds. If NATS cannot take them the
answer is gRPC `UNAVAILABLE` or HTTP 503, which OTLP exporters retry.

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
`otlp.idle_timeout` (10s) later, and the trace shows up in Phoenix if it was
kept.

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
5. **Forward.** A kept trace is sent to Phoenix as its original spans, with
   the original trace and span IDs, under the project `export.project`, with
   the `eval.*` attributes added to the root span. A dropped trace is sent
   nowhere and its spans are deleted.

Because of step 2, a decision arrives at least `otlp.idle_timeout` after the
trace's last span.

### How spans are read

The engine reads the [OpenInference](https://github.com/Arize-ai/openinference)
attributes that Phoenix's instrumentors emit.

| Trace field | Taken from |
| --- | --- |
| Root span | The span whose parent is not in the trace, preferring `openinference.span.kind` = `AGENT`, then `CHAIN` |
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

A trace is **not scorable**, and is discarded with a `spans discarded` log
line, when its root span has no `input.value` or no `output.value`. That
covers ordinary service spans sent to the engine by mistake, and agent runs
that ended without an answer.

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

Stores a trace in the queue. The 202 is sent only after NATS has confirmed
the trace, so an accepted trace survives a restart of the engine.

```json
{
  "trace_id": "7f3a91c2e8",
  "question": "Which river flows through the capital of France?",
  "tool_calls": [{"name": "search", "args": {"query": "capital of France"}, "status": "ok"}],
  "evidence": ["Paris is the capital of France.", "The Seine flows through Paris."],
  "final_answer": "The Seine"
}
```

- `question` and `final_answer` are required.
- `trace_id` is optional; the engine generates 32 hex characters if it is missing.
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
Phoenix when the response arrives.

```json
{
  "trace_id": "wrong-river-001",
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
| `exported` | The trace was kept and its flush to Phoenix did not fail |

### API docs: `GET /docs` and `GET /openapi.yaml`

`/openapi.yaml` serves the OpenAPI 3.0 description of this API. `/docs` renders
it with Swagger UI, where you can also send requests. The UI's scripts load
from a CDN, so `/docs` needs internet access in the browser; the spec does not.

The spec lives in `internal/server/openapi.yaml` and is embedded in the binary.
Update it when you change a handler; a test fails if a route, a documented
status code or a field of the decision response goes missing from it.

### `GET /healthz` and `GET /readyz`

`/healthz` returns 200 whenever the process is up. `/readyz` returns 200 only
if the NATS connection is up and Kev answers `GET /v1/models` on the scorer
host within 2 seconds, and 503 otherwise.

## How a decision is made

1. **Slice.** `trace.BuildState` renders the trace into the text the model was
   trained on (see [The slice](#the-slice-and-its-golden-files)).
2. **Score.** The slice and the manifest questions go to Kev. Timeouts,
   connection errors and 5xx responses are retried in-process with jittered
   backoff (between half and all of the configured delay).
3. **Calibrate.** `p_keep = sigmoid(logit(p_raw) / T)`, with `p_raw` clamped
   to [1e-6, 1 - 1e-6].
4. **Decide.**
   - `p_keep >= keep_threshold`: keep, reason `kev`.
   - Otherwise keep with probability `random_floor`, reason `random_floor`.
   - Otherwise drop, reason `below_threshold`.
5. **Severity.** `high` if kept and `p_keep >= alert.severity_high`, `normal`
   for other keeps, `none` for drops. This version only records severity; it
   sends no alerts.
6. **Export.** Kept traces go to Phoenix. Each decision produces one JSON log
   line with `trace_id`, `decision`, `p_keep`, `reason` and `latency_ms`.

### When Kev cannot score a trace

What happens depends on whose fault it is and which path the trace took.

| Failure | Queued (`POST /v1/traces`) | Sync (`:score`) |
| --- | --- | --- |
| **System error**: Kev unreachable, timing out, 5xx, 401/403/404/429, or an unparseable response | Redelivered, then dead-lettered. No decision is made. | Random fallback decision |
| **Kev rejects the trace**: 400, 413 or 422 | Random fallback decision | Random fallback decision |

The random fallback keeps the trace with probability `random_floor` and drops
it otherwise, with reason `fallback` either way.

## The queue

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

## What Phoenix receives

**A kept trace that arrived over OTLP** is forwarded as its own spans: same
trace ID, span IDs, names, timings and attributes. Two things change: the
resource's `openinference.project.name` is set to `export.project`, and the
root span gains `eval.decision`, `eval.p_keep`, `eval.p_keep_raw`,
`eval.reason`, `eval.severity`, `eval.model` and `eval.latency_ms`. The
forward is synchronous, so the decision log's `exported` field says whether
Phoenix accepted it.

**A kept trace from the JSON API** has no spans of its own, so the engine
builds them. Resource attributes: `service.name=observer-engine` and
`openinference.project.name=<export.project>`.

Per kept trace, a root span `agent_run`:

- `openinference.span.kind=AGENT`, `input.value` (question), `output.value` (final answer)
- `observer.source_trace_id`, and `observer.evidence.<i>` for each evidence item (0-based, untruncated)
- `eval.decision`, `eval.p_keep`, `eval.p_keep_raw`, `eval.reason`,
  `eval.severity`, `eval.model`, `eval.latency_ms`
  (`eval.p_keep` and `eval.p_keep_raw` are omitted on a fallback keep)

and one child span per tool call, named after the tool:
`openinference.span.kind=TOOL`, `tool.name`, `input.value` (args as JSON),
`output.value` (status).

Spans go through a batch span processor and are flushed on shutdown. The
client's `trace_id` is recorded as `observer.source_trace_id`; the OTel trace
ID is generated. If Phoenix is unreachable the engine keeps running and logs
`trace export failed`; those spans are not retried later, and a Phoenix
outage does not send traces to the dead-letter stream.

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
- **Kept traces are lost if Phoenix is down.** A failed export is logged
  (`trace export failed`) and the trace is still acknowledged: it is not
  retried and does not go to the dead-letter stream. For JSON traces the
  failure is only seen after the fact, in the exporter's batch. For OTLP
  traces the forward is synchronous and retried for about 15 seconds first.
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
  leaves a duplicate in Phoenix. The same happens if a client retries a
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
decision. Last, the exporter is flushed (up to 10 seconds).

## Tests

`make test` needs no network and no Docker. The queue tests run a real NATS
server in-process on a loopback port (`internal/queue/queuetest`), so
redelivery, dead-lettering and stream limits are tested against JetStream
itself, not a fake.

## Layout

```
cmd/observer/      flags, config load, wiring, graceful shutdown
internal/config/   YAML config and manifest loading, validation, defaults
internal/trace/    trace types, BuildState, Python repr
internal/scorer/   Kev client, retries, calibration, error classification
internal/policy/   keep/drop decision, random floor, severity
internal/otlp/     OTLP gRPC and HTTP receivers, span-to-trace mapping
internal/queue/    JetStream streams, publisher, span assembly, worker pool, dead-lettering
internal/export/   Phoenix exporter (OTLP over HTTP)
internal/server/   HTTP handlers, the pipeline, OpenAPI spec
testdata/          trace fixtures, golden slices, fake Kev responses
scripts/           send_samples.py: post dataset traces to a running engine
Dockerfile         multi-stage build of the engine image
docker-compose.yml the engine and NATS JetStream
nats.conf          NATS server settings (JetStream, 8 MB message limit)
```

## Make targets

| Target | Does |
| --- | --- |
| `make build` | Build `bin/observer` |
| `make test` | `go vet ./...` and `go test -race ./...` |
| `make run` | Build and run on the host with `observer.yaml` (`CONFIG=path` to override) |
| `make nats` | Start only NATS in Docker, for `make run` |
| `make up` / `make down` | Start or stop the engine and NATS in Docker |
| `make logs` | Follow the engine container's log |
| `make dlq` | Show the dead-letter stream's state |
| `make lint` | gofmt check and `go vet` |
| `make golden` | Regenerate golden slice files |
| `make fuzz-golden` | Randomized Go-versus-Python slice comparison |
