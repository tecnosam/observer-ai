# Observer.ai inference engine

Decides which LLM agent traces are worth keeping for review. For each trace it
asks a fine-tuned decision model (Kev) whether the final answer is likely
wrong, applies a keep/drop policy, and exports the kept traces to
[Arize Phoenix](https://github.com/Arize-ai/phoenix).

This is the first, simple version: traces arrive over an HTTP JSON API. There
is no message queue and no OTLP ingestion.

```
POST /v1/traces ──> validate ──> bounded queue ──> workers ─┐
POST /v1/traces:score ──> validate ────────────────────────┤
                                                            v
                  BuildState (slice) ──> Kev /v1/systemone ──> policy ──> Phoenix (kept only)
```

## Setup

Requirements:

- Go 1.26 or newer (the OpenTelemetry Go SDK v1.47 requires it)
- Python 3, only for regenerating the golden slice files
- At runtime: a Kev server and a Phoenix instance (neither is needed for tests)

```sh
make test     # go vet + go test -race, fully offline
make run      # build and start with observer.yaml
```

With Kev on port 8010 and Phoenix on port 6006:

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
python3 scripts/send_samples.py            # 30 held-out traces through the queue
python3 scripts/send_samples.py -n 60 --sync   # print each decision next to its label
```

It samples equally across the `wrong`, `partial` and `correct` labels and
sends only the fields the API accepts.

Flags: `-config PATH` (default `observer.yaml`) and `-log-level debug|info|warn|error`.

## Configuration

`observer.yaml` documents every key with its default. Only `scorer.manifest`
is required.

| Key | Default | Meaning |
| --- | --- | --- |
| `server.addr` | `:8080` | Listen address |
| `server.workers` | `8` | Workers scoring traces from `POST /v1/traces` |
| `server.queue_size` | `1000` | Traces that may wait; beyond this the API returns 503 |
| `server.shutdown_timeout` | `30s` | Time to drain the queue on shutdown |
| `scorer.endpoint` | `http://localhost:8010/v1/systemone` | Kev endpoint |
| `scorer.model` | `kev-latest` | Model name sent to Kev |
| `scorer.manifest` | (required) | Path to the fine-tuning `manifest.json` |
| `scorer.decision_question` | `answer_wrong` | Manifest question that drives the decision; must be `noul` |
| `scorer.timeout` | `10s` | Timeout per attempt |
| `scorer.max_retries` | `3` | Retries after the first attempt |
| `scorer.backoff` | `[500ms, 2s, 5s]` | Base delay before each retry; the last value repeats |
| `policy.keep_threshold` | `0.5` | Keep when calibrated `p_keep` reaches this |
| `policy.random_floor` | `0.02` | Share of below-threshold traces kept at random |
| `alert.severity_high` | `0.8` | `p_keep` at which a kept trace is `high` severity |
| `export.phoenix_endpoint` | `http://localhost:6006/v1/traces` | Phoenix OTLP/HTTP URL, including the path |
| `export.project` | `observer-ai` | Phoenix project |

Environment overrides: `OBSERVER_SCORER_ENDPOINT` and
`OBSERVER_PHOENIX_ENDPOINT`. The standard OpenTelemetry variables also apply
to the exporter, for example `OTEL_EXPORTER_OTLP_HEADERS` for a Phoenix API key.

The engine validates the config at start-up and exits with a clear error if a
threshold is outside [0, 1], a duration does not parse, a key is unknown, or
the decision question is missing from the manifest or is not `noul`.

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

## API

### `POST /v1/traces`

Queues a trace and returns immediately.

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

| Status | Body | When |
| --- | --- | --- |
| 202 | `{"trace_id": "..."}` | Queued |
| 400 | `{"error": "..."}` | Invalid JSON or a missing required field |
| 413 | `{"error": "..."}` | Body larger than 8 MiB |
| 503 + `Retry-After` | `{"error": "..."}` | Queue full, or the server is shutting down |

### `POST /v1/traces:score`

Same input, processed synchronously. Meant for testing and demos. It returns
the decision and flushes the export, so the trace is in Phoenix when the
response arrives.

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
if Kev answers `GET /v1/models` on the scorer host within 2 seconds, and 503
otherwise.

## How a decision is made

1. **Slice.** `trace.BuildState` renders the trace into the text the model was
   trained on (see [The slice](#the-slice-and-its-golden-files)).
2. **Score.** The slice and the manifest questions go to Kev. Timeouts,
   connection errors and 5xx responses are retried with jittered backoff
   (between half and all of the configured delay). 4xx and malformed responses
   are not retried.
3. **Calibrate.** `p_keep = sigmoid(logit(p_raw) / T)`, with `p_raw` clamped
   to [1e-6, 1 - 1e-6].
4. **Decide.**
   - `p_keep >= keep_threshold`: keep, reason `kev`.
   - Otherwise keep with probability `random_floor`, reason `random_floor`.
   - Otherwise drop, reason `below_threshold`.
   - If Kev failed after all retries: keep with probability `random_floor`,
     otherwise drop. Reason `fallback` either way. A trace is never blocked or
     lost because Kev is down.
5. **Severity.** `high` if kept and `p_keep >= alert.severity_high`, `normal`
   for other keeps, `none` for drops. This version only records severity; it
   sends no alerts.
6. **Export.** Kept traces go to Phoenix. Each decision produces one JSON log
   line with `trace_id`, `decision`, `p_keep`, `reason` and `latency_ms`.

## What Phoenix receives

Resource attributes: `service.name=observer-engine` and
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
`trace export failed`; those spans are not retried later.

## The slice and its golden files

The model was trained on the output of the Python `build_state`, so
`BuildState` must reproduce it byte for byte. That includes Python's `repr()`
for tool arguments (`{'query': 'x'}`, `True`, `None`, `1e+16`, `'\xa0'`) and
evidence truncated to 1500 Unicode code points rather than bytes.

- `testdata/traces/*.json`: input fixtures
- `testdata/golden/*.txt`: what Python produces for each
- `testdata/gen_golden.py`: holds the Python `build_state` and writes the golden files

`TestBuildStateGolden` fails if Go and Python disagree on any fixture.

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

## Shutdown

On SIGINT or SIGTERM the engine stops accepting requests, lets the workers
finish the queue for up to `server.shutdown_timeout`, then flushes the
exporter (up to 10 seconds). If the drain times out, in-flight Kev calls are
cancelled and the remaining traces get `fallback` decisions.

## Layout

```
cmd/observer/      flags, config load, wiring, graceful shutdown
internal/config/   YAML config and manifest loading, validation, defaults
internal/trace/    trace types, BuildState, Python repr
internal/scorer/   Kev client, retries, calibration
internal/policy/   keep/drop decision, random floor, severity
internal/export/   Phoenix exporter (OTLP over HTTP)
internal/server/   HTTP handlers, pipeline, bounded worker pool
testdata/          trace fixtures, golden slices, fake Kev responses
scripts/           send_samples.py: post dataset traces to a running engine
```

## Make targets

| Target | Does |
| --- | --- |
| `make build` | Build `bin/observer` |
| `make test` | `go vet ./...` and `go test -race ./...` |
| `make run` | Build and run with `observer.yaml` (`CONFIG=path` to override) |
| `make lint` | gofmt check and `go vet` |
| `make golden` | Regenerate golden slice files |
| `make fuzz-golden` | Randomized Go-versus-Python slice comparison |
