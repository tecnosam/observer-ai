# Observer.ai

Observer.ai decides which LLM agent traces a person should look at.

An agent in production produces far more traces than anyone can review, and
the few that matter are the ones where its answer was wrong. Observer.ai
scores every trace with a small decision model, **Kev**, that estimates how
likely the final answer is wrong. It keeps the suspicious ones (plus a small
random sample) for review in [Arize Phoenix](https://github.com/Arize-ai/phoenix),
and drops the rest. Reviewers' verdicts can then be fed back to fine-tune Kev
on that agent's own mistakes.

## How it fits together

```
 your agent ──OTLP spans──> engine ──> Kev: p(answer is wrong) ──> keep or drop
 (instrumented)            (Go)                                       │
                                                                      │ kept traces
                                                                      v
                                                             Phoenix: people review
                                                             and annotate them
                                                                      │
                                                                      │ annotated traces
                                                                      v
 engine serves the new Kev <── run directory <── observer-finetune: train and evaluate
```

1. The agent exports its traces as OpenTelemetry spans (OpenInference
   conventions) to the engine.
2. The engine turns each trace into a short text slice (question, tool calls,
   evidence, final answer) and asks Kev how likely the answer is wrong.
3. A policy keeps traces above a threshold, plus a random share of the rest,
   and forwards the kept ones to Phoenix with the score attached.
4. People review them in Phoenix and annotate whether the answer was correct.
5. The fine-tuning SDK pulls those annotated traces, fine-tunes Kev, checks the
   result against the released model and a no-model baseline, and writes a
   run the engine can serve.

## What is in this repository

| Path | What it is |
|---|---|
| [`inference-v1/examples/`](inference-v1/examples/) | Example engine configs: scoring with the hosted Jev, serving an SDK run, JSON API only, a larger review budget, remote services |
| [`inference-v1/engine/`](inference-v1/engine/) | The engine, in Go: OTLP receivers, a JSON API, a work queue (NATS JetStream, or in memory with no NATS), the Kev client, the keep/drop policy and exporters to any OTLP back end (Phoenix, Langfuse, ...). Runs with Docker Compose together with NATS and Phoenix. See its [README](inference-v1/engine/README.md) and [architecture notes](inference-v1/engine/architecture.md) |
| [`finetune/observer_finetune/`](finetune/observer_finetune/) | The fine-tuning SDK: labeled traces in, a fine-tuned Kev and its evaluation out. Reads JSONL, OTLP span files or a Phoenix project. See its [README](finetune/observer_finetune/README.md) |
| [`finetune/testagents/`](finetune/testagents/) | A small ReAct agent (Google ADK, a local model through Ollama) that answers HotpotQA-style questions with search tools. It generated the eval dataset, and can export its traces to the engine or be used from a chat UI. See its [README](finetune/testagents/README.md) |
| [`finetune/notebooks/`](finetune/notebooks/) | The research behind the SDK, in order: building the labeled dataset, fine-tuning Kev, fine-tuning Laya, comparing Kev, Laya and Jev, and scoring and exporting |
| `sample-state.json`, `sample-questions.json` | A sample decision request for trying a Kev server by hand: a trace slice and three example questions about it |

The notebooks:

| Notebook | What it does |
|---|---|
| `observer_eval_dataset_prep.ipynb` | Runs the test agent over HotpotQA questions, with some runs given seeded faults, and labels every answer against the gold answer: `data/eval/labeled_traces.jsonl` |
| `observer_kev_lora_finetune.ipynb` | Fine-tunes Kev on that dataset and evaluates it as a sampler; the logic the SDK packages |
| `observer_laya_finetune.ipynb` | The same for Laya, an encoder-based decision model |
| `observer_model_comparison.ipynb` | Fine-tuned Kev, fine-tuned Laya and the hosted Jev on the same test traces, side by side |
| `observer_score_and_export.ipynb` | Scores one trace with a fine-tuned Kev, decides keep or drop, and sends a kept trace to Phoenix: the engine's job, done by hand in Python |

## Getting started

You need Docker, Go 1.26+, Python 3.12+ with [uv](https://docs.astral.sh/uv/),
and a Mac with Apple Silicon or an NVIDIA GPU for the models. The test agent
also needs [Ollama](https://ollama.com) with `qwen2.5:7b` pulled.

**1. Serve Kev.** The engine needs a Kev server. From a
[Kev](https://github.com/jaredpalmer/kev) checkout (the SDK keeps one in
`~/.cache/observer-finetune/`):

```sh
uv run --extra serve python -m kev.serve --run jaredpalmer/kev-0.8b --port 8010   # the released model
uv run --extra serve python -m kev.serve --run /path/to/run/checkpoint --port 8010  # or a fine-tuned one
```

**2. Start the engine, NATS and Phoenix.**

```sh
cd inference-v1/engine
make up          # engine on :8080 (API, Swagger UI at /docs), OTLP on :4318 and :14317, Phoenix on :6006
make logs        # one line per decision
```

**3. Send it traces.** Talk to the test agent in its chat UI with exporting on,
and every message becomes a trace the engine scores:

```sh
cd finetune/testagents
make corpus                  # once: build the paragraphs the agent searches
make chat EXPORTING=1        # opens the chat UI and prints sample questions
```

Kept traces appear in Phoenix at http://localhost:6006, in the `observer-ai`
project, with the engine's score on the root span.

**4. Fine-tune Kev on reviewed traces.** Annotate traces in Phoenix with a
`correctness` annotation, or bring your own labeled JSONL, then:

```sh
cd finetune/observer_finetune
make train DATA=phoenix://localhost:6006/observer-ai OUT=runs/my-run
make train DATA=traces.jsonl OUT=runs/my-run
```

The run's `report.md` says whether the new model beats the released one and
the baseline, and how to serve it. Point the engine's `scorer.manifest` at the
run's `manifest.json`.

## Ports used

| Port | Service |
|---|---|
| 8080 | Engine JSON API and Swagger UI (`/docs`) |
| 4318 | Engine OTLP over HTTP |
| 14317 | Engine OTLP over gRPC (4317 inside its container) |
| 6006 | Phoenix UI and OTLP over HTTP |
| 4317 | Phoenix OTLP over gRPC |
| 4222, 8222 | NATS clients and monitoring (loopback only) |
| 8010 | Kev, the model the engine calls |
| 8009, 8011, 8012 | Extra model servers the notebooks use: zero-shot Kev, zero-shot Laya, fine-tuned Laya |
| 11434 | Ollama, for the test agent |
| 8000 | The test agent's chat UI |
