# observer-finetune

Fine-tune Kev, Observer.ai's decision model, on your own labeled agent traces.

You give it traces where you know whether the agent's final answer was right.
It trains Kev to spot the wrong ones, checks whether the result is actually
better than the released model and than simple rules, and writes a run
directory that the Observer.ai engine can serve.

```sh
uv run observer-finetune train --data traces.jsonl --out runs/my-run
```

## What it does

Kev reads a slice of an agent trace (the question, the tool calls, the
evidence and the final answer) and gives the probability that the answer is
wrong. The engine uses that probability to decide which traces a person
should review. A Kev fine-tuned on your agent's traces should find its
mistakes better than the general-purpose one.

One `train` run:

1. **Loads** your traces and checks them: both correct and wrong traces in
   every portion, enough of them to measure anything.
2. **Splits** them by question, never by trace, into train, calibration and
   test. Two runs of the same question always land on the same side, so the
   test measures judgement, not memory.
3. **Sizes Kev's context window** from your data with Kev's own tokenizer, and
   stops before training if more than 2% of traces would not fit.
4. **Scores the test traces with the released Kev**, to have a baseline.
5. **Fine-tunes Kev** from its released checkpoint (a LoRA adapter and the
   decision head; the base model stays frozen).
6. **Scores the calibration and test traces with your fine-tuned Kev** and fits
   a temperature so its probabilities mean what they say.
7. **Compares** the released Kev, your fine-tuned Kev and a no-model heuristic
   on the test traces, and writes everything to the run directory.

The checkpoint is always written. If the fine-tuned model is not better than
the released one, or not better than the heuristic, the run says so in a
warning, in `report.md` and in `manifest.json`.

## Install and run

Needs Python 3.12+, [uv](https://docs.astral.sh/uv/) and git. Training runs on
Apple Silicon (MPS) or an NVIDIA GPU (CUDA).

```sh
cd finetune/observer_finetune
uv sync

uv run observer-finetune check --data traces.jsonl                  # validate and show the split, no training
uv run observer-finetune train --data traces.jsonl --out runs/my-run
```

The Makefile wraps the same commands:

```sh
make install
make check DATA=traces.jsonl
make train DATA=traces.jsonl OUT=runs/my-run
make test                     # offline tests
make example-train            # fine-tune on the bundled example
```

From Python:

```python
from observer_finetune import finetune, check, TrainOptions

report = check("traces.jsonl")
print(report.splits, report.problems)

run = finetune("traces.jsonl", "runs/my-run")
print(run.checkpoint, run.warnings)
```

The first run clones Kev (pinned to a known revision) into
`~/.cache/observer-finetune/` and builds its environment. That takes a few
minutes and prints nothing while it downloads.

### Try it on the example

`example/traces.jsonl` is a synthetic eval set in the expected shape: 849
traces over 600 questions about a made-up world. `example/generate.py` rebuilds
it.

```sh
uv run observer-finetune check --data example/traces.jsonl
uv run observer-finetune train --data example/traces.jsonl --out example/runs/demo
```

Training takes about 10 minutes on an M3 Max, plus a few minutes to start the
servers and score. Add `--max-steps 3` for a quick check that everything is
wired up.

The example is easy by construction (every label follows from the facts in
the evidence), so a fine-tuned Kev scores near perfectly on it. That shows
the pipeline works end to end; it says nothing about how Kev will do on your
agent's traces.

### Options

| CLI | Python | Default | |
|---|---|---|---|
| `--data` | `data` | required | The traces: a JSONL file, OTLP span lines, or `phoenix://host:port/project` |
| `--out` | `out` | required | The run directory to create. It must not already hold a checkpoint |
| `--annotations` | `annotations` | | The labels for an OTLP span file |
| `--label-name` | `label_name` | `correctness` | The annotation that holds the label (OTLP files and Phoenix) |
| `--kev-url` | `kev_url` | | A server already running the released Kev, used for the baseline instead of starting one |
| `--kev-path` | `kev_path` | managed checkout | A Kev checkout to use instead of the managed one |
| `--epochs` | `TrainOptions(epochs=)` | 1 | Training epochs |
| `--device` | `TrainOptions(device=)` | `mps` on Apple Silicon, else `cuda` | |
| `--max-steps` | `TrainOptions(max_steps=)` | 0 (all) | Stop after this many optimizer steps, for a quick trial |

`TrainOptions` and `EvalOptions` (in `observer_finetune.schemas`) hold the
rest: learning rate, batch size, the context limit, the review budget and the
base rates the evaluation uses.

## The dataset

### Shape

A JSONL file, one trace per line:

```json
{
  "trace_id": "ex-00042",
  "qid": "q0031",
  "question": "Which river flows through the capital of Velmoka?",
  "tool_calls": [
    {"name": "search", "args": {"query": "capital of Velmoka"}, "status": "ok"},
    {"name": "lookup", "args": {"title": "Torsel"}, "status": "ok"}
  ],
  "evidence": [
    "Torsel is the capital of Velmoka.",
    "Torsel lies on the Rhuka River."
  ],
  "final_answer": "Belva River",
  "label": "wrong",
  "used_search": true,
  "stop_reason": "answered",
  "failure_type": "natural"
}
```

| Field | Required | Type | Meaning |
|---|---|---|---|
| `question` | yes | string | What the user asked the agent |
| `final_answer` | yes | string | What the agent answered. `""` for a run that gave no answer |
| `label` | yes, or `reference` | string or number | `correct`, `partial` or `wrong` (also `incorrect`, `pass`, `fail`), or a score from 0 to 1 where 1 is correct. Partial counts as wrong |
| `reference` | instead of `label` | string | The gold answer. Each trace is then labeled by token F1 against it: correct at 0.8, partial at 0.3, otherwise wrong |
| `tool_calls` | no | list | The calls the agent made, in order: `name` (required), `args` (any JSON) and `status` (`ok` if absent; anything else, such as `not_found`, marks a failed call) |
| `evidence` | no | list of strings | What the tools returned that the answer could rest on, in the order found. Kev reads the first 3, up to 1,500 characters each |
| `trace_id` | no | string | Your identifier. Defaults to the line number |
| `qid` | no | string | Groups runs of the same question so they stay on one side of the split. Without it, runs are grouped by their question text |
| `split` | no | `train` or `heldout` | Sets the split yourself. Used only when every trace has it |
| `used_search` | no | bool | Whether the agent searched at all. Feeds the heuristic baseline |
| `stop_reason` | no | string | `answered`, or why the run ended otherwise. Feeds the heuristic baseline |
| `failure_type` | no | string | Your own category for the trace; kept in the data splits |

The first six fields are the engine's trace record (`POST /v1/traces`), so a
trace means the same thing when training as when the engine scores it.

[RAGAS](https://docs.ragas.io) and DeepEval column names are read too:
`user_input` as `question`, `retrieved_contexts` as `evidence`, `response` as
`final_answer`, and `reference` (or `ground_truth`, `expected_output`).

Lines that cannot be read (no question, no answer, no usable label) are
skipped and counted, not fatal. `check` shows the counts.

### How much data

- Both correct and wrong traces in every portion. `check` reports a portion
  that has only one kind.
- At least a few hundred training traces; below 200 the result is noisy, and
  `check` says so.
- The test portion is held-out questions split in half again, so it is about a
  tenth of your questions. A small test portion gives wide error bars: read
  the standard deviations in `results.csv`.

### Other sources

**OTLP span files.** OpenInference spans as OTLP JSON lines, one export
request per line (what the OpenTelemetry Collector's file exporter writes),
plus a file of labels:

```sh
uv run observer-finetune train --data spans.jsonl --annotations labels.jsonl --out runs/my-run
```

```json
{"trace_id": "5b8efff798038103d269b633813fc60c", "name": "correctness", "label": "wrong"}
```

Spans are turned into traces with the engine's rules: the root span's
`input.value` and `output.value` are the question and answer, `TOOL` spans are
the tool calls, and `observer.evidence.N` on the root span is the evidence. An
annotation names a `trace_id` or any `span_id` of the trace, and carries a
`label` or a `score`.

**Phoenix.** The traces in a Phoenix project, labeled by the annotations people
made there:

```sh
uv run observer-finetune train --data phoenix://localhost:6006/observer-ai --out runs/my-run
```

This is the loop the engine is built for: it forwards the traces it keeps to
Phoenix, reviewers annotate them, and those annotations become the next
training set. Traces with no `correctness` annotation are skipped.

An OTLP endpoint itself cannot be a source: OTLP only sends traces, it has no
way to read them back. Phoenix is where they are kept.

## The run directory

```
runs/my-run/
  checkpoint/        the fine-tuned Kev
  manifest.json      what the engine reads: questions, temperatures, decision question, metrics, warnings
  report.md          the verdict, the results table and how to serve the run
  results.csv        every model, signal, base rate and review budget
  train.log          Kev's training log
  serve_*.log        logs of the Kev servers the run started
  data/              train.jsonl, calibration.jsonl, test.jsonl as used
```

To serve it, run Kev's server on the checkpoint and point the engine at the
manifest. `report.md` has the exact commands:

```sh
cd ~/.cache/observer-finetune/kev-5e42a7a03f28
uv run --extra serve python -m kev.serve --run /path/to/runs/my-run/checkpoint --port 8010
```

```yaml
# inference-v1/engine/observer.yaml
scorer:
  manifest: /path/to/runs/my-run/manifest.json
```

### Reading the results

All numbers are on the test portion, which no model was trained or calibrated
on.

- **AUROC**: how well a model ranks wrong traces above correct ones. 0.5 is
  chance, 1 is perfect.
- **Normalized recall at a 5% budget**: if a person can review only the top 5%
  of traces, the share of wrong ones caught, scaled so 0 is random sampling and
  1 is the best the budget allows. It is measured on test sets rebalanced so
  that 10% or 20% of traces are wrong, closer to production than a labeled set
  usually is, and averaged over 20 such sets.
- **The heuristic**: simple rules with no model (a short answer, few
  evidence items, no search, a failed tool call, no answer, little overlap
  between answer and evidence), combined by a logistic regression fitted on
  the training split. A fine-tuned Kev that cannot beat it is not earning its
  cost, which is what the warning tells you.

## How it is built

```
observer_finetune/
  __init__.py        finetune(), check(): the defaults wired together
  cli.py             observer-finetune check | train
  pipeline.py        FinetunePipeline: the steps above
  schemas.py         LabeledTrace, TrainOptions, EvalOptions, DataReport, RunResult
  state.py           the slice Kev reads; identical to the engine's BuildState
  questions.py       the questions Kev is trained on and asked
  labels.py          label names, scores and gold answers to correct / partial / wrong
  split.py           the split by question
  metrics.py         sampler metrics, calibration, the heuristic baseline
  report.py          manifest.json, results.csv, report.md
  sources/
    base.py          AbstractTraceSource
    jsonl.py         JsonlTraceSource
    otlp.py          OtlpFileTraceSource
    phoenix.py       PhoenixTraceSource
    spans.py         OpenInference spans to traces, as the engine reads them
    factory.py       build_source(): picks the source for --data
  models/
    base.py          AbstractModelFactory, AbstractTrainer, AbstractScorer
    kev/             KevModelFactory, KevTrainer, KevServer, KevWorkspace
```

`FinetunePipeline(source, factory, run_dir)` is given where the traces come
from and which model family to train, and only talks to their abstract
interfaces. A new data source is a new `AbstractTraceSource`; another model
family is a new `AbstractModelFactory` that builds its own trainer and
scorers. `tests/test_pipeline.py` runs the whole pipeline with a fake factory,
with no Kev, GPU or network.

```sh
uv run --group dev pytest
```
