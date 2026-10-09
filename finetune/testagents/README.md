# observer_agent

A small ReAct agent that answers HotpotQA-style questions using only local search
tools over the paragraphs you pass in. It produces agent traces for Observer.ai,
which samples LLM agent traces by how likely the answer is wrong.

- Agent loop: Google ADK (`LlmAgent`, `Runner`, `InMemorySessionService`)
- Models: served locally by Ollama, reached through ADK's LiteLLM wrapper
- Retrieval: BM25 `search` and title `lookup` over the run's paragraphs. No web access.

## Setup

```sh
uv sync
ollama pull qwen2.5:7b      # default model, supports tool calling
ollama pull llama3.2:3b     # smaller second model
```

Optional: to let Ollama serve several runs at once (the notebook uses 4 worker
threads), start it with `OLLAMA_NUM_PARALLEL=4 ollama serve`.

Check that everything works:

```sh
uv run pytest                        # offline, no Ollama needed
uv run python scripts/smoke.py       # one two-hop question against Ollama
```

`scripts/smoke.py` prints the result as JSON and exits 0 when the answer is
"Seine" with at least one `ok` tool call, 1 when Ollama is unreachable or the run
fails, and 2 when the run completes with a wrong answer. It accepts `--model`,
`--base-url` and `--otel-endpoint`.

## Installing into another environment

From the environment that should use the package (for example the notebook's venv):

```sh
uv pip install -e /path/to/observer_agent
# or
pip install -e /path/to/observer_agent
```

`/path/to/observer_agent` is this directory, the one containing `pyproject.toml`.

## Interface

```python
from observer_agent import AgentConfig, run_agent, arun_agent

@dataclass(frozen=True)
class AgentConfig:
    model: str = "qwen2.5:7b"                     # Ollama model name, without the "ollama_chat/" prefix
    base_url: str = "http://localhost:11434/v1"   # Ollama endpoint
    max_tool_calls: int = 6                       # hard budget across all tools in one run
    max_steps: int = 10                           # max LLM turns before giving up
    otel_endpoint: str | None = None              # e.g. "http://localhost:6006/v1/traces"; None disables tracing
    temperature: float = 0.0
    request_timeout_s: float = 120.0

def run_agent(question: str, paragraphs: list[dict], cfg: AgentConfig) -> dict: ...
async def arun_agent(question: str, paragraphs: list[dict], cfg: AgentConfig) -> dict: ...
```

`paragraphs` is a list of `{"title": str, "text": str}`.

```python
result = run_agent(
    "Which river flows through the capital of France?",
    [{"title": "Paris", "text": "Paris is the capital of France. It lies on the Seine."}],
    AgentConfig(),
)
print(result["final_answer"], result["stop_reason"])
```

`run_agent` is synchronous. It works from plain Python, from worker threads and
inside Jupyter: when the calling thread already runs an event loop, the run
happens on a helper thread with its own loop. Every run builds its own paragraph
store, tool budget, agent and session, so concurrent runs share nothing.

`base_url` may include the `/v1` suffix; it is dropped because LiteLLM's
`ollama_chat` provider uses Ollama's native API.

### Errors

Infrastructure failures raise: Ollama unreachable, model not pulled, request
timeout, any unexpected exception. A model that merely fails to answer returns
normally, with `stop_reason` set to `"max_steps"` or `"no_answer"`.

## Return value

```python
{
    "trace_id": str,           # 32 lowercase hex chars; the OTel trace id when tracing is on, else uuid4().hex
    "final_answer": str,       # cleaned short answer, "" if none
    "final_answer_raw": str,   # last model text before cleaning
    "tool_calls": [            # in call order
        {"name": "search" | "lookup", "args": dict, "status": str, "result_preview": str},
    ],
    "evidence": [str],         # full text of every paragraph returned to the model, deduplicated, in first-seen order
    "steps": [                 # one entry per tool call
        {"thought": str, "tool": str, "args": dict, "observation": str},
    ],
    "used_search": bool,       # True if at least one tool call returned status "ok"
    "stop_reason": "answered" | "max_steps" | "no_answer",
    "model": str,
    "latency_ms": float,
}
```

- `status` is one of `ok`, `not_found`, `bad_args`, `budget_exhausted`.
- `result_preview` is the first 300 characters of what the tool returned;
  `steps[i]["observation"]` holds the full text.
- `thought` is any text the model produced alongside the tool call, or `""`.
- `evidence` entries are the paragraph `text` values, without the `[Title]` prefix.
- `stop_reason`: `answered` when the model gave a non-empty final reply;
  `no_answer` when its final reply was empty; `max_steps` when it used all
  `max_steps` LLM turns without a final reply (`final_answer` and
  `final_answer_raw` are then `""`).

Two details worth knowing when slicing traces:

- If the model calls a tool that does not exist, or omits a required argument,
  ADK answers the call itself and the tool never runs. Such calls are still
  logged, with status `bad_args`, and they spend tool budget. For a made-up tool
  the logged `name` is whatever the model called, so it can be something other
  than `search` or `lookup`.
- A `search` whose query shares no word with any paragraph returns status
  `not_found`.
- qwen2.5:7b sometimes writes a tool call as plain text (`search {"query": "..."}`)
  instead of issuing a structured call. A reply that consists of nothing but such
  a call to `search` or `lookup` is executed as a normal tool call. Without this,
  those runs would end with the call text as their "answer" and no tool calls.
- When `max_steps` is reached the model is not called again; the run ends there.

## Tools

Built fresh for each run over that run's paragraphs.

- `search(query)`: BM25 over title + text. Returns the top 3 as `[Title] text`
  blocks separated by blank lines. An empty query gives `bad_args`.
- `lookup(title)`: exact title match, then case-insensitive, then the closest
  title by string similarity (ratio of at least 0.6). No match gives `not_found`
  and a message listing the available titles.
- Budget: every tool call counts. Once `max_tool_calls` is reached the tool is
  not executed and returns "Tool budget exhausted. Answer now using what you
  already have." with status `budget_exhausted`. This is enforced inside the
  tools, not only in the prompt.

## Final answer cleaning

Prefixes such as `Final answer:`, `Answer:` and `The answer is`, surrounding
quotes or markdown emphasis, and a trailing period are removed. The period of an
abbreviation (`U.S.`, `Washington D.C.`) is kept. Replies longer than 20 words
are kept; `final_answer_raw` always holds the original text.

## Tracing (optional)

Off by default. Set `otel_endpoint` to export spans to Phoenix over OTLP HTTP:

```python
cfg = AgentConfig(otel_endpoint="http://localhost:6006/v1/traces")
```

- Each run has one root span, `observer_agent.run`. `trace_id` in the result is
  that span's trace id. ADK's agent, LLM and tool spans nest under it, produced by
  `openinference-instrumentation-google-adk`.
- The tracer provider is created once per process, on the first traced run, and
  is private to this package (it is not installed as the global OTel provider).
  The first `otel_endpoint` wins; a different endpoint later in the same process
  is ignored with a warning.
- The instrumentor patches ADK for the whole process, so other ADK agents running
  in the same process are traced to the same endpoint.
- Spans are exported in the background. If Phoenix is not running, runs still
  succeed; the exporter logs the failed export.
- The Phoenix project name is `observer_agent`, or `PHOENIX_PROJECT_NAME` if set.

## Layout

```
observer_agent/
  __init__.py      # exports AgentConfig, run_agent, arun_agent
  config.py
  tools.py         # BM25 store, search, lookup, budget
  agent.py         # ADK agent, runner, event handling, result assembly
  answer.py        # final answer cleaning
  tracing.py
tests/
  test_tools.py
  test_answer.py
  test_agent_contract.py   # scripted fake model, concurrency, running inside an event loop
  test_tracing.py
scripts/
  smoke.py
```
