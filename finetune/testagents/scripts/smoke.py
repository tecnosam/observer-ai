"""Run one two-hop example against a local Ollama and print the result as JSON.

    uv run python scripts/smoke.py [--model qwen2.5:7b] [--base-url URL] [--exporting] [--otel-endpoint URL]

`--exporting` sends the run's trace to the Observer engine's OTLP endpoint
(http://localhost:4318/v1/traces); `--otel-endpoint` sends it somewhere else
and turns exporting on by itself.

Exit codes: 0 answered "Seine" with at least one ok tool call, 1 Ollama
unreachable or the run failed, 2 the run completed but the answer looks wrong.
"""

import argparse
import json
import sys

from observer_agent import AgentConfig, run_agent
from observer_agent.samples import SMOKE_PARAGRAPHS as PARAGRAPHS
from observer_agent.samples import SMOKE_QUESTION as QUESTION

def main() -> int:
    defaults = AgentConfig()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default=defaults.model)
    parser.add_argument("--base-url", default=defaults.base_url)
    parser.add_argument("--exporting", action="store_true", help="export the trace over OTLP (off by default)")
    parser.add_argument(
        "--otel-endpoint",
        default=None,
        help=f"OTLP HTTP traces URL; implies --exporting (default {defaults.otel_endpoint})",
    )
    args = parser.parse_args()
    cfg = AgentConfig(
        model=args.model,
        base_url=args.base_url,
        exporting=args.exporting or args.otel_endpoint is not None,
        otel_endpoint=args.otel_endpoint or defaults.otel_endpoint,
    )

    try:
        result = run_agent(QUESTION, PARAGRAPHS, cfg)
    except Exception as exc:
        print(
            f"smoke: run failed against Ollama at {cfg.base_url} with model {cfg.model}: "
            f"{type(exc).__name__}: {exc}\n"
            f"Is Ollama running, and has the model been pulled (`ollama pull {cfg.model}`)?",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, indent=2, ensure_ascii=False))
    if cfg.exporting:
        print(f"smoke: exporting trace {result['trace_id']} to {cfg.otel_endpoint}", file=sys.stderr)

    ok_calls = sum(call["status"] == "ok" for call in result["tool_calls"])
    if "seine" not in result["final_answer"].lower() or ok_calls == 0:
        print(
            f"smoke: expected 'Seine' with at least one ok tool call, got "
            f"{result['final_answer']!r} with {ok_calls} ok tool call(s)",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
