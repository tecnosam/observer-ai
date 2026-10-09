"""Run one two-hop example against a local Ollama and print the result as JSON.

    uv run python scripts/smoke.py [--model qwen2.5:7b] [--base-url URL] [--otel-endpoint URL]

Exit codes: 0 answered "Seine" with at least one ok tool call, 1 Ollama
unreachable or the run failed, 2 the run completed but the answer looks wrong.
"""

import argparse
import json
import sys

from observer_agent import AgentConfig, run_agent

QUESTION = "Which river flows through the capital of the country where the Eiffel Tower is?"

PARAGRAPHS = [
    {
        "title": "Eiffel Tower",
        "text": "The Eiffel Tower is a wrought-iron lattice tower on the Champ de Mars in Paris, France. "
        "It was completed in 1889 and is named after the engineer Gustave Eiffel.",
    },
    {
        "title": "Paris",
        "text": "Paris is the capital and most populous city of France. The city is built on the "
        "banks of the Seine, which flows through its centre.",
    },
    {
        "title": "Mount Kilimanjaro",
        "text": "Mount Kilimanjaro is a dormant volcano in Tanzania and the highest mountain in Africa.",
    },
    {
        "title": "Photosynthesis",
        "text": "Photosynthesis is the process by which plants and algae convert light energy into "
        "chemical energy stored in sugars.",
    },
    {
        "title": "Great Barrier Reef",
        "text": "The Great Barrier Reef is the world's largest coral reef system, located in the Coral "
        "Sea off the coast of Queensland, Australia.",
    },
    {
        "title": "Johann Sebastian Bach",
        "text": "Johann Sebastian Bach was a German composer of the Baroque period, known for the "
        "Brandenburg Concertos and the Mass in B minor.",
    },
]


def main() -> int:
    defaults = AgentConfig()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default=defaults.model)
    parser.add_argument("--base-url", default=defaults.base_url)
    parser.add_argument("--otel-endpoint", default=None)
    args = parser.parse_args()
    cfg = AgentConfig(model=args.model, base_url=args.base_url, otel_endpoint=args.otel_endpoint)

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
