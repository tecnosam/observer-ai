"""The slice of a trace the model reads.

It must stay byte for byte identical to the engine's BuildState
(inference-v1/engine/internal/trace/slice.go); tests/test_state.py checks it
against the engine's golden files. Change one, change both.
"""

MAX_EVIDENCE = 3
MAX_EVIDENCE_CHARS = 1500


def build_state(t):
    tools = "\n".join(
        f"{i}. {c['name']}({c.get('args', '')!r}) -> {c.get('status', 'ok')}"
        for i, c in enumerate(t.get("tool_calls", []), 1)
    ) or "(none)"
    evidence = "\n".join(
        f"[{i}] {e[:MAX_EVIDENCE_CHARS]}" for i, e in enumerate(t.get("evidence", [])[:MAX_EVIDENCE], 1)
    ) or "(none)"
    return (
        f"USER QUESTION:\n{t['question']}\n\n"
        f"TOOL CALLS:\n{tools}\n\n"
        f"EVIDENCE:\n{evidence}\n\n"
        f"FINAL ANSWER:\n{t['final_answer']}"
    )
