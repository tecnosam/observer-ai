"""The slice of a trace the model reads.

It must stay identical to the engine's BuildState
(inference-v1/engine/pkg/trace/slice.go); tests/test_state.py checks it
against the engine's golden files. Change one, change both.

Tool arguments, and any status that is not a string, are written as compact
JSON with characters as themselves.
"""

import json

MAX_EVIDENCE = 3
MAX_EVIDENCE_CHARS = 1500


def _json(value):
    """A JSON value on one line: no spaces, characters as themselves."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _tool_call(i, c):
    args = _json(c["args"]) if "args" in c else ""
    status = c.get("status", "ok")
    if not isinstance(status, str):
        status = _json(status)
    return f"{i}. {c['name']}({args}) -> {status}"


def build_state(t):
    tools = "\n".join(_tool_call(i, c) for i, c in enumerate(t.get("tool_calls", []), 1)) or "(none)"
    evidence = "\n".join(
        f"[{i}] {e[:MAX_EVIDENCE_CHARS]}" for i, e in enumerate(t.get("evidence", [])[:MAX_EVIDENCE], 1)
    ) or "(none)"
    return (
        f"USER QUESTION:\n{t['question']}\n\n"
        f"TOOL CALLS:\n{tools}\n\n"
        f"EVIDENCE:\n{evidence}\n\n"
        f"FINAL ANSWER:\n{t['final_answer']}"
    )
