#!/usr/bin/env python3
"""Regenerate golden slice files from the Python training code.

Runs build_state (copied verbatim from the fine-tuning code) over
testdata/traces/*.json and writes testdata/golden/<name>.txt. The Go test
TestBuildStateGolden asserts that BuildState reproduces every file byte for
byte.

Usage: python3 testdata/gen_golden.py   (or: make golden)
"""
import json
from pathlib import Path

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


def main():
    here = Path(__file__).resolve().parent
    golden = here / "golden"
    golden.mkdir(exist_ok=True)
    for old in golden.glob("*.txt"):
        old.unlink()
    traces = sorted((here / "traces").glob("*.json"))
    for path in traces:
        trace = json.loads(path.read_text(encoding="utf-8"))
        # Bytes, not text mode: no newline translation, no trailing newline.
        (golden / f"{path.stem}.txt").write_bytes(build_state(trace).encode("utf-8"))
    print(f"wrote {len(traces)} golden files to {golden}")


if __name__ == "__main__":
    main()
