import json
import random

import pytest


def make_trace(i: int, wrong: bool, qid: str | None = None) -> dict:
    return {
        "trace_id": f"t{i}",
        "qid": qid or f"q{i}",
        "question": f"Which river flows through city {i}?",
        "tool_calls": [{"name": "search", "args": {"query": f"city {i} river"}, "status": "ok" if i % 7 else "not_found"}],
        "evidence": [f"City {i} lies on river {i}, not on river {i + 1}."],  # no rule can tell right from wrong
        "final_answer": f"river {i + 1}" if wrong else f"river {i}",
        "label": "wrong" if wrong else "correct",
    }


@pytest.fixture
def traces_file(tmp_path):
    """1,200 traces over 900 questions, 45% wrong, with no split field."""
    rng = random.Random(0)
    rows = []
    for i in range(1200):
        qid = f"q{i % 900}"
        rows.append(make_trace(i, wrong=rng.random() < 0.45, qid=qid))
    path = tmp_path / "traces.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return path
