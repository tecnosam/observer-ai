import json
from pathlib import Path

from ..labels import label_from_reference, normalize_label
from ..schemas import LabeledTrace
from .base import AbstractTraceSource

# RAGAS / DeepEval column names, read as ours.
ALIASES = {
    "user_input": "question",
    "input": "question",
    "retrieved_contexts": "evidence",
    "contexts": "evidence",
    "response": "final_answer",
    "actual_output": "final_answer",
    "expected_output": "reference",
    "gold_answer": "reference",
    "ground_truth": "reference",
}


class JsonlTraceSource(AbstractTraceSource):
    """One trace per line: the engine's trace JSON plus a verdict.

    The verdict is `label` (correct / partial / wrong, or a 0..1 score), or a
    gold answer in `reference`, from which the label is worked out.
    RAGAS-style columns (user_input, retrieved_contexts, response, reference)
    are accepted too.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def describe(self) -> str:
        return str(self.path)

    def load(self) -> list[LabeledTrace]:
        traces, skipped = [], {}
        for n, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                traces.append(trace_from_row(json.loads(line), default_id=f"line-{n}"))
            except ValueError as err:
                skipped[str(err)] = skipped.get(str(err), 0) + 1
        if not traces:
            raise ValueError(f"{self.path}: no usable traces ({_summary(skipped)})")
        if skipped:
            print(f"{self.path}: skipped {sum(skipped.values())} lines: {_summary(skipped)}")
        return traces


def trace_from_row(row: dict, default_id: str) -> LabeledTrace:
    row = {ALIASES.get(k, k): v for k, v in row.items()}
    for name in ("question", "final_answer"):
        if not isinstance(row.get(name), str):
            raise ValueError(f"missing {name}")

    label = normalize_label(row["label"]) if "label" in row else None
    if label is None and isinstance(row.get("reference"), str):
        label = label_from_reference(row["final_answer"], row["reference"])
    if label is None:
        raise ValueError("no readable label or reference")

    tool_calls = row.get("tool_calls") or []
    if not all(isinstance(c, dict) and c.get("name") for c in tool_calls):
        raise ValueError("a tool call without a name")
    evidence = row.get("evidence") or []
    if not all(isinstance(e, str) for e in evidence):
        raise ValueError("evidence must be a list of strings")

    return LabeledTrace(
        trace_id=str(row.get("trace_id") or default_id),
        question=row["question"],
        tool_calls=tool_calls,
        evidence=evidence,
        final_answer=row["final_answer"],
        label=label,
        qid=row.get("qid"),
        split=row.get("split"),
        used_search=row.get("used_search"),
        stop_reason=row.get("stop_reason"),
        failure_type=row.get("failure_type"),
    )


def _summary(counts: dict) -> str:
    return ", ".join(f"{n} x {reason}" for reason, n in sorted(counts.items(), key=lambda kv: -kv[1])) or "empty"
