"""Joining traces built from spans with the annotations that label them."""

from ..labels import normalize_label
from ..schemas import LabeledTrace
from .spans import trace_record


def label_traces(by_trace: dict[str, list[dict]], annotations: list[dict], label_name: str, where: str) -> list[LabeledTrace]:
    """Annotations may point at a trace (trace_id) or at any of its spans (span_id); the latest one wins."""
    trace_of_span = {s["span_id"]: trace_id for trace_id, spans in by_trace.items() for s in spans}
    verdicts = {}
    for a in sorted(annotations, key=lambda a: a.get("updated_at") or a.get("created_at") or ""):
        if a.get("name") != label_name:
            continue
        result = a.get("result") or a  # Phoenix nests label and score under "result"
        verdict = normalize_label(result.get("label")) or normalize_label(result.get("score"))
        trace_id = a.get("trace_id") or trace_of_span.get(a.get("span_id"))
        if verdict and trace_id:
            verdicts[trace_id] = verdict

    traces, unlabeled, unscorable = [], 0, 0
    for trace_id, spans in by_trace.items():
        record = trace_record(trace_id, spans)
        if record is None:
            unscorable += 1
        elif trace_id not in verdicts:
            unlabeled += 1
        else:
            traces.append(LabeledTrace(label=verdicts[trace_id], **record))
    print(f"{where}: {len(by_trace)} traces, {len(traces)} labeled, {unlabeled} without a {label_name!r} annotation, "
          f"{unscorable} without a question and an answer")
    if not traces:
        raise ValueError(f"{where}: no trace has both a question/answer root span and a {label_name!r} annotation")
    return traces
