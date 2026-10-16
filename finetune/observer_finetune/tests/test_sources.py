import json

import pytest

from observer_finetune.sources.factory import build_source
from observer_finetune.sources.jsonl import JsonlTraceSource
from observer_finetune.sources.otlp import OtlpFileTraceSource
from observer_finetune.sources.phoenix import PhoenixTraceSource


def write_lines(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return path


def test_jsonl_labels_references_and_aliases(tmp_path):
    path = write_lines(tmp_path / "t.jsonl", [
        {"question": "q1", "final_answer": "Seine", "label": "incorrect"},
        {"question": "q2", "final_answer": "The Seine.", "reference": "Seine"},
        {"user_input": "q3", "retrieved_contexts": ["Paris lies on the Seine."], "response": "Rhone", "reference": "Seine"},
        {"question": "q4", "final_answer": "x", "label": 0.9},
        {"question": "q5", "final_answer": "x", "label": True},   # ambiguous: skipped
        {"question": "q6"},                                       # no answer: skipped
    ])
    traces = JsonlTraceSource(path).load()
    assert [t.label for t in traces] == ["wrong", "correct", "wrong", "correct"]
    assert traces[2].question == "q3" and traces[2].evidence == ["Paris lies on the Seine."]
    assert traces[0].trace_id == "line-1"


def test_jsonl_with_nothing_usable_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="no usable traces"):
        JsonlTraceSource(write_lines(tmp_path / "t.jsonl", [{"question": "q"}])).load()


def otlp_line(trace_id, answer, tool_status="ok"):
    def attr(k, v):
        return {"key": k, "value": {"stringValue": v}}
    return {"resourceSpans": [{"scopeSpans": [{"spans": [
        {"traceId": trace_id, "spanId": "a1", "name": "run", "startTimeUnixNano": "1",
         "attributes": [attr("openinference.span.kind", "AGENT"), attr("input.value", "Which river?"),
                        attr("output.value", answer), attr("observer.evidence.0", "Paris lies on the Seine.")]},
        {"traceId": trace_id, "spanId": "b2", "parentSpanId": "a1", "name": "execute_tool search", "startTimeUnixNano": "2",
         "attributes": [attr("openinference.span.kind", "TOOL"), attr("tool.name", "search"),
                        attr("input.value", '{"query": "Paris river"}'), attr("observer.tool.status", tool_status)]},
    ]}]}]}


def test_otlp_file_with_annotations(tmp_path):
    spans = write_lines(tmp_path / "spans.jsonl", [otlp_line("t1", "Seine"), otlp_line("t2", "Rhone", "not_found"), otlp_line("t3", "x")])
    labels = write_lines(tmp_path / "labels.jsonl", [
        {"trace_id": "t1", "name": "correctness", "label": "correct"},
        {"span_id": "b2", "name": "correctness", "score": 0.0},     # t2 or t3: both have a span b2...
        {"trace_id": "t2", "name": "correctness", "label": "wrong"},
        {"trace_id": "t3", "name": "helpfulness", "label": "wrong"},  # another annotation: ignored
    ])
    source = build_source(str(spans), str(labels))
    assert isinstance(source, OtlpFileTraceSource)
    traces = {t.trace_id: t for t in source.load()}
    assert set(traces) >= {"t1", "t2"}
    t2 = traces["t2"]
    assert t2.question == "Which river?" and t2.final_answer == "Rhone" and t2.label == "wrong"
    assert t2.tool_calls == [{"name": "search", "args": {"query": "Paris river"}, "status": "not_found"}]
    assert t2.evidence == ["Paris lies on the Seine."]


def test_otlp_file_without_annotations_is_an_error(tmp_path):
    spans = write_lines(tmp_path / "spans.jsonl", [otlp_line("t1", "Seine")])
    with pytest.raises(ValueError, match="--annotations"):
        build_source(str(spans))


class FakeResponse:
    def __init__(self, body, status=200):
        self.body, self.status_code = body, status
    def json(self):
        return self.body
    def raise_for_status(self):
        pass


class FakePhoenix:
    """Serves Phoenix's span and annotation lists, the spans in two pages."""

    def __init__(self):
        root = {"trace_id": "c455", "span_id": "s1", "parent_span_id": None, "name": "invocation [agent]",
                "start_time_unix_nano": 1, "status": {"code": 1},
                "attributes": [
                    {"key": "openinference.span.kind", "value": {"string_value": "CHAIN"}},
                    {"key": "input.value", "value": {"string_value": json.dumps({"new_message": {"parts": [{"text": "Who wrote Dune?"}]}})}},
                    {"key": "output.value", "value": {"string_value": json.dumps({"content": {"parts": [{"text": "Frank Herbert"}]}})}},
                ]}
        other = {**root, "trace_id": "d566", "span_id": "s2"}
        self.pages = {None: ([root], "next"), "next": ([other], None)}
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params)))
        if url.endswith("/spans/otlpv1"):
            data, cursor = self.pages[params.get("cursor")]
            return FakeResponse({"data": data, "next_cursor": cursor})
        if url.endswith("/span_annotations"):
            return FakeResponse({"data": [{"span_id": "s1", "name": "correctness", "result": {"label": "correct", "score": None}}], "next_cursor": None})
        return FakeResponse({"data": [], "next_cursor": None})


def test_phoenix_source_reads_pages_and_annotations():
    fake = FakePhoenix()
    source = PhoenixTraceSource.from_uri("phoenix://localhost:6006/observer-ai", "correctness", session=fake)
    traces = source.load()
    assert [(t.trace_id, t.question, t.final_answer, t.label) for t in traces] == [("c455", "Who wrote Dune?", "Frank Herbert", "correct")]
    span_calls = [p for url, p in fake.calls if url.endswith("/span_annotations")]
    assert span_calls[0]["include_annotation_names"] == ["correctness"]
    assert sorted(span_calls[0]["span_ids"]) == ["s1", "s2"]
    assert fake.calls[0][0] == "http://localhost:6006/v1/projects/observer-ai/spans/otlpv1"


def test_build_source_picks_phoenix_for_a_phoenix_uri():
    assert isinstance(build_source("phoenix://localhost:6006/observer-ai"), PhoenixTraceSource)
