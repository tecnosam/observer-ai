import json
from pathlib import Path

from ..schemas import LabeledTrace
from .base import AbstractTraceSource
from .labeling import label_traces
from .spans import group_by_trace, spans_from_otlp


class OtlpFileTraceSource(AbstractTraceSource):
    """OTLP JSON lines, as the OpenTelemetry Collector's file exporter writes them, plus a labels file.

    Each line of `path` is one export request ({"resourceSpans": [...]}).
    Labels are annotations in Phoenix's shape, one JSON object per line of
    `annotations`: {"trace_id" or "span_id", "name", "label" and/or "score"}.
    Only annotations called `label_name` are read.
    """

    def __init__(self, path: str | Path, annotations: str | Path, label_name: str):
        self.path, self.annotations, self.label_name = Path(path), Path(annotations), label_name

    def describe(self) -> str:
        return f"{self.path} + {self.annotations} ({self.label_name})"

    def load(self) -> list[LabeledTrace]:
        spans = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                spans += spans_from_otlp(json.loads(line))
        annotations = [json.loads(l) for l in self.annotations.read_text(encoding="utf-8").splitlines() if l.strip()]
        return label_traces(group_by_trace(spans), annotations, self.label_name, self.describe())
