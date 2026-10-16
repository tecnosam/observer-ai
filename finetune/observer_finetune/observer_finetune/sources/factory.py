import json
from pathlib import Path

from .base import AbstractTraceSource
from .jsonl import JsonlTraceSource
from .otlp import OtlpFileTraceSource
from .phoenix import PhoenixTraceSource

DEFAULT_LABEL_NAME = "correctness"


def build_source(data: str, annotations: str | None = None, label_name: str = DEFAULT_LABEL_NAME) -> AbstractTraceSource:
    """Pick the source for `data`.

    phoenix://host:port/project  -> PhoenixTraceSource
    a file of OTLP export lines  -> OtlpFileTraceSource (needs `annotations`)
    any other .jsonl file        -> JsonlTraceSource
    """
    if data.startswith(("phoenix://", "http://", "https://")):
        return PhoenixTraceSource.from_uri(data, label_name)
    path = Path(data)
    if not path.exists():
        raise ValueError(f"{data}: no such file")
    if _is_otlp(path):
        if annotations is None:
            raise ValueError(f"{data} holds OTLP spans; pass the labels with --annotations")
        return OtlpFileTraceSource(path, annotations, label_name)
    return JsonlTraceSource(path)


def _is_otlp(path: Path) -> bool:
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                first = json.loads(line)
                return "resourceSpans" in first or "resource_spans" in first
    return False
