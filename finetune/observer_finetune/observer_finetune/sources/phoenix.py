from urllib.parse import urlparse

import requests

from ..schemas import LabeledTrace
from .base import AbstractTraceSource
from .labeling import label_traces
from .spans import group_by_trace, normalize_span

PAGE = 1000


class PhoenixTraceSource(AbstractTraceSource):
    """A Phoenix project's traces, labeled by its span or trace annotations.

    This is where reviewed traces end up: the engine forwards the traces it
    keeps to Phoenix, people annotate them there, and this source reads both
    back. Annotations called `label_name` decide the label, from their label
    (correct / partial / wrong) or their 0..1 score.
    """

    def __init__(self, base_url: str, project: str, label_name: str, session: requests.Session | None = None):
        self.base_url, self.project, self.label_name = base_url.rstrip("/"), project, label_name
        self.session = session or requests.Session()

    @classmethod
    def from_uri(cls, uri: str, label_name: str, session=None):
        """phoenix://host:port/project, or https://host/project for a Phoenix behind TLS."""
        parsed = urlparse(uri)
        scheme = "http" if parsed.scheme == "phoenix" else parsed.scheme
        project = parsed.path.strip("/")
        if not parsed.netloc or not project:
            raise ValueError(f"{uri}: expected phoenix://host:port/project")
        return cls(f"{scheme}://{parsed.netloc}", project, label_name, session)

    def describe(self) -> str:
        return f"{self.base_url} project {self.project} ({self.label_name})"

    def load(self) -> list[LabeledTrace]:
        by_trace = group_by_trace([normalize_span(s) for s in self._pages("spans/otlpv1")])
        span_ids = [s["span_id"] for spans in by_trace.values() for s in spans]
        annotations = (self._pages("span_annotations", "span_ids", span_ids)
                       + self._pages("trace_annotations", "trace_ids", list(by_trace)))
        return label_traces(by_trace, annotations, self.label_name, self.describe())

    def _pages(self, path, id_param=None, ids=None):
        """Every item of a paginated list. Id filters are sent 100 at a time so the URL stays short."""
        out = []
        for start in range(0, len(ids), 100) if ids is not None else [None]:
            query = {"limit": PAGE}
            if id_param:
                query[id_param] = ids[start:start + 100]
                query["include_annotation_names"] = [self.label_name]
            while True:
                r = self.session.get(f"{self.base_url}/v1/projects/{self.project}/{path}", params=query, timeout=60)
                if r.status_code == 404:
                    raise ValueError(f"Phoenix at {self.base_url} has no project {self.project!r}")
                r.raise_for_status()
                body = r.json()
                out += body["data"]
                if not body.get("next_cursor"):
                    break
                query["cursor"] = body["next_cursor"]
        return out
