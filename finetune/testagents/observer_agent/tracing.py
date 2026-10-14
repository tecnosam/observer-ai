"""Optional OpenTelemetry trace export over OTLP HTTP, set up once per process.

Off unless `AgentConfig.exporting` is True. The default endpoint is the Observer
engine's OTLP receiver; Phoenix or any other OTLP HTTP collector works too.

The tracer provider is private to this package (it is not installed as the
global OTel provider), so it does not interfere with tracing the host
application may already have. Spans are exported in the background by a batch
processor, so an unreachable endpoint never blocks or fails a run.
"""

import logging
import os
import threading
from dataclasses import dataclass

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry import trace
from opentelemetry.trace import Tracer

logger = logging.getLogger(__name__)

# OpenInference attribute names and span kinds.
SPAN_KIND = "openinference.span.kind"
INPUT_VALUE = "input.value"
OUTPUT_VALUE = "output.value"
TOOL_NAME = "tool.name"
KIND_AGENT = "AGENT"
KIND_TOOL = "TOOL"

# Read by the Observer engine (internal/otlp/convert.go), so the trace it scores
# matches the record `run_agent` returns instead of being inferred from tool output.
TOOL_STATUS = "observer.tool.status"        # on a TOOL span: ok | not_found | bad_args | budget_exhausted
EVIDENCE_FMT = "observer.evidence.{}"       # on the root span: evidence items, in order
STOP_REASON = "observer.stop_reason"        # on the root span

# ADK names every tool span "execute_tool <name>", including the ones for calls it
# rejects itself (unknown tool, missing argument), where our tool code never runs.
ADK_TOOL_SPAN_PREFIX = "execute_tool"
REJECTED_TOOL_STATUS = "bad_args"           # same value as tools.STATUS_BAD_ARGS

EXPORT_TIMEOUT_S = 5


class ObserverSpanProcessor(SpanProcessor):
    """Fills in what the Observer engine needs and the ADK instrumentor cannot know.

    - Every ADK tool span starts with the status `bad_args`. A tool that runs
      overwrites it with its real status (`set_tool_status`); a call ADK rejects
      before the tool runs keeps it, which is the status the returned record gives
      such a call.
    - Root spans are remembered while they are open, so code running deeper in
      the trace can put run-level attributes on them (`set_root_attributes`).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._roots: dict[int, Span] = {}

    def on_start(self, span: Span, parent_context=None) -> None:
        if span.name.startswith(ADK_TOOL_SPAN_PREFIX):
            span.set_attribute(TOOL_STATUS, REJECTED_TOOL_STATUS)
        if span.parent is None:
            with self._lock:
                self._roots[span.get_span_context().trace_id] = span

    def on_end(self, span) -> None:
        if span.parent is None:
            with self._lock:
                self._roots.pop(span.get_span_context().trace_id, None)

    def root(self, trace_id: int) -> Span | None:
        with self._lock:
            return self._roots.get(trace_id)


@dataclass(frozen=True)
class Tracing:
    tracer: Tracer
    provider: TracerProvider
    endpoint: str
    spans: ObserverSpanProcessor
    instrumented: bool  # False: the ADK instrumentor is unavailable, the agent emits TOOL spans itself


_lock = threading.Lock()
_state: Tracing | None = None


def _build_exporter(endpoint: str) -> SpanExporter:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter(endpoint=endpoint, timeout=EXPORT_TIMEOUT_S)


def _instrument_adk(provider: TracerProvider) -> bool:
    try:
        from openinference.instrumentation.google_adk import GoogleADKInstrumentor

        instrumentor = GoogleADKInstrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument(tracer_provider=provider)
        return True
    except Exception:
        logger.warning(
            "OpenInference ADK instrumentation unavailable; falling back to manual spans",
            exc_info=True,
        )
        return False


def _setup(endpoint: str) -> Tracing:
    resource = Resource.create(
        {
            "service.name": "observer_agent",
            "openinference.project.name": os.environ.get("PHOENIX_PROJECT_NAME", "observer_agent"),
        }
    )
    provider = TracerProvider(resource=resource)
    spans = ObserverSpanProcessor()
    provider.add_span_processor(spans)
    provider.add_span_processor(BatchSpanProcessor(_build_exporter(endpoint)))
    instrumented = _instrument_adk(provider)
    return Tracing(
        tracer=provider.get_tracer("observer_agent"),
        provider=provider,
        endpoint=endpoint,
        spans=spans,
        instrumented=instrumented,
    )


def set_tool_status(status: str) -> None:
    """Record a tool call's status on the span it runs in.

    Inside a tool the current span is the instrumentor's TOOL span. Without
    tracing there is no recording span and this does nothing.
    """
    span = trace.get_current_span()
    if span.is_recording():
        span.set_attribute(TOOL_STATUS, status)


def set_root_attributes(attributes: dict[str, str]) -> None:
    """Set attributes on the root span of the trace the caller is running in.

    For code that does not hold the root span itself, such as an ADK callback
    under `adk web`, where ADK opens the root span. Does nothing without tracing.
    """
    state = _state
    context = trace.get_current_span().get_span_context()
    if state is None or not context.is_valid:
        return
    root = state.spans.root(context.trace_id)
    if root is not None and root.is_recording():
        root.set_attributes(attributes)


def evidence_attributes(evidence: list[str]) -> dict[str, str]:
    return {EVIDENCE_FMT.format(i): text for i, text in enumerate(evidence)}


def get_tracing(endpoint: str | None) -> Tracing | None:
    """The process-wide tracing state, created on first use. None when `endpoint` is empty.

    The first endpoint wins: the provider is built once per process, so a later
    call with a different endpoint keeps exporting to the first one.
    """
    global _state
    if not endpoint:
        return None
    with _lock:
        if _state is None:
            _state = _setup(endpoint)
        elif _state.endpoint != endpoint:
            logger.warning(
                "Tracing already set up for %s; ignoring otel_endpoint=%s",
                _state.endpoint,
                endpoint,
            )
        return _state


def format_trace_id(trace_id: int) -> str:
    return format(trace_id, "032x")
