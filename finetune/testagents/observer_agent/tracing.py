"""Optional OpenTelemetry tracing to Phoenix, set up once per process.

The tracer provider is private to this package (it is not installed as the
global OTel provider), so it does not interfere with tracing the host
application may already have. Spans are exported in the background by a batch
processor, so a missing Phoenix never blocks or fails a run.
"""

import logging
import os
import threading
from dataclasses import dataclass

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import Tracer

logger = logging.getLogger(__name__)

# OpenInference attribute names and span kinds.
SPAN_KIND = "openinference.span.kind"
INPUT_VALUE = "input.value"
OUTPUT_VALUE = "output.value"
TOOL_NAME = "tool.name"
KIND_AGENT = "AGENT"
KIND_TOOL = "TOOL"

EXPORT_TIMEOUT_S = 5


@dataclass(frozen=True)
class Tracing:
    tracer: Tracer
    provider: TracerProvider
    endpoint: str
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
    provider.add_span_processor(BatchSpanProcessor(_build_exporter(endpoint)))
    instrumented = _instrument_adk(provider)
    return Tracing(
        tracer=provider.get_tracer("observer_agent"),
        provider=provider,
        endpoint=endpoint,
        instrumented=instrumented,
    )


def get_tracing(endpoint: str | None) -> Tracing | None:
    """The process-wide tracing state, created on first use. None when tracing is off.

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
