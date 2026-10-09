import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import observer_agent.agent as agent_module
from observer_agent import AgentConfig, run_agent, tracing

from .fakes import ScriptedLlm, search_lookup_answer

QUESTION = "Which river flows through Paris?"
ENDPOINT = "http://phoenix.invalid/v1/traces"


@pytest.fixture
def fresh_tracing():
    """Each test gets its own process-wide tracing state, torn down afterwards."""

    def reset():
        state, tracing._state = tracing._state, None
        if state is not None:
            state.provider.shutdown()
        try:
            from openinference.instrumentation.google_adk import GoogleADKInstrumentor

            GoogleADKInstrumentor().uninstrument()
        except Exception:
            pass

    reset()
    yield
    reset()


@pytest.fixture
def exporter(fresh_tracing, monkeypatch):
    exporter = InMemorySpanExporter()
    monkeypatch.setattr(tracing, "_build_exporter", lambda endpoint: exporter)
    return exporter


@pytest.fixture(autouse=True)
def scripted_model(monkeypatch):
    script = search_lookup_answer("Paris", lambda obs: "Seine")
    monkeypatch.setattr(agent_module, "build_model", lambda cfg: ScriptedLlm(script=script, delay_s=0.01))


def finished_spans(exporter):
    tracing._state.provider.force_flush()
    return exporter.get_finished_spans()


def kind(span):
    return span.attributes.get(tracing.SPAN_KIND)


def test_tracing_is_off_by_default(fresh_tracing, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    assert tracing._state is None
    assert len(result["trace_id"]) == 32


def test_trace_id_is_the_root_spans_trace_id(exporter, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(otel_endpoint=ENDPOINT))
    spans = finished_spans(exporter)

    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 1, "exactly one root span per run"
    root = roots[0]
    assert root.name == "observer_agent.run"
    assert tracing.format_trace_id(root.context.trace_id) == result["trace_id"]
    assert kind(root) == "AGENT"
    assert root.attributes[tracing.INPUT_VALUE] == QUESTION
    assert root.attributes[tracing.OUTPUT_VALUE] == "Seine"

    # Everything ADK did in this run hangs off that root.
    assert {s.context.trace_id for s in spans} == {root.context.trace_id}
    kinds = [kind(s) for s in spans]
    assert kinds.count("LLM") == 3
    assert kinds.count("TOOL") == 2
    tool_names = sorted(s.attributes[tracing.TOOL_NAME] for s in spans if kind(s) == "TOOL")
    assert tool_names == ["lookup", "search"]


def test_manual_tool_spans_when_the_instrumentor_is_unavailable(exporter, paragraphs, monkeypatch):
    monkeypatch.setattr(tracing, "_instrument_adk", lambda provider: False)
    result = run_agent(QUESTION, paragraphs, AgentConfig(otel_endpoint=ENDPOINT))
    spans = finished_spans(exporter)

    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 1
    assert tracing.format_trace_id(roots[0].context.trace_id) == result["trace_id"]
    tools = [s for s in spans if kind(s) == "TOOL"]
    assert sorted(s.attributes[tracing.TOOL_NAME] for s in tools) == ["lookup", "search"]
    for span in tools:
        assert span.parent.span_id == roots[0].context.span_id
        assert span.attributes[tracing.OUTPUT_VALUE].startswith("[")
        assert span.start_time <= span.end_time


def test_provider_is_set_up_once_across_threads(exporter, paragraphs, monkeypatch):
    setups = []
    real_setup = tracing._setup

    def counting_setup(endpoint):
        setups.append(threading.get_ident())
        time.sleep(0.05)  # widen the window for a race
        return real_setup(endpoint)

    monkeypatch.setattr(tracing, "_setup", counting_setup)
    cfg = AgentConfig(otel_endpoint=ENDPOINT)
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(lambda i: run_agent(QUESTION, paragraphs, cfg), range(8)))

    assert len(setups) == 1
    spans = finished_spans(exporter)
    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 8
    assert {tracing.format_trace_id(s.context.trace_id) for s in roots} == {r["trace_id"] for r in results}
    # No span leaked into another run's trace.
    for result in results:
        own = [s for s in spans if tracing.format_trace_id(s.context.trace_id) == result["trace_id"]]
        assert [kind(s) for s in own].count("TOOL") == 2


def test_run_inside_event_loop_keeps_one_trace(exporter, paragraphs):
    import asyncio

    async def main():
        return run_agent(QUESTION, paragraphs, AgentConfig(otel_endpoint=ENDPOINT))

    result = asyncio.run(main())
    spans = finished_spans(exporter)
    assert {tracing.format_trace_id(s.context.trace_id) for s in spans} == {result["trace_id"]}


def test_failed_run_still_raises_and_closes_the_root_span(exporter, paragraphs, monkeypatch):
    monkeypatch.setattr(
        agent_module, "build_model", lambda cfg: ScriptedLlm(script=[ConnectionError("down")])
    )
    with pytest.raises(ConnectionError):
        run_agent(QUESTION, paragraphs, AgentConfig(otel_endpoint=ENDPOINT))
    roots = [s for s in finished_spans(exporter) if s.parent is None]
    assert len(roots) == 1
    assert not roots[0].status.is_ok


def test_run_succeeds_when_phoenix_is_not_running(fresh_tracing, paragraphs):
    # A local port with nothing listening: the real OTLP exporter gets connection refused.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    cfg = AgentConfig(otel_endpoint=f"http://127.0.0.1:{port}/v1/traces")

    started = time.perf_counter()
    result = run_agent(QUESTION, paragraphs, cfg)
    elapsed = time.perf_counter() - started

    assert result["final_answer"] == "Seine"
    assert result["stop_reason"] == "answered"
    assert len(result["trace_id"]) == 32
    assert elapsed < 5, "export must not block the run"
