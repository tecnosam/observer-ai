import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import observer_agent.agent as agent_module
from observer_agent import AgentConfig, run_agent, tracing

from observer_agent.tools import STATUS_BAD_ARGS

from .fakes import ScriptedLlm, call, search_lookup_answer, text

QUESTION = "Which river flows through Paris?"
ENDPOINT = "http://collector.invalid/v1/traces"


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


def test_an_endpoint_alone_does_not_turn_exporting_on(fresh_tracing, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(otel_endpoint=ENDPOINT))
    assert tracing._state is None
    assert len(result["trace_id"]) == 32


def test_exporting_defaults_to_the_observer_engine(exporter, paragraphs):
    assert AgentConfig().exporting is False
    assert AgentConfig().otel_endpoint == "http://localhost:4318/v1/traces"
    run_agent(QUESTION, paragraphs, AgentConfig(exporting=True))
    assert tracing._state.endpoint == "http://localhost:4318/v1/traces"


def test_exporting_without_an_endpoint_is_an_error(fresh_tracing, paragraphs):
    with pytest.raises(ValueError, match="otel_endpoint"):
        run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=None))


def test_spans_carry_what_the_observer_engine_reads(exporter, paragraphs, monkeypatch):
    script = [
        lambda q, obs: [call("search", query=q)],
        lambda q, obs: [call("lookup", title="Nowhere At All")],
        lambda q, obs: [text("Seine")],
    ]
    monkeypatch.setattr(agent_module, "build_model", lambda cfg: ScriptedLlm(script=script))
    result = run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
    spans = finished_spans(exporter)

    # Tool statuses match the returned record, in call order.
    tools = sorted((s for s in spans if kind(s) == "TOOL"), key=lambda s: s.start_time)
    assert [s.attributes[tracing.TOOL_STATUS] for s in tools] == ["ok", "not_found"]
    assert [s.attributes[tracing.TOOL_STATUS] for s in tools] == [c["status"] for c in result["tool_calls"]]
    for span in tools:
        json.loads(span.attributes[tracing.INPUT_VALUE])  # the engine takes JSON arguments as they are

    # The root span lists the evidence exactly as the record does.
    root = next(s for s in spans if s.parent is None)
    assert result["evidence"]
    assert [
        root.attributes[tracing.EVIDENCE_FMT.format(i)] for i in range(len(result["evidence"]))
    ] == result["evidence"]
    assert tracing.EVIDENCE_FMT.format(len(result["evidence"])) not in root.attributes
    assert root.attributes[tracing.STOP_REASON] == "answered"


def test_a_rejected_tool_call_is_exported_as_bad_args(exporter, paragraphs, monkeypatch):
    # ADK answers these two calls itself; our tool code never runs for them.
    script = [
        lambda q, obs: [call("search")],  # missing required argument
        lambda q, obs: [call("nosuchtool", x=1)],
        lambda q, obs: [call("search", query=q)],
        lambda q, obs: [text("Seine")],
    ]
    monkeypatch.setattr(agent_module, "build_model", lambda cfg: ScriptedLlm(script=script))
    result = run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
    assert tracing.REJECTED_TOOL_STATUS == STATUS_BAD_ARGS

    tools = sorted((s for s in finished_spans(exporter) if kind(s) == "TOOL"), key=lambda s: s.start_time)
    assert [s.attributes[tracing.TOOL_STATUS] for s in tools] == ["bad_args", "bad_args", "ok"]
    assert [c["status"] for c in result["tool_calls"]] == ["bad_args", "bad_args", "ok"]


def test_trace_id_is_the_root_spans_trace_id(exporter, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
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
    result = run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
    spans = finished_spans(exporter)

    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 1
    assert tracing.format_trace_id(roots[0].context.trace_id) == result["trace_id"]
    tools = [s for s in spans if kind(s) == "TOOL"]
    assert sorted(s.attributes[tracing.TOOL_NAME] for s in tools) == ["lookup", "search"]
    for span in tools:
        assert span.parent.span_id == roots[0].context.span_id
        assert span.attributes[tracing.TOOL_STATUS] == "ok"
        json.loads(span.attributes[tracing.INPUT_VALUE])
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
    cfg = AgentConfig(exporting=True, otel_endpoint=ENDPOINT)
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
        return run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))

    result = asyncio.run(main())
    spans = finished_spans(exporter)
    assert {tracing.format_trace_id(s.context.trace_id) for s in spans} == {result["trace_id"]}


def test_failed_run_still_raises_and_closes_the_root_span(exporter, paragraphs, monkeypatch):
    monkeypatch.setattr(
        agent_module, "build_model", lambda cfg: ScriptedLlm(script=[ConnectionError("down")])
    )
    with pytest.raises(ConnectionError):
        run_agent(QUESTION, paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
    roots = [s for s in finished_spans(exporter) if s.parent is None]
    assert len(roots) == 1
    assert not roots[0].status.is_ok


def test_run_succeeds_when_the_endpoint_is_not_running(fresh_tracing, paragraphs):
    # A local port with nothing listening: the real OTLP exporter gets connection refused.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    cfg = AgentConfig(exporting=True, otel_endpoint=f"http://127.0.0.1:{port}/v1/traces")

    started = time.perf_counter()
    result = run_agent(QUESTION, paragraphs, cfg)
    elapsed = time.perf_counter() - started

    assert result["final_answer"] == "Seine"
    assert result["stop_reason"] == "answered"
    assert len(result["trace_id"]) == 32
    assert elapsed < 5, "export must not block the run"
