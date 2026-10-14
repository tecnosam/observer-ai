"""The long-lived chat agent (`adk web`): per-message budget, and the spans the Observer engine reads."""

import asyncio

import pytest
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import observer_agent.chat as chat_module
from observer_agent import AgentConfig, tracing
from observer_agent.chat import build_chat_agent

from .fakes import ScriptedLlm, call, text
from .test_tracing import ENDPOINT, fresh_tracing, kind  # noqa: F401  (fresh_tracing is a fixture)

APP = "observer_agent"


def chat(agent, messages: list[str]) -> list[str]:
    """Send `messages` in one session, as the chat UI does; return the final reply to each."""

    async def main():
        sessions = InMemorySessionService()
        runner = Runner(app_name=APP, agent=agent, session_service=sessions)
        session = await sessions.create_session(app_name=APP, user_id="u")
        replies = []
        for message in messages:
            reply = ""
            async for event in runner.run_async(
                user_id="u",
                session_id=session.id,
                new_message=types.Content(role="user", parts=[types.Part(text=message)]),
            ):
                if event.is_final_response() and event.content:
                    reply = "".join(p.text or "" for p in event.content.parts or [])
            replies.append(reply)
        await runner.close()
        return replies

    return asyncio.run(main())


def last_question(q, obs):
    return q


class LastMessageLlm(ScriptedLlm):
    """Scripted per message: the turn count restarts at the latest user message."""

    async def generate_content_async(self, llm_request, stream=False):
        contents = llm_request.contents
        start = max(
            i for i, c in enumerate(contents) if c.role == "user" and any(p.text for p in c.parts or [])
        )
        question = "".join(p.text or "" for p in contents[start].parts)
        turn = sum(c.role == "model" for c in contents[start:])
        step = self.script[min(turn, len(self.script) - 1)]
        from google.adk.models.llm_response import LlmResponse

        yield LlmResponse(content=types.Content(role="model", parts=step(question, [])))


SEARCH_THEN_ANSWER = [
    lambda q, obs: [call("search", query=q)],
    lambda q, obs: [call("lookup", title="Nowhere At All")],
    lambda q, obs: [text("Seine")],
]


@pytest.fixture
def exporter(fresh_tracing, monkeypatch):  # noqa: F811
    exporter = InMemorySpanExporter()
    monkeypatch.setattr(tracing, "_build_exporter", lambda endpoint: exporter)
    return exporter


def use_script(monkeypatch, script):
    monkeypatch.setattr(chat_module, "build_model", lambda cfg: LastMessageLlm(script=script))


def test_chat_answers_without_exporting(fresh_tracing, paragraphs, monkeypatch):  # noqa: F811
    use_script(monkeypatch, SEARCH_THEN_ANSWER)
    replies = chat(build_chat_agent(paragraphs, AgentConfig()), ["Which river flows through Paris?"])
    assert replies == ["Seine"]
    assert tracing._state is None


def test_each_message_is_one_trace_the_engine_can_read(exporter, paragraphs, monkeypatch):
    use_script(monkeypatch, SEARCH_THEN_ANSWER)
    agent = build_chat_agent(paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT))
    replies = chat(agent, ["Which river flows through Paris?", "Where is the Eiffel Tower?"])
    assert replies == ["Seine", "Seine"]

    tracing._state.provider.force_flush()
    spans = exporter.get_finished_spans()
    roots = [s for s in spans if s.parent is None]
    assert len(roots) == 2, "one trace per message"
    for root, question in zip(sorted(roots, key=lambda s: s.start_time), ["Paris", "Eiffel Tower"]):
        own = [s for s in spans if s.context.trace_id == root.context.trace_id]
        tools = sorted((s for s in own if kind(s) == "TOOL"), key=lambda s: s.start_time)
        assert [s.attributes[tracing.TOOL_STATUS] for s in tools] == ["ok", "not_found"]
        # ADK's own root span carries the message and the reply; we add the evidence.
        assert question in root.attributes[tracing.INPUT_VALUE]
        assert "Seine" in root.attributes[tracing.OUTPUT_VALUE]
        evidence = root.attributes[tracing.EVIDENCE_FMT.format(0)]
        assert not evidence.startswith("["), "bare paragraph text, without the [Title] prefix"
        assert question in evidence


def test_tool_budget_is_per_message(exporter, paragraphs, monkeypatch):
    script = [
        lambda q, obs: [call("search", query=q)],
        lambda q, obs: [call("search", query=q)],
        lambda q, obs: [text("done")],
    ]
    use_script(monkeypatch, script)
    agent = build_chat_agent(paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT, max_tool_calls=1))
    chat(agent, ["Paris river", "Paris river"])

    tracing._state.provider.force_flush()
    tools = sorted(
        (s for s in exporter.get_finished_spans() if kind(s) == "TOOL"), key=lambda s: s.start_time
    )
    assert [s.attributes[tracing.TOOL_STATUS] for s in tools] == ["ok", "budget_exhausted"] * 2


def test_step_limit_ends_the_message_and_is_recorded(exporter, paragraphs, monkeypatch):
    use_script(monkeypatch, [lambda q, obs: [call("search", query=q)]])  # never answers
    agent = build_chat_agent(paragraphs, AgentConfig(exporting=True, otel_endpoint=ENDPOINT, max_steps=2))
    assert chat(agent, ["Paris river"]) == [""]

    tracing._state.provider.force_flush()
    root = next(s for s in exporter.get_finished_spans() if s.parent is None)
    assert root.attributes[tracing.STOP_REASON] == "max_steps"
