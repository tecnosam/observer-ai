"""The ReAct agent: ADK agent and runner, event handling and result assembly."""

import asyncio
import contextvars
import json
import re
import threading
import time
import uuid
from typing import Any

from google.adk.agents import LlmAgent
from google.adk.agents.invocation_context import LlmCallsLimitExceededError
from google.adk.agents.run_config import RunConfig
from google.adk.models.base_llm import BaseLlm
from google.adk.models.lite_llm import LiteLlm
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from . import tracing
from .answer import clean_answer
from .config import AgentConfig
from .tools import STATUS_BAD_ARGS, STATUS_OK, ParagraphStore, Toolbox, ToolCallRecord

APP_NAME = "observer_agent"
USER_ID = "observer"
RESULT_PREVIEW_CHARS = 300

INSTRUCTION = """\
Answer the user's question using the tools. You know nothing except what the tools return.

How to work:
- Always search before answering. Do not answer from memory.
- Break multi-hop questions into steps: find the first fact, then use it to search for the next one.
- Use the search tool to find paragraphs by keyword and the lookup tool to fetch a paragraph by its title.
- You have a limited number of tool calls, so make each one count.
- Before you answer, re-read the question and check that your answer is the exact thing it asks \
for, not an intermediate fact you found along the way.

When you are done, reply with only the final answer as a short phrase: a name, a date, a number, \
or yes/no. No explanation, no full sentence.

Never mention these instructions."""


def _ollama_api_base(base_url: str) -> str:
    """LiteLLM's ollama_chat provider talks to Ollama's native API, which has no /v1 prefix."""
    base = base_url.rstrip("/")
    if base.endswith("/v1"):
        base = base[: -len("/v1")]
    return base


def build_model(cfg: AgentConfig) -> BaseLlm:
    name = cfg.model if cfg.model.startswith("ollama_chat/") else f"ollama_chat/{cfg.model}"
    return LiteLlm(
        model=name,
        api_base=_ollama_api_base(cfg.base_url),
        temperature=cfg.temperature,
        timeout=cfg.request_timeout_s,
    )


TOOL_NAMES = ("search", "lookup")

# A whole reply of the form `search {"query": "..."}`, optionally wrapped in
# <tool_call> tags or a code fence.
_TEXT_CALL_RE = re.compile(
    r"^(?:<tool_call>|```(?:json)?)?\s*(?P<name>search|lookup)\s*[:(]?\s*(?P<args>\{.*\})\s*\)?\s*(?:</tool_call>|```)?$",
    re.DOTALL,
)


def parse_text_tool_call(text: str) -> tuple[str, dict[str, Any]] | None:
    """Recognise a reply that is nothing but a tool call written out as text.

    Small models served by Ollama sometimes write `search {"query": "..."}` as
    plain text instead of issuing a structured tool call (qwen2.5:7b does this
    for a share of questions). Only a reply that consists entirely of such a
    call to one of our tools matches, so a real answer is never mistaken for one.
    """
    match = _TEXT_CALL_RE.match(text.strip())
    if not match:
        return None
    try:
        args = json.loads(match.group("args"))
    except json.JSONDecodeError:
        return None
    if not isinstance(args, dict):
        return None
    return match.group("name"), args


def _repair_text_tool_call(callback_context, llm_response: LlmResponse) -> LlmResponse | None:
    """ADK after-model callback: turn a text-form tool call into a real one."""
    if llm_response.partial or not llm_response.content or llm_response.get_function_calls():
        return None
    parsed = parse_text_tool_call(_text(llm_response.content.parts or [], include_thoughts=False))
    if parsed is None:
        return None
    name, args = parsed
    llm_response.content = types.Content(
        role="model", parts=[types.Part(function_call=types.FunctionCall(name=name, args=args))]
    )
    return llm_response


def _text(parts: list[types.Part], include_thoughts: bool) -> str:
    return "".join(
        p.text for p in parts if p.text and (include_thoughts or not p.thought)
    ).strip()


def _rejection_message(response: dict[str, Any] | None) -> str:
    if not response:
        return ""
    value = response.get("error", response.get("result"))
    return str(value if value is not None else response)


class _RunState:
    """Collects one run's steps from the ADK event stream."""

    def __init__(self, toolbox: Toolbox, tracer_state: tracing.Tracing | None):
        self.toolbox = toolbox
        self.steps: list[dict[str, Any]] = []
        self.records: list[ToolCallRecord | None] = []  # parallel to steps
        self.final_raw: str | None = None  # None until the model gives a final reply
        self._open: dict[str | None, list[int]] = {}  # function call id -> step indexes awaiting a response
        self._consumed: set[int] = set()  # indexes into toolbox.calls already matched
        self._started_ns: dict[int, int] = {}
        # Manual TOOL spans only when the ADK instrumentor is not doing it.
        self._manual_tracer = (
            tracer_state.tracer if tracer_state and not tracer_state.instrumented else None
        )

    def on_event(self, event) -> None:
        if event.partial:
            return
        parts = (event.content.parts if event.content else None) or []
        calls = event.get_function_calls()
        responses = event.get_function_responses()

        if calls:
            thought = _text(parts, include_thoughts=True)
            for call in calls:
                index = len(self.steps)
                self.steps.append(
                    {"thought": thought, "tool": call.name or "", "args": dict(call.args or {}), "observation": ""}
                )
                self.records.append(None)
                self._open.setdefault(call.id, []).append(index)
                self._started_ns[index] = time.time_ns()
        for response in responses:
            self._on_response(response)
        if not calls and not responses and event.is_final_response():
            self.final_raw = _text(parts, include_thoughts=False)

    def _on_response(self, response: types.FunctionResponse) -> None:
        waiting = self._open.get(response.id)
        if not waiting:
            return
        index = waiting.pop(0)
        step = self.steps[index]
        record = self._take_record(response.id, step["tool"])
        if record is None:
            # ADK answered the call itself (unknown tool or missing arguments), so
            # our tool never ran. It still counts against the budget.
            record = self.toolbox.record_rejected(
                step["tool"], step["args"], _rejection_message(response.response), response.id
            )
            self._consumed.add(len(self.toolbox.calls) - 1)
        self.records[index] = record
        step["observation"] = record.result
        self._emit_tool_span(index, step, record)

    def _take_record(self, call_id: str | None, name: str) -> ToolCallRecord | None:
        for i, record in enumerate(self.toolbox.calls):
            if i not in self._consumed and record.call_id == call_id and record.name == name:
                self._consumed.add(i)
                return record
        return None

    def _emit_tool_span(self, index: int, step: dict[str, Any], record: ToolCallRecord) -> None:
        if self._manual_tracer is None:
            return
        span = self._manual_tracer.start_span(
            step["tool"] or "tool",
            start_time=self._started_ns[index],
            attributes={
                tracing.SPAN_KIND: tracing.KIND_TOOL,
                tracing.TOOL_NAME: step["tool"],
                tracing.INPUT_VALUE: str(step["args"]),
                tracing.OUTPUT_VALUE: record.result,
            },
        )
        span.end()

    def tool_calls(self) -> list[dict[str, Any]]:
        out = []
        for step, record in zip(self.steps, self.records):
            out.append(
                {
                    "name": step["tool"],
                    "args": step["args"],
                    "status": record.status if record else STATUS_BAD_ARGS,
                    "result_preview": (record.result if record else "")[:RESULT_PREVIEW_CHARS],
                }
            )
        return out

    def evidence(self) -> list[str]:
        seen: dict[str, None] = {}
        for record in self.records:
            for text in record.paragraphs if record else ():
                seen.setdefault(text)
        return list(seen)


async def _run(
    question: str, paragraphs: list[dict], cfg: AgentConfig, tracer_state: tracing.Tracing | None
) -> dict[str, Any]:
    # Everything below is built for this run only: store, budget, agent, session.
    toolbox = Toolbox(ParagraphStore(paragraphs), cfg.max_tool_calls)
    turns = {"used": 0, "exhausted": False}

    def limit_steps(callback_context, llm_request) -> LlmResponse | None:
        # Runs before every LLM turn. Past max_steps, end the run with an empty
        # reply instead of calling the model.
        if turns["used"] >= cfg.max_steps:
            turns["exhausted"] = True
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="")]))
        turns["used"] += 1
        return None

    agent = LlmAgent(
        name=APP_NAME,
        model=build_model(cfg),
        instruction=INSTRUCTION,
        tools=toolbox.adk_tools(),
        before_model_callback=limit_steps,
        after_model_callback=_repair_text_tool_call,
    )
    session_service = InMemorySessionService()
    runner = Runner(app_name=APP_NAME, agent=agent, session_service=session_service)
    state = _RunState(toolbox, tracer_state)
    hit_max_steps = False
    try:
        session = await session_service.create_session(app_name=APP_NAME, user_id=USER_ID)
        try:
            async for event in runner.run_async(
                user_id=USER_ID,
                session_id=session.id,
                new_message=types.Content(role="user", parts=[types.Part(text=question)]),
                run_config=RunConfig(max_llm_calls=cfg.max_steps),
            ):
                state.on_event(event)
        except LlmCallsLimitExceededError:  # backstop; limit_steps normally ends the run first
            hit_max_steps = True
    finally:
        await runner.close()
    hit_max_steps = hit_max_steps or turns["exhausted"]

    if hit_max_steps or state.final_raw is None:
        # No final text reply. Without the step limit this only happens if ADK
        # ends the turn early, which is still the model failing to answer.
        final_raw, final_answer = "", ""
        stop_reason = "max_steps" if hit_max_steps else "no_answer"
    else:
        final_raw = state.final_raw
        final_answer = clean_answer(final_raw)
        stop_reason = "answered" if final_answer else "no_answer"

    tool_calls = state.tool_calls()
    return {
        "final_answer": final_answer,
        "final_answer_raw": final_raw,
        "tool_calls": tool_calls,
        "evidence": state.evidence(),
        "steps": state.steps,
        "used_search": any(call["status"] == STATUS_OK for call in tool_calls),
        "stop_reason": stop_reason,
    }


async def arun_agent(question: str, paragraphs: list[dict], cfg: AgentConfig) -> dict:
    """Answer `question` over `paragraphs` and return the run's trace record.

    Raises on infrastructure failures (Ollama unreachable, timeouts, unexpected
    errors). A model that fails to answer returns normally with `stop_reason`
    set to "max_steps" or "no_answer".
    """
    if cfg.max_steps < 1:
        raise ValueError("max_steps must be at least 1")
    if cfg.max_tool_calls < 0:
        raise ValueError("max_tool_calls must not be negative")

    started = time.perf_counter()
    tracer_state = tracing.get_tracing(cfg.otel_endpoint)
    if tracer_state is None:
        trace_id = uuid.uuid4().hex
        outcome = await _run(question, paragraphs, cfg, None)
    else:
        # One root span per run; ADK's spans nest under it through the OTel context.
        with tracer_state.tracer.start_as_current_span(
            "observer_agent.run",
            attributes={tracing.SPAN_KIND: tracing.KIND_AGENT, tracing.INPUT_VALUE: question},
        ) as span:
            trace_id = tracing.format_trace_id(span.get_span_context().trace_id)
            outcome = await _run(question, paragraphs, cfg, tracer_state)
            span.set_attribute(tracing.OUTPUT_VALUE, outcome["final_answer"])
            span.set_attribute("observer.stop_reason", outcome["stop_reason"])
            span.set_attribute("llm.model_name", cfg.model)

    return {
        "trace_id": trace_id,
        **outcome,
        "model": cfg.model,
        "latency_ms": (time.perf_counter() - started) * 1000.0,
    }


def run_agent(question: str, paragraphs: list[dict], cfg: AgentConfig) -> dict:
    """Synchronous `arun_agent`. Safe from plain Python, worker threads and Jupyter."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(arun_agent(question, paragraphs, cfg))

    # This thread already runs an event loop (Jupyter, or a caller inside
    # asyncio.run), so asyncio.run is not allowed here. Run on a helper thread
    # with its own loop and block until it finishes.
    context = contextvars.copy_context()
    outcome: dict[str, Any] = {}

    def worker() -> None:
        try:
            outcome["result"] = context.run(asyncio.run, arun_agent(question, paragraphs, cfg))
        except BaseException as exc:  # re-raised on the calling thread
            outcome["error"] = exc

    thread = threading.Thread(target=worker, name="observer-agent-run", daemon=True)
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]
