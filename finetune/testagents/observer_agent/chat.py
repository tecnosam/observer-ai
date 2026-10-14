"""The agent as one long-lived ADK agent, for the chat UI (`adk web`).

`run_agent` builds a fresh agent for every question. A chat UI needs the
opposite: one `root_agent` that answers many messages. Here the paragraph
store is built once, and each message (an ADK invocation) gets its own tool
budget and step limit, like a `run_agent` call does.

Each message is one trace. ADK opens the root span itself, and the Observer
engine reads the question and answer from it. This module adds the evidence
and stop reason to that span when the run ends.
"""

import threading
from dataclasses import dataclass

from google.adk.agents import LlmAgent
from google.adk.models.llm_response import LlmResponse
from google.genai import types

from . import tracing
from .agent import APP_NAME, INSTRUCTION, _repair_text_tool_call, build_model
from .config import AgentConfig
from .tools import ParagraphStore, Toolbox, adk_tools


@dataclass
class _Invocation:
    toolbox: Toolbox
    turns: int = 0
    hit_max_steps: bool = False


def build_chat_agent(paragraphs: list[dict], cfg: AgentConfig) -> LlmAgent:
    """An agent over `paragraphs` that can be served by `adk web`.

    With `cfg.exporting` on, every message's trace goes to `cfg.otel_endpoint`.
    """
    if cfg.max_steps < 1:
        raise ValueError("max_steps must be at least 1")
    if cfg.exporting and not cfg.otel_endpoint:
        raise ValueError("exporting is on but otel_endpoint is empty")
    tracing.get_tracing(cfg.otel_endpoint if cfg.exporting else None)

    store = ParagraphStore(paragraphs)
    lock = threading.Lock()
    invocations: dict[str, _Invocation] = {}

    def current(invocation_id: str) -> _Invocation:
        with lock:
            if invocation_id not in invocations:
                invocations[invocation_id] = _Invocation(Toolbox(store, cfg.max_tool_calls))
            return invocations[invocation_id]

    def limit_steps(callback_context, llm_request) -> LlmResponse | None:
        # Past max_steps, end the message with an empty reply instead of calling the model.
        invocation = current(callback_context.invocation_id)
        if invocation.turns >= cfg.max_steps:
            invocation.hit_max_steps = True
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="")]))
        invocation.turns += 1
        return None

    def finish(callback_context) -> None:
        with lock:
            invocation = invocations.pop(callback_context.invocation_id, None)
        if invocation is None:
            return None
        attributes = tracing.evidence_attributes(invocation.toolbox.evidence())
        if invocation.hit_max_steps:
            attributes[tracing.STOP_REASON] = "max_steps"
        tracing.set_root_attributes(attributes)
        return None

    return LlmAgent(
        name=APP_NAME,
        model=build_model(cfg),
        instruction=INSTRUCTION,
        tools=adk_tools(lambda tool_context: current(tool_context.invocation_id).toolbox),
        before_model_callback=limit_steps,
        after_model_callback=_repair_text_tool_call,
        after_agent_callback=finish,
    )
