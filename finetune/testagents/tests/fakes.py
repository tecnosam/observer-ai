"""A scripted stand-in for the LLM so the agent loop can be tested offline."""

import asyncio
import re
from typing import Any, Callable

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types

# A script step gets (question, observations so far) and returns the model's parts.
Step = Callable[[str, list[str]], list[types.Part]]


def text(value: str) -> types.Part:
    return types.Part(text=value)


def call(name: str, **args: Any) -> types.Part:
    return types.Part(function_call=types.FunctionCall(name=name, args=args))


class ScriptedLlm(BaseLlm):
    """Plays `script` one step per LLM turn; the last step repeats if the run goes on.

    The turn number is read from the request itself (how many model turns it
    already contains), so the fake holds no state of its own.
    """

    model: str = "scripted"
    script: list[Any]
    delay_s: float = 0.0

    async def generate_content_async(self, llm_request, stream=False):
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        question = ""
        observations: list[str] = []
        turn = 0
        for content in llm_request.contents:
            if content.role == "model":
                turn += 1
            for part in content.parts or []:
                if part.function_response:
                    response = part.function_response.response or {}
                    observations.append(str(response.get("result", response.get("error", ""))))
                elif part.text and content.role == "user" and not question:
                    question = part.text
        step = self.script[min(turn, len(self.script) - 1)]
        if isinstance(step, BaseException):
            raise step
        yield LlmResponse(content=types.Content(role="model", parts=step(question, observations)))


def first_title(observation: str) -> str:
    match = re.match(r"\[(.+?)\]", observation)
    return match.group(1) if match else ""


def search_lookup_answer(lookup_title: str, answer: Callable[[list[str]], str]) -> list[Step]:
    """The standard three-turn script: search the question, look a title up, answer."""
    return [
        lambda q, obs: [text("I should search first."), call("search", query=q)],
        lambda q, obs: [call("lookup", title=lookup_title)],
        lambda q, obs: [text(answer(obs))],
    ]
