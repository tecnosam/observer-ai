"""ReAct agent over local search tools that generates HotpotQA traces for Observer.ai."""

from .agent import arun_agent, run_agent
from .config import OBSERVER_OTLP_ENDPOINT, AgentConfig

__all__ = ["AgentConfig", "OBSERVER_OTLP_ENDPOINT", "run_agent", "arun_agent"]
