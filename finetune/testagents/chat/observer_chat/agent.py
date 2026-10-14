"""The agent for ADK's chat UI. From finetune/testagents:

    uv run adk web chat                          # then open http://127.0.0.1:8000
    OBSERVER_EXPORTING=1 uv run adk web chat     # also send every message's trace to the Observer engine

Environment:
    OBSERVER_EXPORTING       1/true to export traces over OTLP. Off by default.
    OBSERVER_OTLP_ENDPOINT   where to export; default is the Observer engine, http://localhost:4318/v1/traces
    OBSERVER_MODEL           Ollama model, default qwen2.5:7b
    OBSERVER_OLLAMA_URL      Ollama endpoint, default http://localhost:11434/v1
    OBSERVER_CHAT_CORPUS     JSON file holding a list of {"title", "text"} paragraphs to search.
                             Default: chat/corpus.json if it exists (scripts/build_chat_corpus.py
                             writes it), else the six smoke-test paragraphs.
"""

import json
import os
from pathlib import Path

from observer_agent import AgentConfig
from observer_agent.chat import build_chat_agent
from observer_agent.samples import SMOKE_PARAGRAPHS

DEFAULT_CORPUS = Path(__file__).resolve().parent.parent / "corpus.json"


def load_paragraphs() -> list[dict]:
    path = os.environ.get("OBSERVER_CHAT_CORPUS")
    if path:
        return json.loads(Path(path).read_text())
    if DEFAULT_CORPUS.exists():
        return json.loads(DEFAULT_CORPUS.read_text())
    return SMOKE_PARAGRAPHS


defaults = AgentConfig()
cfg = AgentConfig(
    model=os.environ.get("OBSERVER_MODEL", defaults.model),
    base_url=os.environ.get("OBSERVER_OLLAMA_URL", defaults.base_url),
    exporting=os.environ.get("OBSERVER_EXPORTING", "").lower() in ("1", "true", "yes", "on"),
    otel_endpoint=os.environ.get("OBSERVER_OTLP_ENDPOINT", defaults.otel_endpoint),
)
paragraphs = load_paragraphs()
print(
    f"observer_chat: model {cfg.model}, {len(paragraphs)} paragraphs, "
    + (f"exporting traces to {cfg.otel_endpoint}" if cfg.exporting else "exporting off")
)

root_agent = build_chat_agent(paragraphs, cfg)
