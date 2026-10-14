from dataclasses import dataclass

# The Observer engine's OTLP HTTP receiver (inference-v1/engine, `otlp.http_addr`).
OBSERVER_OTLP_ENDPOINT = "http://localhost:4318/v1/traces"


@dataclass(frozen=True)
class AgentConfig:
    model: str = "qwen2.5:7b"                     # Ollama model name, without the "ollama_chat/" prefix
    base_url: str = "http://localhost:11434/v1"   # Ollama endpoint; the /v1 suffix is dropped for LiteLLM
    max_tool_calls: int = 6                       # hard budget across all tools in one run
    max_steps: int = 10                           # max LLM turns before giving up
    exporting: bool = False                       # True exports each run's spans to otel_endpoint; keep off for eval dataset runs
    otel_endpoint: str | None = OBSERVER_OTLP_ENDPOINT   # OTLP HTTP traces URL, used only when exporting is True
    temperature: float = 0.0
    request_timeout_s: float = 120.0
