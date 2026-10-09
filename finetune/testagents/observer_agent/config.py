from dataclasses import dataclass


@dataclass(frozen=True)
class AgentConfig:
    model: str = "qwen2.5:7b"                     # Ollama model name, without the "ollama_chat/" prefix
    base_url: str = "http://localhost:11434/v1"   # Ollama endpoint; the /v1 suffix is dropped for LiteLLM
    max_tool_calls: int = 6                       # hard budget across all tools in one run
    max_steps: int = 10                           # max LLM turns before giving up
    otel_endpoint: str | None = None              # e.g. "http://localhost:6006/v1/traces"; None disables tracing
    temperature: float = 0.0
    request_timeout_s: float = 120.0
