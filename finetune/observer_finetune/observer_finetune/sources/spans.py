"""OpenInference spans to traces, with the same rules as the engine (inference-v1/engine/internal/otlp/convert.go).

Accepts OTLP JSON as the OpenTelemetry Collector writes it (camelCase keys)
and as Phoenix serves it (snake_case keys).
"""

import json
import re

ATTR_EVIDENCE = re.compile(r"^observer\.evidence\.(\d+)$")
ATTR_RETRIEVAL_DOC = re.compile(r"^retrieval\.documents\.(\d+)\.document\.content$")
QUESTION_KEYS = ("question", "input", "query", "prompt", "text", "content")
ANSWER_KEYS = ("final_answer", "answer", "output", "result", "response", "text", "content")
OUTPUT_KEYS = ("result", "output", "content", "text")


def snake(key: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()


def flatten_attributes(attributes) -> dict:
    """[{key, value: {stringValue: ...}}] -> {key: value}."""
    out = {}
    for kv in attributes or []:
        value = {snake(k): v for k, v in (kv.get("value") or {}).items() if v is not None}
        for kind in ("string_value", "int_value", "double_value", "bool_value"):
            if kind in value:
                out[kv["key"]] = value[kind]
                break
    return out


def spans_from_otlp(payload: dict) -> list[dict]:
    """The spans of one OTLP export request ({resourceSpans: [...]}), flattened."""
    spans = []
    for rs in payload.get("resourceSpans") or payload.get("resource_spans") or []:
        for ss in rs.get("scopeSpans") or rs.get("scope_spans") or []:
            spans.extend(normalize_span(s) for s in ss.get("spans") or [])
    return spans


def normalize_span(span: dict) -> dict:
    span = {snake(k): v for k, v in span.items()}
    return {
        "trace_id": span.get("trace_id") or "",
        "span_id": span.get("span_id") or "",
        "parent_span_id": span.get("parent_span_id") or "",
        "name": span.get("name") or "",
        "start": int(span.get("start_time_unix_nano") or 0),
        "status": (span.get("status") or {}).get("code"),
        "attributes": flatten_attributes(span.get("attributes")),
    }


def group_by_trace(spans: list[dict]) -> dict[str, list[dict]]:
    by_trace = {}
    for span in spans:
        if span["trace_id"] and span["span_id"]:
            by_trace.setdefault(span["trace_id"], {})[span["span_id"]] = span  # a re-sent span counts once
    return {trace_id: list(spans.values()) for trace_id, spans in by_trace.items()}


def trace_record(trace_id: str, spans: list[dict]) -> dict | None:
    """The engine's trace record for one trace's spans, or None when no root span carries a question and an answer."""
    ids = {s["span_id"] for s in spans}
    roots = [s for s in spans if not s["parent_span_id"] or s["parent_span_id"] not in ids]
    if not roots:
        return None
    rank = {"AGENT": 0, "CHAIN": 1}
    root = min(roots, key=lambda s: (rank.get(s["attributes"].get("openinference.span.kind"), 2), s["start"]))

    question = message_text(root["attributes"].get("input.value", ""), QUESTION_KEYS)
    if not question.strip() or "output.value" not in root["attributes"]:
        return None
    answer = message_text(root["attributes"]["output.value"], ANSWER_KEYS).strip()

    tools = sorted((s for s in spans if s["attributes"].get("openinference.span.kind") == "TOOL"), key=lambda s: s["start"])
    tool_calls = [
        {"name": s["attributes"].get("tool.name") or s["name"], "args": tool_args(s), "status": tool_status(s)}
        for s in tools
    ]
    return {
        "trace_id": trace_id,
        "question": question,
        "tool_calls": tool_calls,
        "evidence": evidence(root, spans, tools),
        "final_answer": answer,
        "stop_reason": root["attributes"].get("observer.stop_reason"),
    }


def tool_status(span: dict) -> str:
    if "observer.tool.status" in span["attributes"]:
        return span["attributes"]["observer.tool.status"]
    return "error" if span["status"] in (2, "STATUS_CODE_ERROR") else "ok"


def tool_args(span: dict):
    for candidate in (span["attributes"].get("input.value"), span["attributes"].get("tool.parameters")):
        if isinstance(candidate, str):
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
    return span["attributes"].get("input.value", "")


def evidence(root: dict, spans: list[dict], tools: list[dict]) -> list[str]:
    explicit = _indexed(root["attributes"], ATTR_EVIDENCE)
    if explicit:
        return explicit
    out = []
    for span in sorted(spans, key=lambda s: s["start"]):
        if span["attributes"].get("openinference.span.kind") == "RETRIEVER":
            out += [d for d in _indexed(span["attributes"], ATTR_RETRIEVAL_DOC) if d.strip() not in out]
    if out:
        return out
    for span in tools:
        if tool_status(span) == "ok":
            text = message_text(span["attributes"].get("output.value", ""), OUTPUT_KEYS)
            out += [p.strip() for p in text.split("\n\n") if p.strip() and p.strip() not in out]
    return out


def message_text(value, keys) -> str:
    """Plain text as is; a JSON string unquoted; a JSON object searched for the first key holding text."""
    if not isinstance(value, str) or not value.strip():
        return "" if value is None else str(value)
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return value
    if isinstance(parsed, str):
        return parsed
    if not isinstance(parsed, dict):
        return value
    if isinstance(parsed.get("response"), dict):
        parsed = parsed["response"]
    for key in keys:
        if isinstance(parsed.get(key), str):
            return parsed[key]
    # An ADK message: {"new_message" | "content": {"parts": [{"text": ...}]}}
    for key in ("new_message", "content"):
        message = parsed.get(key)
        if isinstance(message, dict) and isinstance(message.get("parts"), list):
            return "".join(p.get("text", "") for p in message["parts"] if isinstance(p, dict) and not p.get("thought"))
    return value


def _indexed(attributes: dict, pattern) -> list[str]:
    found = {int(m.group(1)): v for k, v in attributes.items() if (m := pattern.match(k))}
    out = []
    for i in range(len(found)):
        if i not in found:
            break
        out.append(found[i])
    return out
