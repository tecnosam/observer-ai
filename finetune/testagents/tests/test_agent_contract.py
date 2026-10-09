import asyncio
import re
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import observer_agent.agent as agent_module
from observer_agent import AgentConfig, arun_agent, run_agent
from observer_agent.tools import BUDGET_EXHAUSTED_MESSAGE

from .fakes import ScriptedLlm, call, search_lookup_answer, text

QUESTION = "Which river flows through the capital of the country where the Eiffel Tower is?"

EXPECTED_KEYS = {
    "trace_id", "final_answer", "final_answer_raw", "tool_calls", "evidence", "steps",
    "used_search", "stop_reason", "model", "latency_ms",
}


@pytest.fixture
def use_script(monkeypatch):
    """Make the agent use a ScriptedLlm playing the given script instead of Ollama."""

    def install(script, delay_s=0.0):
        monkeypatch.setattr(
            agent_module, "build_model", lambda cfg: ScriptedLlm(script=script, delay_s=delay_s)
        )

    return install


@pytest.fixture
def happy_path(use_script):
    use_script(search_lookup_answer("paris", lambda obs: 'Final answer: "Seine".'))


def test_return_value_has_every_key_with_the_right_type(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(model="some-model"))

    assert set(result) == EXPECTED_KEYS
    assert isinstance(result["trace_id"], str) and re.fullmatch(r"[0-9a-f]{32}", result["trace_id"])
    assert isinstance(result["final_answer"], str)
    assert isinstance(result["final_answer_raw"], str)
    assert isinstance(result["tool_calls"], list)
    assert isinstance(result["evidence"], list) and all(isinstance(e, str) for e in result["evidence"])
    assert isinstance(result["steps"], list)
    assert isinstance(result["used_search"], bool)
    assert result["stop_reason"] in {"answered", "max_steps", "no_answer"}
    assert result["model"] == "some-model"
    assert isinstance(result["latency_ms"], float) and result["latency_ms"] > 0

    for tool_call in result["tool_calls"]:
        assert set(tool_call) == {"name", "args", "status", "result_preview"}
        assert tool_call["name"] in {"search", "lookup"}
        assert isinstance(tool_call["args"], dict)
        assert tool_call["status"] in {"ok", "not_found", "bad_args", "budget_exhausted"}
        assert isinstance(tool_call["result_preview"], str) and len(tool_call["result_preview"]) <= 300
    for step in result["steps"]:
        assert set(step) == {"thought", "tool", "args", "observation"}
        assert isinstance(step["thought"], str)
        assert isinstance(step["tool"], str)
        assert isinstance(step["args"], dict)
        assert isinstance(step["observation"], str)


def test_search_then_lookup_then_answer(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig())

    assert result["final_answer"] == "Seine"
    assert result["final_answer_raw"] == 'Final answer: "Seine".'
    assert result["stop_reason"] == "answered"
    assert result["used_search"] is True

    assert [(c["name"], c["args"], c["status"]) for c in result["tool_calls"]] == [
        ("search", {"query": QUESTION}, "ok"),
        ("lookup", {"title": "paris"}, "ok"),
    ]
    assert result["tool_calls"][1]["result_preview"] == "[Paris] " + paragraphs[1]["text"]

    assert len(result["steps"]) == len(result["tool_calls"]) == 2
    assert result["steps"][0]["thought"] == "I should search first."
    assert result["steps"][1]["thought"] == ""
    assert [s["tool"] for s in result["steps"]] == ["search", "lookup"]
    assert result["steps"][1]["observation"] == "[Paris] " + paragraphs[1]["text"]


def test_evidence_is_deduplicated_in_first_seen_order(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    evidence = result["evidence"]
    paris, eiffel = paragraphs[1]["text"], paragraphs[0]["text"]

    # search returned 3 paragraphs including Paris; lookup returned Paris again.
    assert len(evidence) == 3
    assert len(set(evidence)) == len(evidence)
    assert evidence.count(paris) == 1
    assert {paris, eiffel} <= set(evidence)
    search_blocks = result["steps"][0]["observation"].split("\n\n")
    assert [block.split("] ", 1)[1] for block in search_blocks] == evidence


def test_result_preview_is_first_300_chars(use_script):
    long_text = "river " * 200
    use_script(search_lookup_answer("Long", lambda obs: "x"))
    result = run_agent("river", [{"title": "Long", "text": long_text}], AgentConfig())
    full = f"[Long] {long_text}"
    assert result["tool_calls"][0]["result_preview"] == full[:300]
    assert result["steps"][0]["observation"] == full
    assert result["evidence"] == [long_text]


def test_budget_of_one_exhausts_on_the_second_call(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_tool_calls=1))

    assert [c["status"] for c in result["tool_calls"]] == ["ok", "budget_exhausted"]
    assert result["tool_calls"][1]["result_preview"] == BUDGET_EXHAUSTED_MESSAGE
    assert result["steps"][1]["observation"] == BUDGET_EXHAUSTED_MESSAGE
    assert result["used_search"] is True
    assert result["stop_reason"] == "answered"


def test_budget_of_zero_never_runs_a_tool(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_tool_calls=0))
    assert [c["status"] for c in result["tool_calls"]] == ["budget_exhausted"] * 2
    assert result["evidence"] == []
    assert result["used_search"] is False


def test_max_steps_without_a_final_reply(use_script, paragraphs):
    use_script([lambda q, obs: [text("still looking"), call("search", query="Paris")]])
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_steps=3, max_tool_calls=2))

    assert result["stop_reason"] == "max_steps"
    assert result["final_answer"] == ""
    assert result["final_answer_raw"] == ""
    assert [c["status"] for c in result["tool_calls"]] == ["ok", "ok", "budget_exhausted"]


def test_answer_on_the_last_allowed_step_counts(happy_path, paragraphs):
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_steps=3))
    assert result["stop_reason"] == "answered"
    assert result["final_answer"] == "Seine"


@pytest.mark.parametrize("reply", ["", "   \n"])
def test_empty_reply_is_no_answer(use_script, paragraphs, reply):
    use_script([lambda q, obs: [text(reply)]])
    result = run_agent(QUESTION, paragraphs, AgentConfig())

    assert result["stop_reason"] == "no_answer"
    assert result["final_answer"] == ""
    assert result["tool_calls"] == [] and result["steps"] == [] and result["evidence"] == []
    assert result["used_search"] is False


def test_long_reply_is_kept_and_raw_holds_the_original(use_script, paragraphs):
    raw = "Answer: " + " ".join(["word"] * 40) + "."
    use_script([lambda q, obs: [text(raw)]])
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    assert result["final_answer_raw"] == raw
    assert result["final_answer"] == " ".join(["word"] * 40)
    assert result["stop_reason"] == "answered"


def test_failed_tool_calls_have_statuses_and_do_not_count_as_search(use_script, paragraphs):
    use_script(
        [
            lambda q, obs: [call("search", query="   ")],
            lambda q, obs: [call("lookup", title="Quantum chromodynamics")],
            lambda q, obs: [text("unknown")],
        ]
    )
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    assert [c["status"] for c in result["tool_calls"]] == ["bad_args", "not_found"]
    assert "Available titles" in result["tool_calls"][1]["result_preview"]
    assert result["used_search"] is False
    assert result["evidence"] == []


def test_calls_adk_rejects_are_logged_as_bad_args_and_spend_budget(use_script, paragraphs):
    use_script(
        [
            lambda q, obs: [call("search", q="Paris")],  # wrong argument name
            lambda q, obs: [call("browse", url="http://x")],  # no such tool
            lambda q, obs: [call("lookup", title="Paris")],
            lambda q, obs: [text("Seine")],
        ]
    )
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_tool_calls=2))
    assert [(c["name"], c["status"]) for c in result["tool_calls"]] == [
        ("search", "bad_args"),
        ("browse", "bad_args"),
        ("lookup", "budget_exhausted"),
    ]
    assert "query" in result["steps"][0]["observation"]
    assert result["final_answer"] == "Seine"


def test_parallel_tool_calls_in_one_turn(use_script, paragraphs):
    use_script(
        [
            lambda q, obs: [text("both"), call("search", query="Everest"), call("lookup", title="Paris")],
            lambda q, obs: [text("Seine")],
        ]
    )
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    assert [(c["name"], c["status"]) for c in result["tool_calls"]] == [("search", "ok"), ("lookup", "ok")]
    assert [s["thought"] for s in result["steps"]] == ["both", "both"]
    assert result["steps"][1]["observation"].startswith("[Paris] ")


def test_infrastructure_failure_raises(use_script, paragraphs):
    use_script([ConnectionError("ollama is down")])
    with pytest.raises(ConnectionError, match="ollama is down"):
        run_agent(QUESTION, paragraphs, AgentConfig())


def test_infrastructure_failure_mid_run_raises(use_script, paragraphs):
    use_script([lambda q, obs: [call("search", query="Paris")], TimeoutError("timed out")])
    with pytest.raises(TimeoutError):
        run_agent(QUESTION, paragraphs, AgentConfig())


def test_invalid_limits_raise(paragraphs):
    with pytest.raises(ValueError):
        run_agent(QUESTION, paragraphs, AgentConfig(max_steps=0))


def test_arun_agent(happy_path, paragraphs):
    result = asyncio.run(arun_agent(QUESTION, paragraphs, AgentConfig()))
    assert set(result) == EXPECTED_KEYS
    assert result["final_answer"] == "Seine"


def test_run_agent_inside_a_running_event_loop(happy_path, paragraphs):
    async def main():
        assert asyncio.get_running_loop().is_running()
        return run_agent(QUESTION, paragraphs, AgentConfig())

    result = asyncio.run(main())
    assert result["final_answer"] == "Seine"
    assert [c["status"] for c in result["tool_calls"]] == ["ok", "ok"]


def test_run_agent_inside_a_running_loop_propagates_errors(use_script, paragraphs):
    use_script([ConnectionError("ollama is down")])

    async def main():
        return run_agent(QUESTION, paragraphs, AgentConfig())

    with pytest.raises(ConnectionError):
        asyncio.run(main())


def test_default_model_targets_ollama_without_the_v1_suffix():
    model = agent_module.build_model(AgentConfig(temperature=0.3, request_timeout_s=7.0))
    assert model.model == "ollama_chat/qwen2.5:7b"
    assert model._additional_args["api_base"] == "http://localhost:11434"
    assert model._additional_args["temperature"] == 0.3
    assert model._additional_args["timeout"] == 7.0
    assert agent_module._ollama_api_base("http://host:1234/v1/") == "http://host:1234"
    assert agent_module._ollama_api_base("http://host:1234") == "http://host:1234"


def test_concurrent_runs_do_not_share_state(use_script):
    runs = 8

    def paragraphs_for(i):
        return [
            {"title": f"Doc {i}", "text": f"The secret code of run {i} is CODE{i}X."},
            # Same title in every run, different text: a shared store would mix these up.
            {"title": "Common", "text": f"Common note belonging to run {i}."},
        ]

    def answer(obs):
        return "Answer: " + re.search(r"CODE\d+X", obs[0]).group(0)

    # The delay makes the 8 runs overlap on the 4 workers.
    use_script(search_lookup_answer("Common", answer), delay_s=0.02)

    seen_threads = set()

    def one(i):
        seen_threads.add(threading.get_ident())
        # Odd runs get a budget of 1: a shared counter would exhaust the even runs too.
        cfg = AgentConfig(max_tool_calls=1 if i % 2 else 6)
        return run_agent(f"What is the secret code of run {i}?", paragraphs_for(i), cfg)

    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(one, range(runs)))

    assert len(seen_threads) > 1
    assert len({r["trace_id"] for r in results}) == runs
    for i, result in enumerate(results):
        own = [p["text"] for p in paragraphs_for(i)]
        assert result["final_answer"] == f"CODE{i}X"
        assert result["stop_reason"] == "answered"
        assert result["tool_calls"][0]["args"] == {"query": f"What is the secret code of run {i}?"}
        assert len(result["tool_calls"]) == len(result["steps"]) == 2
        if i % 2:
            assert [c["status"] for c in result["tool_calls"]] == ["ok", "budget_exhausted"]
        else:
            assert [c["status"] for c in result["tool_calls"]] == ["ok", "ok"]
            assert result["steps"][1]["observation"] == f"[Common] Common note belonging to run {i}."
        assert result["evidence"] and set(result["evidence"]) <= set(own)
        assert own[0] in result["evidence"]


@pytest.mark.parametrize(
    "reply",
    [
        'search {"query":"Paris"}',
        'search {"query": "Paris"}\n',
        '<tool_call>search {"query": "Paris"}</tool_call>',
        '```json\nsearch {"query": "Paris"}\n```',
        'search({"query": "Paris"})',
    ],
)
def test_tool_call_written_as_text_is_executed(use_script, paragraphs, reply):
    use_script([lambda q, obs: [text(reply)], lambda q, obs: [text("Seine")]])
    result = run_agent(QUESTION, paragraphs, AgentConfig())
    assert [(c["name"], c["args"], c["status"]) for c in result["tool_calls"]] == [
        ("search", {"query": "Paris"}, "ok")
    ]
    assert result["final_answer"] == "Seine"
    assert result["stop_reason"] == "answered"


@pytest.mark.parametrize(
    "reply",
    [
        "search",
        "lookup Paris",
        'I would search {"query": "Paris"}',
        'search {"query": "Paris"} and then answer',
        'browse {"url": "x"}',
        "search {not json}",
        '{"query": "Paris"}',
        "Seine",
    ],
)
def test_ordinary_text_is_not_mistaken_for_a_tool_call(reply):
    assert agent_module.parse_text_tool_call(reply) is None


def test_max_steps_does_not_call_the_model_again(monkeypatch, paragraphs):
    turns = []

    def step(q, obs):
        turns.append(len(obs))
        return [call("search", query="Paris")]

    monkeypatch.setattr(agent_module, "build_model", lambda cfg: ScriptedLlm(script=[step]))
    result = run_agent(QUESTION, paragraphs, AgentConfig(max_steps=4))
    assert len(turns) == 4
    assert result["stop_reason"] == "max_steps"
    assert len(result["tool_calls"]) == 4
