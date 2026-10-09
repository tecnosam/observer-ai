from observer_agent.tools import (
    BUDGET_EXHAUSTED_MESSAGE,
    ParagraphStore,
    Toolbox,
)


def make_toolbox(paragraphs, max_tool_calls=6):
    return Toolbox(ParagraphStore(paragraphs), max_tool_calls)


def test_bm25_ranks_obvious_paragraph_first(paragraphs):
    store = ParagraphStore(paragraphs)
    assert store.search("highest mountain on Earth")[0].title == "Mount Everest"
    assert store.search("coral reef Australia")[0].title == "Great Barrier Reef"
    assert store.search("who created the Python language")[0].title == "Python (programming language)"


def test_bm25_matches_on_title_terms(paragraphs):
    store = ParagraphStore(paragraphs)
    assert store.search("Photosynthesis")[0].title == "Photosynthesis"


def test_search_returns_top_3_blocks(paragraphs):
    record = make_toolbox(paragraphs).search("capital of France, reef, mountain")
    assert record.status == "ok"
    blocks = record.result.split("\n\n")
    assert len(blocks) == 3
    assert all(block.startswith("[") for block in blocks)
    assert blocks[0].startswith("[Paris] Paris is the capital of France.")
    assert len(record.paragraphs) == 3
    assert record.paragraphs[0] == paragraphs[1]["text"]


def test_search_empty_or_whitespace_query_is_bad_args(paragraphs):
    toolbox = make_toolbox(paragraphs)
    assert toolbox.search("").status == "bad_args"
    assert toolbox.search("   \n\t").status == "bad_args"
    assert toolbox.call("search", {"query": None}).status == "bad_args"


def test_search_without_any_matching_term_is_not_found(paragraphs):
    record = make_toolbox(paragraphs).search("zzzz qqqq")
    assert record.status == "not_found"
    assert record.paragraphs == []


def test_lookup_exact(paragraphs):
    record = make_toolbox(paragraphs).lookup("Paris")
    assert record.status == "ok"
    assert record.result == "[Paris] Paris is the capital of France. It lies on the river Seine."
    assert record.paragraphs == [paragraphs[1]["text"]]


def test_lookup_case_insensitive(paragraphs):
    record = make_toolbox(paragraphs).lookup("eiffel TOWER")
    assert record.status == "ok"
    assert record.result.startswith("[Eiffel Tower] ")


def test_lookup_exact_wins_over_case_insensitive():
    toolbox = make_toolbox(
        [{"title": "paris", "text": "lowercase"}, {"title": "Paris", "text": "capitalised"}]
    )
    assert toolbox.lookup("Paris").result == "[Paris] capitalised"
    assert toolbox.lookup("PARIS").result == "[paris] lowercase"


def test_lookup_fuzzy(paragraphs):
    toolbox = make_toolbox(paragraphs)
    assert toolbox.lookup("Eifel Towr").result.startswith("[Eiffel Tower] ")
    assert toolbox.lookup("Mt Everest").result.startswith("[Mount Everest] ")


def test_lookup_missing_lists_available_titles(paragraphs):
    record = make_toolbox(paragraphs).lookup("Quantum chromodynamics")
    assert record.status == "not_found"
    assert record.paragraphs == []
    for p in paragraphs:
        assert p["title"] in record.result


def test_lookup_empty_title_is_bad_args(paragraphs):
    assert make_toolbox(paragraphs).lookup("  ").status == "bad_args"


def test_budget_exhausted_after_n_calls_and_tool_not_executed(paragraphs):
    toolbox = make_toolbox(paragraphs, max_tool_calls=2)
    executed = []
    real_search, real_lookup = toolbox.store.search, toolbox.store.lookup
    toolbox.store.search = lambda q, **kw: executed.append(("search", q)) or real_search(q, **kw)
    toolbox.store.lookup = lambda t: executed.append(("lookup", t)) or real_lookup(t)

    assert toolbox.search("Paris").status == "ok"
    assert toolbox.search("").status == "bad_args"  # failed calls still spend budget
    assert len(executed) == 1

    third = toolbox.search("Everest")
    fourth = toolbox.lookup("Paris")
    for record in (third, fourth):
        assert record.status == "budget_exhausted"
        assert record.result == BUDGET_EXHAUSTED_MESSAGE
        assert record.paragraphs == []
    assert len(executed) == 1, "tool body must not run once the budget is spent"
    assert [c.status for c in toolbox.calls] == ["ok", "bad_args", "budget_exhausted", "budget_exhausted"]


def test_toolboxes_do_not_share_state(paragraphs):
    a = make_toolbox(paragraphs, max_tool_calls=1)
    b = make_toolbox([{"title": "Only", "text": "other run"}], max_tool_calls=1)
    a.search("Paris")
    assert b.calls == []
    assert b.lookup("Only").status == "ok"
    assert a.lookup("Only").status == "budget_exhausted"


def test_adk_tools_are_named_and_documented(paragraphs):
    search, lookup = make_toolbox(paragraphs).adk_tools()
    assert (search.__name__, lookup.__name__) == ("search", "lookup")
    assert "query" in search.__doc__ and "title" in lookup.__doc__
