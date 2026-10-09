import pytest

from observer_agent.answer import clean_answer


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Final answer: Seine", "Seine"),
        ("final answer:Seine", "Seine"),
        ("Answer: 1889", "1889"),
        ("ANSWER:   yes", "yes"),
        ("The answer is Seine.", "Seine"),
        ("The final answer is: Seine", "Seine"),
        ("**Final Answer:** Seine", "Seine"),
    ],
)
def test_strips_prefixes(raw, expected):
    assert clean_answer(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('"Seine"', "Seine"),
        ("'Seine'", "Seine"),
        ("“Seine”", "Seine"),
        ("`Seine`", "Seine"),
        ("**Seine**", "Seine"),
        ('Answer: "The Seine."', "The Seine"),
        ('"Final answer: Seine."', "Seine"),
    ],
)
def test_strips_surrounding_quotes(raw, expected):
    assert clean_answer(raw) == expected


def test_keeps_inner_and_unbalanced_quotes():
    assert clean_answer('The "Iron Lady"') == 'The "Iron Lady"'
    assert clean_answer("Rock 'n' Roll") == "Rock 'n' Roll"
    assert clean_answer("O'Brien") == "O'Brien"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Seine.", "Seine"),
        ("  Seine.  \n", "Seine"),
        ("3.5", "3.5"),
        ("Washington D.C.", "Washington D.C."),
        ("U.S.", "U.S."),
        ("yes", "yes"),
    ],
)
def test_trailing_period(raw, expected):
    assert clean_answer(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "\n\t", None, '""', "Answer:"])
def test_empty_input(raw):
    assert clean_answer(raw) == ""


def test_does_not_strip_words_that_merely_start_like_a_prefix():
    assert clean_answer("Answering machine") == "Answering machine"


def test_long_reply_is_kept():
    raw = "Answer: " + " ".join(["word"] * 30) + "."
    cleaned = clean_answer(raw)
    assert len(cleaned.split()) == 30
    assert cleaned == " ".join(["word"] * 30)
