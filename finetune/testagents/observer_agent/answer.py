"""Final answer cleaning: turn the model's last reply into a short answer string."""

import re

_PREFIX_RE = re.compile(
    # Optional markdown emphasis around the label, as in "**Final Answer:** Seine".
    r"^\s*[*_]{0,2}(?:the\s+)?(?:final\s+answer|answer)\s*(?:is\b\s*:?|:)\s*[*_]{0,2}\s*",
    re.IGNORECASE,
)
_QUOTE_PAIRS = {'"': '"', "'": "'", "`": "`", "“": "”", "‘": "’", "«": "»"}
_MARKDOWN_WRAPPERS = ("**", "__", "*", "_")


def _strip_wrappers(text: str) -> str:
    """Remove matching quotes or markdown emphasis around the whole string."""
    changed = True
    while changed and len(text) >= 2:
        changed = False
        closing = _QUOTE_PAIRS.get(text[0])
        if closing and text.endswith(closing):
            text = text[1:-1].strip()
            changed = True
            continue
        for mark in _MARKDOWN_WRAPPERS:
            if text.startswith(mark) and text.endswith(mark) and len(text) > 2 * len(mark):
                text = text[len(mark):-len(mark)].strip()
                changed = True
                break
    return text


def _strip_trailing_period(text: str) -> str:
    if not text.endswith(".") or text.endswith(".."):
        return text
    # Keep the period of an abbreviation such as "U.S." or "Washington D.C.".
    last_token = text.split()[-1]
    if "." in last_token[:-1]:
        return text
    return text[:-1].rstrip()


def clean_answer(raw: str | None) -> str:
    """Strip answer prefixes, surrounding quotes and a trailing period.

    Long replies (over 20 words) get the same light cleaning and are otherwise
    kept as they are; the caller keeps the untouched text in `final_answer_raw`.
    """
    text = (raw or "").strip()
    previous = None
    while text and text != previous:
        previous = text
        text = _strip_wrappers(text)
        text = _PREFIX_RE.sub("", text, count=1).strip()
        text = _strip_trailing_period(text)
    return text
