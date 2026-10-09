"""Per-run retrieval tools: a small BM25 store, `search`, `lookup` and the tool budget.

Nothing here is module-level state. Every run builds its own `ParagraphStore`
and `Toolbox`, so concurrent runs cannot see each other's paragraphs or
budget counters.
"""

import math
import re
import threading
from collections import Counter
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Callable

from google.adk.tools.tool_context import ToolContext

STATUS_OK = "ok"
STATUS_NOT_FOUND = "not_found"
STATUS_BAD_ARGS = "bad_args"
STATUS_BUDGET_EXHAUSTED = "budget_exhausted"

BUDGET_EXHAUSTED_MESSAGE = (
    "Tool budget exhausted. Answer now using what you already have."
)

TOP_K = 3
FUZZY_THRESHOLD = 0.6

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


class BM25:
    """Okapi BM25 over a fixed list of tokenized documents."""

    def __init__(self, docs: list[list[str]], k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.term_freqs = [Counter(doc) for doc in docs]
        self.doc_lens = [len(doc) for doc in docs]
        self.avg_len = (sum(self.doc_lens) / len(docs)) if docs else 0.0
        doc_freq: Counter[str] = Counter()
        for tf in self.term_freqs:
            doc_freq.update(tf.keys())
        n = len(docs)
        # The +1 inside the log keeps idf positive for terms in most documents.
        self.idf = {
            term: math.log(1.0 + (n - df + 0.5) / (df + 0.5))
            for term, df in doc_freq.items()
        }

    def scores(self, query_tokens: list[str]) -> list[float]:
        out = []
        for tf, doc_len in zip(self.term_freqs, self.doc_lens):
            norm = self.k1 * (1.0 - self.b + self.b * doc_len / (self.avg_len or 1.0))
            score = 0.0
            for term in query_tokens:
                freq = tf.get(term)
                if freq:
                    score += self.idf[term] * freq * (self.k1 + 1.0) / (freq + norm)
            out.append(score)
        return out


@dataclass(frozen=True)
class Paragraph:
    title: str
    text: str

    def render(self) -> str:
        return f"[{self.title}] {self.text}"


class ParagraphStore:
    """The paragraphs of one run, indexed for BM25 search and title lookup."""

    def __init__(self, paragraphs: list[dict]):
        self.paragraphs = [
            Paragraph(title=str(p.get("title", "")), text=str(p.get("text", "")))
            for p in paragraphs
        ]
        self._bm25 = BM25([tokenize(f"{p.title} {p.text}") for p in self.paragraphs])

    @property
    def titles(self) -> list[str]:
        return [p.title for p in self.paragraphs]

    def search(self, query: str, k: int = TOP_K) -> list[Paragraph]:
        """Top-k paragraphs by BM25 score. Paragraphs that share no term with the query are dropped."""
        scores = self._bm25.scores(tokenize(query))
        ranked = sorted(range(len(scores)), key=lambda i: (-scores[i], i))
        return [self.paragraphs[i] for i in ranked[:k] if scores[i] > 0.0]

    def lookup(self, title: str) -> Paragraph | None:
        """Exact title, then case-insensitive, then the closest title above FUZZY_THRESHOLD."""
        wanted = title.strip()
        for p in self.paragraphs:
            if p.title == wanted:
                return p
        folded = wanted.casefold()
        for p in self.paragraphs:
            if p.title.strip().casefold() == folded:
                return p
        best, best_ratio = None, 0.0
        for p in self.paragraphs:
            ratio = SequenceMatcher(None, folded, p.title.strip().casefold()).ratio()
            if ratio > best_ratio:
                best, best_ratio = p, ratio
        return best if best_ratio >= FUZZY_THRESHOLD else None


@dataclass
class ToolCallRecord:
    name: str
    args: dict[str, Any]
    status: str
    result: str
    paragraphs: list[str] = field(default_factory=list)  # full texts returned to the model
    call_id: str | None = None


class Toolbox:
    """The tools of one run, with the run's budget and call log.

    Every call goes through `call`, which counts it against `max_tool_calls`
    before anything executes. Once the budget is spent the tool body is never
    reached.
    """

    def __init__(self, store: ParagraphStore, max_tool_calls: int):
        self.store = store
        self.max_tool_calls = max_tool_calls
        self.calls: list[ToolCallRecord] = []
        # ADK may run sync tools on a thread pool when the model issues parallel calls.
        self._lock = threading.Lock()

    def call(self, name: str, args: dict[str, Any], call_id: str | None = None) -> ToolCallRecord:
        with self._lock:
            if len(self.calls) >= self.max_tool_calls:
                record = ToolCallRecord(
                    name, dict(args), STATUS_BUDGET_EXHAUSTED, BUDGET_EXHAUSTED_MESSAGE
                )
            elif name == "search":
                record = self._search(args)
            elif name == "lookup":
                record = self._lookup(args)
            else:
                raise ValueError(f"unknown tool: {name}")
            record.call_id = call_id
            self.calls.append(record)
            return record

    def record_rejected(
        self, name: str, args: dict[str, Any], message: str, call_id: str | None = None
    ) -> ToolCallRecord:
        """Log a call the framework turned down before it reached a tool.

        This covers an unknown tool name or missing arguments. The call is logged
        as `bad_args` and spends budget like any other call.
        """
        with self._lock:
            record = ToolCallRecord(name, dict(args), STATUS_BAD_ARGS, message, call_id=call_id)
            self.calls.append(record)
            return record

    def search(self, query: str) -> ToolCallRecord:
        return self.call("search", {"query": query})

    def lookup(self, title: str) -> ToolCallRecord:
        return self.call("lookup", {"title": title})

    def _search(self, args: dict[str, Any]) -> ToolCallRecord:
        query = args.get("query")
        if not isinstance(query, str) or not query.strip():
            return ToolCallRecord(
                "search", dict(args), STATUS_BAD_ARGS,
                "Invalid arguments: `query` must be a non-empty string of keywords.",
            )
        hits = self.store.search(query)
        if not hits:
            return ToolCallRecord(
                "search", dict(args), STATUS_NOT_FOUND,
                "No paragraphs matched the query. Try different keywords. "
                f"Available titles: {self._titles()}",
            )
        return ToolCallRecord(
            "search", dict(args), STATUS_OK,
            "\n\n".join(p.render() for p in hits),
            paragraphs=[p.text for p in hits],
        )

    def _lookup(self, args: dict[str, Any]) -> ToolCallRecord:
        title = args.get("title")
        if not isinstance(title, str) or not title.strip():
            return ToolCallRecord(
                "lookup", dict(args), STATUS_BAD_ARGS,
                "Invalid arguments: `title` must be a non-empty paragraph title.",
            )
        hit = self.store.lookup(title)
        if hit is None:
            return ToolCallRecord(
                "lookup", dict(args), STATUS_NOT_FOUND,
                f"No paragraph titled '{title.strip()}'. Available titles: {self._titles()}",
            )
        return ToolCallRecord(
            "lookup", dict(args), STATUS_OK, hit.render(), paragraphs=[hit.text]
        )

    def _titles(self) -> str:
        return "; ".join(self.store.titles) or "(none)"

    def adk_tools(self) -> list[Callable[..., str]]:
        """`search` and `lookup` as plain functions bound to this run.

        ADK turns the signatures and docstrings into the tool declarations the
        model sees, and injects `tool_context` itself (it is hidden from the model).
        """
        toolbox = self

        def search(query: str, tool_context: ToolContext) -> str:
            """Search the reference paragraphs by keyword and return the 3 most relevant ones.

            Use this first to find facts. Each call is a fresh keyword search, so
            include the specific names, places or terms you need. For multi-hop
            questions, search once per hop, using what the previous result told you.

            Args:
                query: Keywords to search for, for example "Marie Curie birthplace". Must not be empty.

            Returns:
                Up to 3 paragraphs, each formatted as "[Title] text" and separated by blank lines.
            """
            return toolbox.call(
                "search", {"query": query}, tool_context.function_call_id
            ).result

        def lookup(title: str, tool_context: ToolContext) -> str:
            """Fetch one reference paragraph by its title.

            Use this when you already know the title, for example a "[Title]" seen in
            a search result or an entity named in the question. Matching ignores case
            and tolerates small typos.

            Args:
                title: The title of the paragraph to fetch, for example "Marie Curie".

            Returns:
                The paragraph formatted as "[Title] text". If no title matches, a
                message listing the available titles.
            """
            return toolbox.call(
                "lookup", {"title": title}, tool_context.function_call_id
            ).result

        return [search, lookup]
