"""Build a paragraph corpus for the chat UI from HotpotQA.

    uv run --with datasets python scripts/build_chat_corpus.py [--questions 100] [--out chat/corpus.json]

Takes the first N validation questions of the distractor set (10 paragraphs
each) and writes their paragraphs to chat/corpus.json. The questions and
their gold answers go next to it, in questions.json, for `make chat` to show
as samples. The chat agent picks the corpus up on its next start.
"""

import argparse
import json
from pathlib import Path

DEFAULT_OUT = Path(__file__).resolve().parent.parent / "chat" / "corpus.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--questions", type=int, default=100)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    from datasets import load_dataset

    rows = load_dataset("hotpotqa/hotpot_qa", "distractor", split=f"validation[:{args.questions}]")
    paragraphs: dict[str, str] = {}
    for row in rows:
        for title, sentences in zip(row["context"]["title"], row["context"]["sentences"]):
            paragraphs.setdefault(title, "".join(sentences).strip())

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps([{"title": t, "text": x} for t, x in paragraphs.items()], ensure_ascii=False, indent=1)
    )
    questions_out = args.out.with_name("questions.json")
    questions_out.write_text(
        json.dumps(
            [{"question": row["question"], "answer": row["answer"]} for row in rows],
            ensure_ascii=False,
            indent=1,
        )
    )
    print(f"wrote {len(paragraphs)} paragraphs from {len(rows)} questions to {args.out}")
    print(f"wrote the questions to {questions_out}; `make chat` shows a few of them")


if __name__ == "__main__":
    main()
