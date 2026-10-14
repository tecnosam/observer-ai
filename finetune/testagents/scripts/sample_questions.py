"""Print a few questions the chat agent can answer from its corpus.

    uv run python scripts/sample_questions.py [-n 4]

Reads chat/questions.json, written by scripts/build_chat_corpus.py, and picks
at random, so every start shows different ones. Once scripts/grade_questions.py
has run, half are questions the agent got right and half are ones it got
wrong, so both a correct and a wrong trace can be sent to the Observer engine.
Without the file the agent searches only the smoke-test paragraphs, and the
smoke question is shown.
"""

import argparse
import json
import random
from pathlib import Path

from observer_agent.samples import SMOKE_QUESTION

QUESTIONS = Path(__file__).resolve().parent.parent / "chat" / "questions.json"


def show(title: str, items: list[dict], with_agent_answer: bool = False) -> None:
    print(f"\n{title}\n")
    for item in items:
        print(f"  - {item['question'].strip()}")
        print(f"      expected: {item['answer']}")
        if with_agent_answer:
            print(f"      agent said: {item.get('agent_answer') or '(no answer)'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-n", type=int, default=4)
    args = parser.parse_args()

    if not QUESTIONS.exists():
        show("Sample question to ask:", [{"question": SMOKE_QUESTION, "answer": "Seine"}])
        print("\n(Only the smoke-test paragraphs are loaded. Run `make corpus` for about 1,000 more.)\n")
        return

    questions = json.loads(QUESTIONS.read_text())
    right = [q for q in questions if q.get("label") == "correct"]
    wrong = [q for q in questions if q.get("label") == "wrong"]
    if not right or not wrong:
        show("Sample questions to ask:", random.sample(questions, min(args.n, len(questions))))
        print("\n(Run `make grade` to see which ones the agent gets right and which it gets wrong.)\n")
        return

    half = max(1, args.n // 2)
    show(
        "Questions the agent got RIGHT (the engine should drop these):",
        random.sample(right, min(half, len(right))),
    )
    show(
        "Questions the agent got WRONG (the engine should keep these):",
        random.sample(wrong, min(args.n - half, len(wrong))),
        with_agent_answer=True,
    )
    print(
        f"\n(From one graded run of {right[0].get('model', 'the agent')}: {len(right)} right, {len(wrong)} wrong "
        f"of {len(questions)}. The agent can answer differently in a chat, and keep or drop is the engine's call.)\n"
    )


if __name__ == "__main__":
    main()
