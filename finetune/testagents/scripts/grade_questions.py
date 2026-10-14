"""Find out which chat questions the agent gets right and which it gets wrong.

    uv run python scripts/grade_questions.py [--workers 2] [--redo] [--model qwen2.5:7b]

Runs the agent once on every question in chat/questions.json, over the same
corpus the chat UI searches, and stores its answer and a label next to the
question. `make chat` then shows samples of both kinds. Needs Ollama; about
ten minutes for 100 questions. Progress is saved after every question, so an
interrupted run continues where it stopped.

Labels follow the eval dataset (observer_eval_dataset_prep.ipynb): token F1
against the gold answer, correct at 0.8 or an exact match, partial at 0.3,
else wrong. Nothing is exported.
"""

import argparse
import json
import re
import string
import sys
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from observer_agent import AgentConfig, run_agent

CHAT_DIR = Path(__file__).resolve().parent.parent / "chat"
CORRECT_F1 = 0.8
PARTIAL_F1 = 0.3


def normalize_answer(s: str) -> str:
    s = s.lower()
    s = "".join(ch for ch in s if ch not in set(string.punctuation))
    s = re.sub(r"\b(a|an|the)\b", " ", s)
    return " ".join(s.split())


def f1_score(pred: str, gold: str) -> float:
    p, g = normalize_answer(pred), normalize_answer(gold)
    if (p in {"yes", "no", "noanswer"} or g in {"yes", "no", "noanswer"}) and p != g:
        return 0.0
    pt, gt = p.split(), g.split()
    same = sum((Counter(pt) & Counter(gt)).values())
    if same == 0:
        return 0.0
    precision, recall = same / len(pt), same / len(gt)
    return 2 * precision * recall / (precision + recall)


def label_answer(pred: str, gold: str) -> str:
    pred = pred.strip()
    if not pred:
        return "wrong"
    f1 = f1_score(pred, gold)
    if normalize_answer(pred) == normalize_answer(gold) or f1 >= CORRECT_F1:
        return "correct"
    return "partial" if f1 >= PARTIAL_F1 else "wrong"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--redo", action="store_true", help="grade every question again")
    parser.add_argument("--model", default=AgentConfig().model)
    args = parser.parse_args()

    questions_path = CHAT_DIR / "questions.json"
    if not questions_path.exists():
        print("grade: chat/questions.json is missing; run `make corpus` first", file=sys.stderr)
        return 1
    questions = json.loads(questions_path.read_text())
    paragraphs = json.loads((CHAT_DIR / "corpus.json").read_text())
    cfg = AgentConfig(model=args.model)  # exporting stays off

    todo = [q for q in questions if args.redo or q.get("model") != cfg.model or "label" not in q]
    print(f"grading {len(todo)} of {len(questions)} questions with {cfg.model}")
    lock = threading.Lock()
    done = 0

    def grade(q: dict) -> None:
        nonlocal done
        try:
            result = run_agent(q["question"], paragraphs, cfg)
        except Exception as exc:
            print(f"  failed: {q['question'][:60]}: {type(exc).__name__}: {exc}", file=sys.stderr)
            return
        with lock:
            q["agent_answer"] = result["final_answer"]
            q["label"] = label_answer(result["final_answer"], q["answer"])
            q["model"] = cfg.model
            done += 1
            questions_path.write_text(json.dumps(questions, ensure_ascii=False, indent=1))
            print(f"  [{done}/{len(todo)}] {q['label']:<8} {q['question'][:70]}", flush=True)

    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(grade, todo))

    counts = Counter(q.get("label", "ungraded") for q in questions)
    print("labels:", dict(counts))
    return 0 if done == len(todo) else 1


if __name__ == "__main__":
    sys.exit(main())
