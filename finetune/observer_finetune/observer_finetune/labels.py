"""Turning whatever a dataset calls its verdict into correct / partial / wrong."""

import re
import string
from collections import Counter

CORRECT_F1 = 0.8
PARTIAL_F1 = 0.3

_SYNONYMS = {
    "correct": "correct", "right": "correct", "pass": "correct", "good": "correct",
    "partial": "partial", "partially_correct": "partial", "partially correct": "partial",
    "wrong": "wrong", "incorrect": "wrong", "fail": "wrong", "bad": "wrong",
}


def normalize_label(value) -> str | None:
    """A label string, or a 0..1 score where higher is more correct. None if it cannot be read."""
    if isinstance(value, bool):
        return None  # ambiguous: true could mean "correct" or "is wrong"
    if isinstance(value, (int, float)):
        return "correct" if value >= CORRECT_F1 else "partial" if value >= PARTIAL_F1 else "wrong"
    if isinstance(value, str):
        return _SYNONYMS.get(value.strip().lower())
    return None


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


def label_from_reference(final_answer: str, reference: str) -> str:
    """Label an answer against a gold answer, with the eval dataset's rule (observer_eval_dataset_prep.ipynb)."""
    pred = (final_answer or "").strip()
    if not pred:
        return "wrong"
    if normalize_answer(pred) == normalize_answer(reference):
        return "correct"
    return normalize_label(f1_score(pred, reference))
