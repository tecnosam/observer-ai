"""Sampler metrics, calibration and the no-model heuristic, as in the fine-tuning notebooks.

Normalized recall = (sampler - random) / (oracle - random) at a review budget:
0 is random sampling, 1 is the best possible. It is measured on rebalanced
test sets (every correct trace, wrong ones downsampled to a realistic base
rate), because a labeled test set is usually far more wrong than production.
"""

import math
import re

import numpy as np
from scipy.optimize import minimize_scalar
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .schemas import EvalOptions, LabeledTrace


def sampler_metrics(p, y, rate):
    y = np.asarray(y, dtype=bool)
    k = max(1, math.ceil(rate * len(y)))
    top_k = np.argsort(-np.asarray(p), kind="stable")[:k]   # ties keep the (shuffled) order of the set
    recall, random_r, oracle = y[top_k].sum() / y.sum(), k / len(y), min(1.0, k / y.sum())
    return recall, random_r, oracle, (recall - random_r) / (oracle - random_r)


def rebalanced_indices(y, base_rate, seed):
    """Every correct trace plus a seeded sample of wrong ones, shuffled, so wrong traces make up base_rate."""
    rng = np.random.default_rng(seed)
    correct, wrong = np.flatnonzero(~y), np.flatnonzero(y)
    n_wrong = round(base_rate * len(correct) / (1 - base_rate))
    if not 0 < n_wrong <= len(wrong):
        raise ValueError(f"the test portion cannot reach a {base_rate:.0%} base rate ({len(correct)} correct, {len(wrong)} wrong)")
    return rng.permutation(np.concatenate([correct, rng.choice(wrong, n_wrong, replace=False)]))


class Evaluator:
    """Scores signals on one test portion, every model on the same rebalanced sets."""

    def __init__(self, y_test, options: EvalOptions):
        self.y, self.options = np.asarray(y_test, dtype=bool), options
        self.resamples = {br: [rebalanced_indices(self.y, br, options.seed + i) for i in range(options.n_resamples)]
                          for br in options.base_rates}

    def rows(self, model: str, signal: str, p) -> list[dict]:
        p = np.asarray(p, dtype=float)
        auroc = float(roc_auc_score(self.y, p))
        out = []
        for br, sets in self.resamples.items():
            for rate in self.options.sample_rates:
                runs = np.array([sampler_metrics(p[idx], self.y[idx], rate) for idx in sets])
                out.append({
                    "model": model, "signal": signal, "base_rate": br, "sample_rate": rate,
                    "auroc_full_test": auroc,
                    "recall_mean": runs[:, 0].mean(), "recall_std": runs[:, 0].std(),
                    "random_recall": runs[:, 1].mean(), "oracle_recall": runs[:, 2].mean(),
                    "norm_recall_mean": runs[:, 3].mean(), "norm_recall_std": runs[:, 3].std(),
                })
        return out

    def headline(self, rows: list[dict]) -> dict[float, float]:
        """Normalized recall at the headline budget, per base rate."""
        return {r["base_rate"]: r["norm_recall_mean"] for r in rows if r["sample_rate"] == self.options.sample_rate}


def fit_temperature(p, y) -> float:
    logits = _logit(p)
    def nll(T):
        q = np.clip(1 / (1 + np.exp(-logits / T)), 1e-9, 1 - 1e-9)
        return -np.mean(y * np.log(q) + (1 - y) * np.log(1 - q))
    return float(minimize_scalar(nll, bounds=(0.05, 20), method="bounded").x)


def apply_temperature(p, T):
    return 1 / (1 + np.exp(-_logit(p) / T))


def ece(p, y, bins=10) -> float:
    p, y = np.asarray(p), np.asarray(y, dtype=float)
    idx = np.clip(np.digitize(p, np.linspace(0, 1, bins + 1)) - 1, 0, bins - 1)
    return float(sum(abs(p[idx == b].mean() - y[idx == b].mean()) * (idx == b).mean() for b in range(bins) if (idx == b).any()))


def _logit(p):
    p = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


# ---- heuristic baseline: simple rules, no model. Higher means more suspicious.

def _words(text):
    return re.findall(r"[a-z0-9]+", str(text).lower())


def heuristic_features(t: LabeledTrace) -> dict:
    answer = _words(t.final_answer)
    evidence = set(_words(" ".join(t.evidence)))
    return {
        "short_answer": -len(answer),
        "few_evidence": -len(t.evidence),
        "no_search": float(not (t.used_search if t.used_search is not None else any(c.get("name") == "search" for c in t.tool_calls))),
        "tool_error": float(any(c.get("status", "ok") != "ok" for c in t.tool_calls)),
        "bad_stop": float((t.stop_reason or "answered") != "answered"),
        "low_overlap": 1.0 - (np.mean([w in evidence for w in answer]) if answer else 0.0),
    }


def heuristic_scores(train: list[LabeledTrace], test: list[LabeledTrace], seed: int) -> dict[str, np.ndarray]:
    """Each rule on its own, and all rules combined by a logistic regression fitted on the train split."""
    rules = list(heuristic_features(train[0]))
    X_train = np.array([[heuristic_features(t)[r] for r in rules] for t in train])
    X_test = np.array([[heuristic_features(t)[r] for r in rules] for t in test])
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000, random_state=seed))
    model.fit(X_train, [t.is_wrong for t in train])
    return {"logreg": model.predict_proba(X_test)[:, 1], **{r: X_test[:, j] for j, r in enumerate(rules)}}
