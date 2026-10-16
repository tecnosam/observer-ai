"""Train / calibration / test, always by question, never by trace.

Natural and seeded runs of one question must land on the same side, or the
test portion measures memory instead of judgement.
"""

import random

from .schemas import LabeledTrace


def split_traces(traces: list[LabeledTrace], heldout_frac: float, seed: int):
    """Returns (train, heldout, method). Uses the data's own split field when every trace has one."""
    if all(t.split is not None for t in traces):
        unknown = {t.split for t in traces} - {"train", "heldout"}
        if unknown:
            raise ValueError(f"unknown split values {sorted(unknown)}; use train or heldout")
        train = [t for t in traces if t.split == "train"]
        heldout = [t for t in traces if t.split == "heldout"]
        method = "dataset_split_field"
    else:
        heldout_keys = _pick_keys(traces, heldout_frac, seed)
        train = [t for t in traces if t.question_key not in heldout_keys]
        heldout = [t for t in traces if t.question_key in heldout_keys]
        method = f"group_by_question(heldout_frac={heldout_frac}, seed={seed})"
    random.Random(seed).shuffle(train)
    _assert_disjoint(train, heldout)
    return train, heldout, method


def split_heldout(heldout: list[LabeledTrace], calib_frac: float, seed: int):
    """Returns (calibration, test), again by question."""
    calib_keys = _pick_keys(heldout, calib_frac, seed)
    calib = [t for t in heldout if t.question_key in calib_keys]
    test = [t for t in heldout if t.question_key not in calib_keys]
    _assert_disjoint(calib, test)
    return calib, test


def _pick_keys(traces, frac, seed):
    keys = sorted({t.question_key for t in traces})
    random.Random(seed).shuffle(keys)
    return set(keys[: round(len(keys) * frac)])


def _assert_disjoint(a, b):
    shared = {t.question_key for t in a} & {t.question_key for t in b}
    if shared:
        raise ValueError(f"{len(shared)} questions are on both sides of the split, e.g. {sorted(shared)[:3]}")
