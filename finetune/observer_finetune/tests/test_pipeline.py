"""The whole pipeline with a fake model factory: no Kev, no GPU, no network."""

import json
from contextlib import nullcontext

import numpy as np
import pytest

from observer_finetune.models.base import AbstractModelFactory, AbstractScorer, AbstractTrainer
from observer_finetune.pipeline import FinetunePipeline
from observer_finetune.questions import QUESTIONS
from observer_finetune.schemas import EvalOptions
from observer_finetune.sources.jsonl import JsonlTraceSource


class FakeScorer(AbstractScorer):
    """Answers from the slice text: knows the truth with some noise, more of it when skill is low."""

    def __init__(self, skill: float):
        self.skill, self.rng = skill, np.random.default_rng(0)

    def answer(self, state, questions):
        lines = state.split("\n")
        q = int(lines[1].split("city ")[1].rstrip("?"))
        wrong = lines[-1] != f"river {q}"
        p = float(np.clip(self.skill * wrong + (1 - self.skill) * self.rng.random(), 0.01, 0.99))
        return {"answer_wrong": {"noul": p}, "keep_or_drop": {"probabilities": {"keep": p, "drop": 1 - p}}}


class FakeTrainer(AbstractTrainer):
    def prepare(self, train, work_dir, options):
        return {"records": len(train), "max_state": 384, "max_branch": 1024, "max_packed": 2048, "state_tokens": {"max": 100}}

    def train(self, plan, checkpoint_dir, log_path, options):
        checkpoint_dir.mkdir()
        (checkpoint_dir / "weights").write_text("fake")
        log_path.write_text("trained\n")
        return {"records": plan["records"], "dropped": 0, "dropped_frac": 0.0, "minutes": 0.0}


class FakeFactory(AbstractModelFactory):
    def __init__(self, zero_shot_skill, finetuned_skill):
        self.skills = {None: zero_shot_skill, "finetuned": finetuned_skill}
        self.opened = []

    def create_trainer(self):
        return FakeTrainer()

    def open_scorer(self, checkpoint):
        self.opened.append(checkpoint)
        return nullcontext(FakeScorer(self.skills[None if checkpoint is None else "finetuned"]))

    def describe(self):
        return {"family": "fake", "base": "fake-base", "init_from": "fake-init", "kev_path": "/fake", "kev_revision": "x"}


def run(tmp_path, traces_file, zero_shot_skill, finetuned_skill):
    factory = FakeFactory(zero_shot_skill, finetuned_skill)
    pipeline = FinetunePipeline(JsonlTraceSource(traces_file), factory, tmp_path / "run", eval_options=EvalOptions(n_resamples=5))
    return pipeline.run(), factory


def test_a_run_writes_everything_the_engine_and_a_person_need(tmp_path, traces_file):
    result, factory = run(tmp_path, traces_file, zero_shot_skill=0.0, finetuned_skill=0.9)
    run_dir = tmp_path / "run"
    for name in ("checkpoint/weights", "manifest.json", "report.md", "results.csv", "train.log",
                 "data/train.jsonl", "data/calibration.jsonl", "data/test.jsonl"):
        assert (run_dir / name).exists(), name
    manifest = json.loads((run_dir / "manifest.json").read_text())
    # What the engine's config loader reads
    assert manifest["questions"] == QUESTIONS
    assert manifest["questions"]["answer_wrong"]["type"] == "noul"
    assert set(manifest["temperatures"]) == set(QUESTIONS)
    assert manifest["metrics"]["finetuned"]["auroc"] > 0.9
    assert result.warnings == []
    assert factory.opened == [None, run_dir / "checkpoint"]   # the released model before training, the run after


def test_a_run_that_does_not_improve_still_writes_the_checkpoint_and_warns(tmp_path, traces_file, capsys):
    result, _ = run(tmp_path, traces_file, zero_shot_skill=0.9, finetuned_skill=0.0)
    assert (tmp_path / "run" / "checkpoint" / "weights").exists()
    assert any("did not beat the released model" in w for w in result.warnings)
    assert "WARNING: fine-tuning did not beat" in capsys.readouterr().out
    assert "## Warnings" in (tmp_path / "run" / "report.md").read_text()


def test_a_relative_run_dir_becomes_absolute(tmp_path, traces_file, monkeypatch):
    # The trainer and the servers run from Kev's checkout, so every path they get must be absolute.
    monkeypatch.chdir(tmp_path)
    factory = FakeFactory(0.0, 0.9)
    FinetunePipeline(JsonlTraceSource(traces_file), factory, "runs/relative", eval_options=EvalOptions(n_resamples=5)).run()
    assert factory.opened[1] == tmp_path / "runs" / "relative" / "checkpoint"
    assert factory.opened[1].is_absolute()


def test_an_existing_checkpoint_is_never_overwritten(tmp_path, traces_file):
    (tmp_path / "run" / "checkpoint").mkdir(parents=True)
    with pytest.raises(ValueError, match="already exists"):
        run(tmp_path, traces_file, 0.0, 0.9)


def test_check_reports_data_that_cannot_be_trained_on(tmp_path):
    path = tmp_path / "t.jsonl"
    path.write_text("\n".join(json.dumps({"qid": f"q{i}", "question": f"q{i}", "final_answer": "a", "label": "correct"})
                              for i in range(50)) + "\n")
    report = FinetunePipeline(JsonlTraceSource(path), FakeFactory(0, 0), tmp_path / "run").check()
    assert not report.ok
    assert any("both correct and wrong" in p for p in report.problems)
    with pytest.raises(ValueError, match="cannot be trained on"):
        FinetunePipeline(JsonlTraceSource(path), FakeFactory(0, 0), tmp_path / "run").run()
