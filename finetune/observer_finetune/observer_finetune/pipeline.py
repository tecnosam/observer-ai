import json
import time
from pathlib import Path

import numpy as np

from .metrics import Evaluator, apply_temperature, ece, fit_temperature, heuristic_scores, rebalanced_indices
from .models.base import AbstractModelFactory, AbstractScorer
from .questions import QUESTIONS, SIGNAL_READ
from .report import write_manifest, write_report, write_results
from .schemas import DataReport, EvalOptions, LabeledTrace, RunResult, TrainOptions
from .sources.base import AbstractTraceSource
from .split import split_heldout, split_traces
from .state import build_state

MIN_TRAIN_TRACES = 200


class FinetunePipeline:
    """Labeled traces in, a fine-tuned checkpoint and its evaluation out.

    Where the traces come from (source) and which model is trained and how
    (factory) are passed in; the pipeline itself only orders the steps:
    check the data, score the released model, train, score the result,
    compare, write the run directory.
    """

    def __init__(self, source: AbstractTraceSource, factory: AbstractModelFactory, run_dir: str | Path,
                 train_options: TrainOptions | None = None, eval_options: EvalOptions | None = None):
        # absolute: the trainer and the servers run from the model's own checkout, not from here
        self.source, self.factory, self.run_dir = source, factory, Path(run_dir).resolve()
        self.train_options = train_options or TrainOptions()
        self.eval_options = eval_options or EvalOptions()

    def check(self) -> DataReport:
        """Load and split the data and report what would stop a run. Trains nothing."""
        report, _ = self._load()
        return report

    def run(self) -> RunResult:
        report, (train, calib, test) = self._load()
        if not report.ok:
            raise ValueError("the data cannot be trained on:\n  " + "\n  ".join(report.problems))
        checkpoint = self.run_dir / "checkpoint"
        if checkpoint.exists():
            raise ValueError(f"{checkpoint} already exists; choose a new run directory")
        data_dir = self.run_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        for name, rows in (("train", train), ("calibration", calib), ("test", test)):
            (data_dir / f"{name}.jsonl").write_text("".join(json.dumps(t.to_dict(), ensure_ascii=False) + "\n" for t in rows))

        trainer = self.factory.create_trainer()
        plan = trainer.prepare(train, data_dir, self.train_options)

        print("scoring the test portion with the released model")
        with self.factory.open_scorer(None) as scorer:
            zero_shot, zero_latency = score(scorer, test)

        training = trainer.train(plan, checkpoint, self.run_dir / "train.log", self.train_options)

        print("scoring the calibration and test portions with the fine-tuned model")
        with self.factory.open_scorer(checkpoint) as scorer:
            ft_calib, _ = score(scorer, calib)
            finetuned, ft_latency = score(scorer, test)

        manifest = self._evaluate(report, train, calib, test, zero_shot, finetuned, ft_calib, plan, training,
                                  {"zero_shot": latency_summary(zero_latency), "finetuned": latency_summary(ft_latency)})
        for warning in manifest["warnings"]:
            print(f"WARNING: {warning}. The checkpoint is written anyway; see report.md.")
        write_results(self.run_dir / "results.csv", manifest["results"])
        write_manifest(self.run_dir / "manifest.json", manifest)
        write_report(self.run_dir / "report.md", manifest)
        print(f"run written to {self.run_dir}")
        return RunResult(str(self.run_dir), str(checkpoint), manifest, manifest["warnings"])

    # ---- steps

    def _load(self) -> tuple[DataReport, tuple[list, list, list]]:
        traces = self.source.load()
        opts = self.eval_options
        train, heldout, method = split_traces(traces, opts.heldout_frac, opts.seed)
        calib, test = split_heldout(heldout, opts.calib_frac, opts.seed)
        report = DataReport(
            source=self.source.describe(),
            traces=len(traces),
            labels={label: sum(t.label == label for t in traces) for label in ("correct", "partial", "wrong")},
            split_method=method,
            splits={name: {"traces": len(rows), "questions": len({t.question_key for t in rows}),
                           "wrong": sum(t.is_wrong for t in rows)}
                    for name, rows in (("train", train), ("calibration", calib), ("test", test))},
        )
        for name, rows in (("train", train), ("calibration", calib), ("test", test)):
            wrong = sum(t.is_wrong for t in rows)
            if wrong == 0 or wrong == len(rows):
                report.problems.append(f"the {name} portion needs both correct and wrong traces ({len(rows)} traces, {wrong} wrong)")
        if not report.problems:
            y_test = np.array([t.is_wrong for t in test])
            for br in opts.base_rates:
                try:
                    rebalanced_indices(y_test, br, opts.seed)
                except ValueError as err:
                    report.problems.append(str(err))
        if len(train) < MIN_TRAIN_TRACES:
            report.notes.append(f"only {len(train)} training traces; expect a noisy result below {MIN_TRAIN_TRACES}")
        if method != "dataset_split_field" and all(t.qid is None for t in traces):
            report.notes.append("no qid or split field: runs of the same question are grouped by their question text")
        return report, (train, calib, test)

    def _evaluate(self, report, train, calib, test, zero_shot, finetuned, ft_calib, plan, training, latency) -> dict:
        opts = self.eval_options
        y_test = np.array([t.is_wrong for t in test])
        y_calib = np.array([t.is_wrong for t in calib])
        evaluator = Evaluator(y_test, opts)

        rows = {}
        for signal in SIGNAL_READ:
            rows[("zero_shot", signal)] = evaluator.rows("zero_shot", signal, zero_shot[signal])
            rows[("finetuned", signal)] = evaluator.rows("finetuned", signal, finetuned[signal])
        for rule, scores in heuristic_scores(train, test, opts.seed).items():
            rows[("heuristic", rule)] = evaluator.rows("heuristic", rule, scores)

        def summary(key):
            return {"auroc": rows[key][0]["auroc_full_test"],
                    "normalized_recall": {str(br): v for br, v in evaluator.headline(rows[key]).items()}}

        signal = opts.decision_signal
        best_rule = max((k for k in rows if k[0] == "heuristic"),
                        key=lambda k: np.mean(list(evaluator.headline(rows[k]).values())))
        metrics = {"zero_shot": summary(("zero_shot", signal)), "finetuned": summary(("finetuned", signal)),
                   "heuristic": {**summary(best_rule), "name": best_rule[1]}, "signal": signal}

        warnings = []
        for other in ("zero_shot", "heuristic"):
            behind = [br for br, v in metrics["finetuned"]["normalized_recall"].items()
                      if v <= metrics[other]["normalized_recall"][br]]
            if behind:
                name = "the released model (zero-shot)" if other == "zero_shot" else f"the {best_rule[1]} heuristic"
                warnings.append(f"fine-tuning did not beat {name} on normalized recall at a {opts.sample_rate:.0%} budget "
                                f"(base rate {', '.join(f'{float(b):.0%}' for b in behind)})")

        temperatures, test_ece = {}, {}
        for s in SIGNAL_READ:
            T = fit_temperature(ft_calib[s], y_calib.astype(float))
            temperatures[s] = round(T, 4)
            test_ece[s] = {"before": ece(finetuned[s], y_test), "after": ece(apply_temperature(finetuned[s], T), y_test)}

        decision_type = QUESTIONS[signal]["type"]
        model = self.factory.describe()
        return {
            "model": self.run_dir.name,
            **model,
            "run_dir": str(self.run_dir),
            "checkpoint": str(self.run_dir / "checkpoint"),
            "source": report.source,
            "seed": opts.seed,
            "questions": QUESTIONS,
            "temperatures": temperatures,
            "decision": {
                "question": signal, "type": decision_type,
                "probability_field": "noul" if decision_type == "noul" else "probabilities",
                "positive_option": "keep" if decision_type == "choice" else None,
                "temperature": temperatures[signal],
            },
            "split": {"method": report.split_method,
                      **{name: {**counts, "base_rate": round(counts["wrong"] / counts["traces"], 4)}
                         for name, counts in report.splits.items()}},
            "token_limits": {k: plan[k] for k in ("max_state", "max_branch", "max_packed", "state_tokens")},
            "training": {**training, "options": vars(self.train_options)},
            "calibration": {"fitted_on": "calibration", "temperatures": temperatures, "test_ece": test_ece},
            "evaluation": {"sample_rate": opts.sample_rate, "sample_rates": list(opts.sample_rates),
                           "base_rates": list(opts.base_rates), "n_resamples": opts.n_resamples},
            "metrics": metrics,
            "latency_ms": latency,
            "warnings": warnings,
            "results": [r for group in rows.values() for r in group],
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }


def score(scorer: AbstractScorer, traces: list[LabeledTrace]):
    """Both signals for every trace, and each request's latency in ms."""
    signals, latencies = {s: [] for s in SIGNAL_READ}, []
    for i, t in enumerate(traces, 1):
        started = time.perf_counter()
        answers = scorer.answer(build_state(t.to_dict()), QUESTIONS)
        latencies.append((time.perf_counter() - started) * 1000)
        for s, read in SIGNAL_READ.items():
            signals[s].append(float(read(answers[s])))
        if i % 100 == 0 or i == len(traces):
            print(f"  {i}/{len(traces)}", flush=True)
    return {s: np.array(v) for s, v in signals.items()}, np.array(latencies)


def latency_summary(latencies) -> dict:
    return {"p50": float(np.percentile(latencies, 50)), "p95": float(np.percentile(latencies, 95))}
