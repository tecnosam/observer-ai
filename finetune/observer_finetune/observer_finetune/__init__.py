"""Fine-tune Kev on labeled agent traces.

    from observer_finetune import finetune
    run = finetune("labeled_traces.jsonl", "runs/my-run")

The run directory holds the checkpoint (checkpoint/), the manifest the
Observer engine reads (manifest.json), a summary (report.md), every metric
(results.csv), the training log and the data splits.
"""

from pathlib import Path

from .models.base import AbstractModelFactory
from .models.kev.factory import KevModelFactory
from .pipeline import FinetunePipeline
from .schemas import DataReport, EvalOptions, LabeledTrace, RunResult, TrainOptions
from .sources.base import AbstractTraceSource
from .sources.factory import DEFAULT_LABEL_NAME, build_source

__all__ = [
    "finetune", "check", "FinetunePipeline", "AbstractTraceSource", "AbstractModelFactory", "KevModelFactory",
    "build_source", "LabeledTrace", "TrainOptions", "EvalOptions", "DataReport", "RunResult",
]


def finetune(data: str, out: str | Path, *, kev_url: str | None = None, kev_path: str | None = None,
             annotations: str | None = None, label_name: str = DEFAULT_LABEL_NAME,
             train_options: TrainOptions | None = None, eval_options: EvalOptions | None = None) -> RunResult:
    """Fine-tune Kev on `data` and write the run to `out`.

    data:     a JSONL file of labeled traces, a file of OTLP spans (with
              `annotations`), or phoenix://host:port/project
    kev_url:  a server already running the released Kev, for the zero-shot
              baseline. Without it the SDK starts one.
    kev_path: a Kev checkout to use. Without it the SDK clones a pinned one.
    """
    return _pipeline(data, out, kev_url, kev_path, annotations, label_name, train_options, eval_options).run()


def check(data: str, *, annotations: str | None = None, label_name: str = DEFAULT_LABEL_NAME,
          eval_options: EvalOptions | None = None) -> DataReport:
    """Load and split the data and report what would stop a run, without training."""
    return _pipeline(data, ".", None, None, annotations, label_name, None, eval_options).check()


def _pipeline(data, out, kev_url, kev_path, annotations, label_name, train_options, eval_options):
    source = build_source(data, annotations, label_name)
    factory = KevModelFactory(kev_path=kev_path, kev_url=kev_url, log_dir=out)
    return FinetunePipeline(source, factory, out, train_options, eval_options)
