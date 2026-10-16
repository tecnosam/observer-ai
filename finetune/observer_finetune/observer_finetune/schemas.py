"""The records the SDK passes between its parts."""

from dataclasses import asdict, dataclass, field
from typing import Any

LABELS = ("correct", "partial", "wrong")


@dataclass
class LabeledTrace:
    """One agent run and whether its final answer was right.

    The fields up to final_answer are the engine's trace record (POST
    /v1/traces), so a trace means the same thing here as at serving time.
    """

    trace_id: str
    question: str
    tool_calls: list[dict[str, Any]]
    evidence: list[str]
    final_answer: str
    label: str  # one of LABELS

    # Optional. qid groups runs of the same question so they land on the same
    # side of the split; split pins a trace to "train" or "heldout".
    qid: str | None = None
    split: str | None = None
    used_search: bool | None = None
    stop_reason: str | None = None
    failure_type: str | None = None

    @property
    def is_wrong(self) -> bool:
        # Partial answers count as wrong, as in every metric here.
        return self.label != "correct"

    @property
    def question_key(self) -> str:
        return self.qid if self.qid is not None else " ".join(self.question.lower().split())

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class TrainOptions:
    epochs: int = 1
    lr: float = 2e-5
    batch: int = 2  # records per forward pass
    accum: int = 4  # forward passes per optimizer step
    device: str | None = None  # None: mps on Apple Silicon, else cuda
    dtype: str = "fp32"  # bf16 is CUDA only
    max_state_tokens: int | None = None  # None: p99 slice length, rounded up to a multiple of 128
    max_drop_frac: float = 0.02  # stop if more training records than this would not fit
    max_steps: int = 0  # 0 trains every epoch; a small number makes a quick smoke run
    seed: int = 7


@dataclass
class EvalOptions:
    heldout_frac: float = 0.2  # only used when the data has no split field
    calib_frac: float = 0.5  # share of held-out questions used to fit temperatures
    sample_rate: float = 0.05  # the headline review budget
    sample_rates: tuple[float, ...] = (0.05, 0.10, 0.20)
    base_rates: tuple[float, ...] = (0.10, 0.20)  # share of wrong traces in the rebalanced test sets
    n_resamples: int = 20
    decision_signal: str = "keep_or_drop"
    seed: int = 7


@dataclass
class DataReport:
    """What `check` found in the data, before any training."""

    source: str
    traces: int
    labels: dict[str, int]
    split_method: str
    splits: dict[str, dict[str, int]]
    problems: list[str] = field(default_factory=list)  # stop a run
    notes: list[str] = field(default_factory=list)  # worth knowing, do not stop a run

    @property
    def ok(self) -> bool:
        return not self.problems


@dataclass
class RunResult:
    run_dir: str
    checkpoint: str
    manifest: dict[str, Any]
    warnings: list[str]
