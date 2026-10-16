from abc import ABC, abstractmethod

from ..schemas import LabeledTrace


class AbstractTraceSource(ABC):
    """Where labeled traces come from: a file, or a trace store."""

    @abstractmethod
    def load(self) -> list[LabeledTrace]:
        """Every usable trace. Raises ValueError, naming what is wrong, when none are."""

    @abstractmethod
    def describe(self) -> str:
        """A short name for reports, e.g. the file path or the Phoenix project."""
