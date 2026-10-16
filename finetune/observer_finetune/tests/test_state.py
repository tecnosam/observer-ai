"""build_state must match the engine's BuildState byte for byte; the engine's golden files are the reference."""

import json
from pathlib import Path

import pytest

from observer_finetune.state import build_state

ENGINE_TESTDATA = Path(__file__).resolve().parents[3] / "inference-v1" / "engine" / "testdata"


@pytest.mark.skipif(not (ENGINE_TESTDATA / "golden").exists(), reason="engine testdata not found")
def test_matches_engine_golden_files():
    traces = sorted((ENGINE_TESTDATA / "traces").glob("*.json"))
    assert traces
    for path in traces:
        want = (ENGINE_TESTDATA / "golden" / f"{path.stem}.txt").read_bytes()
        assert build_state(json.loads(path.read_text(encoding="utf-8"))).encode("utf-8") == want, path.name
