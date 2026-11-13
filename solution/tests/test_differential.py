"""Differential tests: the pipeline's reports must equal an independent oracle's,
on several unseen seeds, whether ingested drop-by-drop or all at once."""
from __future__ import annotations

import pytest

from solution.tests import oracle
from solution.tests.conftest import SCHEMAS

SEEDS = [42, 7, 2026, 31337]


@pytest.mark.parametrize("seed", SEEDS)
def test_incremental_matches_oracle(seed, gen_data, pipeline, tmp_path):
    data = gen_data(seed)
    pipeline.ingest(data / "drop_1")
    pipeline.ingest(data / "drop_2")
    got = pipeline.report(tmp_path / "out")
    want = oracle.run([data / "drop_1", data / "drop_2"], SCHEMAS)
    assert set(got) == set(want) == {"batch_summary.csv", "daily_overview.csv", "lead_report.csv",
                                     "rejections.csv", "data_quality.json"}
    for name in want:
        assert got[name] == want[name], f"{name} differs from oracle (seed {seed})"


@pytest.mark.parametrize("seed", [42, 7])
def test_full_ingest_equals_incremental(seed, gen_data, make_pipeline, tmp_path):
    data = gen_data(seed)
    inc = make_pipeline("inc.db")
    inc.ingest(data / "drop_1")
    inc.ingest(data / "drop_2")
    full = make_pipeline("full.db")
    full.ingest(data)                       # parent folder: drops processed in name order
    assert inc.report(tmp_path / "a") == full.report(tmp_path / "b")
