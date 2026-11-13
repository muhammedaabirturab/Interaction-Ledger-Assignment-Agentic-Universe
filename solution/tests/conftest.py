"""Shared fixtures: deterministic test keys, generated datasets, pipeline runner."""
from __future__ import annotations

import base64
import subprocess
import sys
from pathlib import Path

import pytest

from solution.config import Keys
from solution.db import connect, init_schema, verify_or_register_keys
from solution.ingest import Ingestor, sync_reference_data
from solution.reporting.writers import write_all
from solution.security import Crypto
from solution.workflows import load_dispositions, load_schemas

REPO = Path(__file__).resolve().parents[2]
SCHEMAS = REPO / "assignment-a-data" / "schemas"
GENERATOR = REPO / "assignment-a-data" / "tools" / "generate_data.py"

TEST_KEYS = Keys(encryption=bytes(range(32)), hmac=bytes(range(100, 164)))
OTHER_KEYS = Keys(encryption=bytes(range(1, 33)), hmac=bytes(range(101, 165)))


def b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode()


@pytest.fixture(scope="session")
def gen_data(tmp_path_factory):
    """seed -> generated small dataset directory (cached for the session)."""
    cache: dict[int, Path] = {}

    def make(seed: int) -> Path:
        if seed not in cache:
            out = tmp_path_factory.mktemp(f"data{seed}")
            subprocess.run([sys.executable, str(GENERATOR), "--seed", str(seed), "--scale", "small",
                            "--out", str(out)], check=True, capture_output=True)
            cache[seed] = out
        return cache[seed]
    return make


class Pipeline:
    """Thin harness around the library API (same code path as the CLI)."""

    def __init__(self, db_path: Path, keys: Keys = TEST_KEYS):
        self.db_path = db_path
        self.crypto = Crypto(keys)
        self.schemas = load_schemas(SCHEMAS)
        self.conn = connect(db_path)
        init_schema(self.conn)
        self.conn.execute("BEGIN IMMEDIATE")
        verify_or_register_keys(self.conn, self.crypto)
        self.conn.execute("COMMIT")
        sync_reference_data(self.conn, self.schemas, load_dispositions(SCHEMAS))

    def ingest(self, path: Path):
        return Ingestor(self.conn, self.crypto, self.schemas).run(Path(path))

    def report(self, out: Path) -> dict[str, str]:
        write_all(self.conn, self.crypto, out)
        return {p.name: p.read_text(encoding="utf-8") for p in sorted(out.iterdir())}

    def close(self):
        self.conn.close()


@pytest.fixture
def pipeline(tmp_path):
    p = Pipeline(tmp_path / "ledger.db")
    yield p
    p.close()


@pytest.fixture
def make_pipeline(tmp_path):
    created = []

    def make(name="ledger.db", keys=TEST_KEYS):
        p = Pipeline(tmp_path / name, keys)
        created.append(p)
        return p
    yield make
    for p in created:
        try:
            p.close()
        except Exception:
            pass


def write_drop(root: Path, name: str, leads: dict | None = None, events: list[str] | None = None,
               events_name: str = "events_0001.jsonl") -> Path:
    """Create a tiny drop. ``leads``: {(workflow, 'batch_001'): csv_text}; events: raw lines."""
    d = root / name
    for (wf, batch), text in (leads or {}).items():
        f = d / "leads" / wf / f"{batch}.csv"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text, encoding="utf-8", newline="")
    if events is not None:
        f = d / "events" / events_name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("\n".join(events) + "\n", encoding="utf-8")
    return d
