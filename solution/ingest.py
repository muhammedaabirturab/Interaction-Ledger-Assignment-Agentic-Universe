"""Ingest orchestration: drop discovery, idempotency, per-file transactions.

Idempotency key: (kind, workflow_id, sha256 of file bytes).  A file whose bytes were
already ingested is skipped, so re-running ``ingest`` changes nothing.  Each file is
applied together with its ``source_files`` row in a single transaction: after a crash
the file is either fully applied or not at all, and a re-run simply retries it.
"""
from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from solution.config import ConfigError
from solution.event_ingest import ingest_event_file
from solution.lead_ingest import parse_lead_csv, persist_leads
from solution.reporting import derive
from solution.security import Crypto
from solution.workflows import Disposition, WorkflowSchema

log = logging.getLogger("solution.ingest")


@dataclass
class DropPlan:
    name: str
    lead_files: list[tuple[str, Path]] = field(default_factory=list)   # (workflow_id, csv)
    event_files: list[Path] = field(default_factory=list)


@dataclass
class IngestSummary:
    files_ingested: int = 0
    files_skipped: int = 0
    lead_rows: int = 0
    lead_rows_rejected: int = 0
    duplicates_merged: int = 0
    events_read: int = 0
    events_inserted: int = 0
    events_malformed: int = 0
    events_duplicate: int = 0
    orphans_before: int | None = None
    orphans_after: int | None = None


def _natural_key(p: Path):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p.name)]


def discover(root: Path, schemas: dict[str, WorkflowSchema]) -> list[DropPlan]:
    """Find drops under ``root``. ``root`` is one drop (has leads/ or events/) or a
    directory of drops (processed in natural name order)."""
    if not root.is_dir():
        raise ConfigError(f"Input path is not a directory: {root}")

    def is_drop(d: Path) -> bool:
        return (d / "leads").is_dir() or (d / "events").is_dir()

    drop_dirs = [root] if is_drop(root) else sorted(
        (d for d in root.iterdir() if d.is_dir() and is_drop(d)), key=_natural_key)
    if not drop_dirs:
        raise ConfigError(f"No drops found in {root} (expected leads/ and/or events/ folders).")

    plans = []
    for d in drop_dirs:
        plan = DropPlan(name=d.name)
        leads_dir = d / "leads"
        if leads_dir.is_dir():
            for wf_dir in sorted(p for p in leads_dir.iterdir() if p.is_dir()):
                if wf_dir.name not in schemas:
                    raise ConfigError(f"{d.name}/leads/{wf_dir.name}: no schema for workflow "
                                      f"'{wf_dir.name}' in the schemas directory.")
                plan.lead_files += [(wf_dir.name, f) for f in sorted(wf_dir.glob("*.csv"))]
        events_dir = d / "events"
        if events_dir.is_dir():
            plan.event_files = sorted(events_dir.glob("*.jsonl"))
        plans.append(plan)
    return plans


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sync_reference_data(conn: sqlite3.Connection, schemas: dict[str, WorkflowSchema],
                        dispositions: list[Disposition]) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        for s in schemas.values():
            conn.execute("INSERT INTO workflows VALUES (?,?,?) ON CONFLICT(workflow_id) DO UPDATE SET "
                         "description=excluded.description, schema_json=excluded.schema_json",
                         (s.workflow_id, s.description, s.raw_json))
        conn.execute("DELETE FROM dispositions")
        conn.executemany("INSERT INTO dispositions VALUES (?,?,?,?,?)",
                         [(d.workflow_id, d.code, d.label, int(d.is_terminal), int(d.is_success))
                          for d in dispositions if d.workflow_id in schemas])
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


class Ingestor:
    def __init__(self, conn: sqlite3.Connection, crypto: Crypto,
                 schemas: dict[str, WorkflowSchema]):
        self.conn, self.crypto, self.schemas = conn, crypto, schemas

    # -- one file == one transaction -------------------------------------
    def _ingest_file(self, run_id: int, kind: str, workflow_id: str, drop: str, path: Path,
                     summary: IngestSummary) -> None:
        digest = sha256_of(path)
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if conn.execute("SELECT 1 FROM source_files WHERE kind=? AND workflow_id=? AND sha256=?",
                            (kind, workflow_id, digest)).fetchone():
                conn.execute("ROLLBACK")
                summary.files_skipped += 1
                log.info("skip (already ingested): %s/%s", drop, path.name)
                return
            sf_id = conn.execute(
                "INSERT INTO source_files(run_id, kind, workflow_id, drop_name, rel_path, sha256,"
                " ingested_at) VALUES (?,?,?,?,?,?,?)",
                (run_id, kind, workflow_id, drop, f"{path.parent.name}/{path.name}", digest, _now())
            ).lastrowid
            if kind == "leads":
                schema = self.schemas[workflow_id]
                try:
                    text = path.read_text(encoding="utf-8-sig")
                except UnicodeDecodeError as exc:
                    raise ValueError(f"{path.name}: not valid UTF-8 ({exc.reason})") from None
                parsed = list(parse_lead_csv(text, schema))
                st = persist_leads(conn, self.crypto, schema, path.stem, sf_id, parsed)
                conn.execute("UPDATE source_files SET rows_total=?, rows_rejected=?, "
                             "duplicates_merged=?, field_warnings=? WHERE id=?",
                             (st.rows_total, st.rows_rejected, st.duplicates_merged,
                              st.field_warnings, sf_id))
                summary.lead_rows += st.rows_total
                summary.lead_rows_rejected += st.rows_rejected
                summary.duplicates_merged += st.duplicates_merged
            else:
                st = ingest_event_file(conn, self.crypto, path, sf_id)
                conn.execute("UPDATE source_files SET events_total=?, events_malformed=?, "
                             "events_duplicate=? WHERE id=?",
                             (st.events_total, st.malformed, st.duplicates, sf_id))
                summary.events_read += st.events_total
                summary.events_inserted += st.inserted
                summary.events_malformed += st.malformed
                summary.events_duplicate += st.duplicates
            conn.execute("COMMIT")
            summary.files_ingested += 1
            log.info("ingested %s: %s/%s", kind, drop, path.name)
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def _last_orphans(self) -> int:
        """Orphan count recorded by the previous run, if it finished cleanly (so state is
        unchanged since); otherwise recompute."""
        row = self.conn.execute("SELECT status, orphans_after FROM ingest_runs "
                                "ORDER BY id DESC LIMIT 1").fetchone()
        if row and row[0] == "ok" and row[1] is not None:
            return row[1]
        return derive.orphan_count(self.conn)

    def run(self, root: Path) -> IngestSummary:
        plans = discover(root, self.schemas)
        conn = self.conn
        summary = IngestSummary()
        run_id = conn.execute("INSERT INTO ingest_runs(started_at, input_path) VALUES (?,?)",
                              (_now(), root.name)).lastrowid
        try:
            summary.orphans_before = self._last_orphans()
            for plan in plans:
                for wf, path in plan.lead_files:          # leads first, then events (per drop)
                    self._ingest_file(run_id, "leads", wf, plan.name, path, summary)
                for path in plan.event_files:
                    self._ingest_file(run_id, "events", "", plan.name, path, summary)
            # Linkage only changes when something new was ingested; otherwise reuse the last value.
            summary.orphans_after = (derive.orphan_count(conn)
                                     if summary.files_ingested else summary.orphans_before)
            if summary.files_ingested == 0:
                # Re-ingest of already-seen data must change nothing: leave no audit row either.
                conn.execute("DELETE FROM ingest_runs WHERE id=?", (run_id,))
            else:
                conn.execute("UPDATE ingest_runs SET status='ok', finished_at=?, orphans_before=?, "
                             "orphans_after=? WHERE id=?",
                             (_now(), summary.orphans_before, summary.orphans_after, run_id))
        except BaseException:
            conn.execute("UPDATE ingest_runs SET status='failed', finished_at=? WHERE id=?",
                         (_now(), run_id))
            raise
        return summary
