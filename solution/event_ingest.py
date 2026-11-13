"""Event JSONL ingestion into the append-only ledger.

Per-line pipeline (spec order): malformed (bad JSON / no event_id) -> duplicate -> (orphan, derived later)
-> (unknown disposition, derived later).  Orphan and unknown-disposition status is
*not* decided here because it depends on which leads exist; the ledger keeps every
valid, non-duplicate event and reports derive the rest from current state.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from solution.normalise import normalise_phone, parse_timestamp_us
from solution.security import Crypto

DISPOSITION_TYPES = ("disposition.set", "disposition.override")
CHUNK = 500


@dataclass
class EventStats:
    events_total: int = 0
    malformed: int = 0
    duplicates: int = 0
    inserted: int = 0


def _clean_str(v: object) -> str | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, str)):
        s = str(v).strip()
        return s or None
    return None


def parse_event_line(raw: bytes, crypto: Crypto, line_no: int, source_file_id: int) -> tuple | None:
    """Return an INSERT row for ``events`` or ``None`` if the line is malformed.

    Exactly the spec's definition: the line is not valid JSON (or not a JSON object, which
    cannot carry an ``event_id``), or it has no ``event_id``.  Everything else is a real event
    and goes through dedupe / orphan / unknown-disposition handling:
    * a missing ``event_type`` is stored as '' (inert: it matches no known type);
    * an unparseable ``occurred_at`` is stored as NULL - the event still counts for dedupe,
      linking and orphan statistics but cannot be ordered, so it never creates or dates a call
      and never decides an effective disposition.
    """
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    event_id = _clean_str(obj.get("event_id"))
    if event_id is None:
        return None
    event_type = _clean_str(obj.get("event_type")) or ""
    occurred_us = parse_timestamp_us(obj.get("occurred_at"))

    payload = obj.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    raw_phone = obj.get("phone")
    phone_hash = None
    if raw_phone is None or (isinstance(raw_phone, str) and not raw_phone.strip()):
        phone_state = "none"
    else:
        canonical = normalise_phone(raw_phone)
        if canonical is None:
            phone_state = "invalid"
        else:
            phone_state, phone_hash = "valid", crypto.phone_hash(canonical)
    code = _clean_str(payload.get("disposition_code")) if event_type in DISPOSITION_TYPES else None
    return (event_id, event_type, _clean_str(obj.get("workflow_id")), _clean_str(obj.get("call_id")),
            occurred_us, phone_state, phone_hash, code,
            json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
            source_file_id, line_no)


def iter_lines(path: Path) -> Iterator[tuple[int, bytes]]:
    """Yield (physical line number, line bytes) for every non-empty line."""
    with path.open("rb") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if line:
                yield n, line


def _existing_ids(conn: sqlite3.Connection, ids: list[str]) -> set[str]:
    found: set[str] = set()
    for i in range(0, len(ids), CHUNK):
        part = ids[i:i + CHUNK]
        q = ",".join("?" * len(part))
        found.update(r[0] for r in conn.execute(
            f"SELECT event_id FROM events WHERE event_id IN ({q})", part))
    return found


def ingest_event_file(conn: sqlite3.Connection, crypto: Crypto, path: Path,
                      source_file_id: int) -> EventStats:
    stats = EventStats()
    candidates: list[tuple] = []
    for line_no, raw in iter_lines(path):
        stats.events_total += 1
        row = parse_event_line(raw, crypto, line_no, source_file_id)
        if row is None:
            stats.malformed += 1
        else:
            candidates.append(row)

    seen = _existing_ids(conn, [r[0] for r in candidates])
    to_insert = []
    for row in candidates:                 # file order => first occurrence wins
        if row[0] in seen:
            stats.duplicates += 1
        else:
            seen.add(row[0])
            to_insert.append(row)
    conn.executemany(
        "INSERT INTO events(event_id, event_type, workflow_id, call_id, occurred_us, phone_state,"
        " phone_hash, disposition_code, payload_json, source_file_id, line_no)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)", to_insert)
    stats.inserted = len(to_insert)
    return stats
