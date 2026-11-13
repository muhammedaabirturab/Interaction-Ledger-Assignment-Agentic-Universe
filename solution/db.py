"""SQLite connection handling and DDL.

Design notes (see README "Database design"):
* ``events`` is the append-only ledger; triggers forbid UPDATE/DELETE.
* Event->lead linkage is *derived* (join on workflow + phone HMAC, or via
  call_id), never stored, so late-arriving leads reconcile orphans with no
  mutation of the ledger.
* Every input file is ingested in ONE transaction together with its
  ``source_files`` marker, so a crash leaves no half-applied file.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from solution.config import ConfigError
from solution.security import Crypto, DecryptionError

SCHEMA_VERSION = "1"

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS workflows (
    workflow_id   TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    schema_json   TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS dispositions (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
    code        TEXT NOT NULL,
    label       TEXT NOT NULL,
    is_terminal INTEGER NOT NULL CHECK (is_terminal IN (0,1)),
    is_success  INTEGER NOT NULL CHECK (is_success  IN (0,1)),
    PRIMARY KEY (workflow_id, code)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS ingest_runs (
    id            INTEGER PRIMARY KEY,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    input_path    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running','ok','failed')),
    orphans_before INTEGER,
    orphans_after  INTEGER
);

-- One row per ingested file; the UNIQUE key is what makes re-ingest a no-op.
CREATE TABLE IF NOT EXISTS source_files (
    id            INTEGER PRIMARY KEY,
    run_id        INTEGER NOT NULL REFERENCES ingest_runs(id),
    kind          TEXT NOT NULL CHECK (kind IN ('leads','events')),
    workflow_id   TEXT NOT NULL DEFAULT '',
    drop_name     TEXT NOT NULL,
    rel_path      TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    ingested_at   TEXT NOT NULL,
    rows_total    INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    duplicates_merged INTEGER NOT NULL DEFAULT 0,
    field_warnings INTEGER NOT NULL DEFAULT 0,
    events_total  INTEGER NOT NULL DEFAULT 0,
    events_malformed INTEGER NOT NULL DEFAULT 0,
    events_duplicate INTEGER NOT NULL DEFAULT 0,
    UNIQUE (kind, workflow_id, sha256)
);

CREATE TABLE IF NOT EXISTS batches (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
    batch_id    TEXT NOT NULL,
    first_source_file_id INTEGER NOT NULL REFERENCES source_files(id),
    PRIMARY KEY (workflow_id, batch_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS leads (
    id           INTEGER PRIMARY KEY,
    workflow_id  TEXT NOT NULL,
    phone_hash   BLOB NOT NULL,
    external_ref TEXT NOT NULL,
    batch_id     TEXT NOT NULL,
    UNIQUE (workflow_id, phone_hash),
    FOREIGN KEY (workflow_id, batch_id) REFERENCES batches(workflow_id, batch_id)
);
CREATE INDEX IF NOT EXISTS ix_leads_batch ON leads(workflow_id, batch_id);

-- Non-null field values of a lead. PII fields live only in value_enc.
CREATE TABLE IF NOT EXISTS lead_fields (
    lead_id     INTEGER NOT NULL REFERENCES leads(id),
    field_name  TEXT NOT NULL,
    value_plain TEXT,
    value_enc   BLOB,
    PRIMARY KEY (lead_id, field_name),
    CHECK ((value_plain IS NULL) <> (value_enc IS NULL))
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lead_rejections (
    id             INTEGER PRIMARY KEY,
    source_file_id INTEGER NOT NULL REFERENCES source_files(id),
    workflow_id    TEXT NOT NULL,
    batch_id       TEXT NOT NULL,
    row_number     INTEGER NOT NULL,
    reason         TEXT NOT NULL,
    UNIQUE (source_file_id, row_number)
);

-- Append-only interaction ledger. No customer PII: the phone is only an HMAC.
CREATE TABLE IF NOT EXISTS events (
    id               INTEGER PRIMARY KEY,
    event_id         TEXT NOT NULL UNIQUE,
    event_type       TEXT NOT NULL,
    workflow_id      TEXT,
    call_id          TEXT,
    occurred_us      INTEGER NOT NULL,
    phone_state      TEXT NOT NULL CHECK (phone_state IN ('none','invalid','valid')),
    phone_hash       BLOB,
    disposition_code TEXT,
    payload_json     TEXT NOT NULL,
    source_file_id   INTEGER NOT NULL REFERENCES source_files(id),
    line_no          INTEGER NOT NULL,
    CHECK ((phone_state = 'valid') = (phone_hash IS NOT NULL))
);
-- Deliberately NO secondary indexes on events: reports are whole-ledger aggregations that
-- scan events once and probe leads' UNIQUE(workflow_id, phone_hash) index, so extra indexes
-- would only slow the 600k-row append path. (UNIQUE(event_id) is the dedupe key.)

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
"""


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)  # manual transactions
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -131072")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    elif row[0] != SCHEMA_VERSION:
        raise ConfigError(f"Database schema version {row[0]!r} != expected {SCHEMA_VERSION!r}.")


def verify_or_register_keys(conn: sqlite3.Connection, crypto: Crypto) -> None:
    """Refuse to run with keys that differ from the ones the DB was built with."""
    row = conn.execute("SELECT value FROM meta WHERE key='hmac_check'").fetchone()
    if row is None:
        mac, enc = crypto.key_check_values()
        conn.execute("INSERT INTO meta VALUES ('hmac_check', ?)", (mac,))
        conn.execute("INSERT INTO meta VALUES ('enc_check', ?)", (enc,))
        return
    mac, _ = crypto.key_check_values()
    enc_row = conn.execute("SELECT value FROM meta WHERE key='enc_check'").fetchone()
    try:
        ok = mac == bytes(row[0]) and crypto.decrypt(bytes(enc_row[0]), "meta:enc_check") == "key-check"
    except DecryptionError:
        ok = False
    if not ok:
        raise ConfigError("The configured keys do not match the keys this database was created "
                          "with. Refusing to continue (would corrupt phone matching).")
