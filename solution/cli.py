"""Command-line interface: ``python -m solution {ingest,report,gen-keys,init-db}``.

Exit codes: 0 success, 1 runtime/ingest failure, 2 configuration or usage error.
Terminal output and logs contain counts and file names only - never row values.
"""
from __future__ import annotations

import argparse
import base64
import logging
import os
import sqlite3
import sys
from pathlib import Path

from solution import __version__
from solution.config import (ConfigError, load_dotenv, load_keys, resolve_db_path,
                             resolve_schemas_dir)
from solution.db import connect, init_schema, verify_or_register_keys
from solution.ingest import Ingestor, sync_reference_data
from solution.reporting.writers import write_all
from solution.security import Crypto, DecryptionError
from solution.workflows import load_dispositions, load_schemas

log = logging.getLogger("solution")


def _open(args) -> tuple[sqlite3.Connection, Crypto]:
    crypto = Crypto(load_keys())
    db_path = resolve_db_path(args.db)
    conn = connect(db_path)
    try:
        init_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        verify_or_register_keys(conn, crypto)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        conn.close()
        raise
    return conn, crypto


def cmd_gen_keys(_args) -> int:
    enc = base64.urlsafe_b64encode(os.urandom(32)).decode()
    mac = base64.urlsafe_b64encode(os.urandom(32)).decode()
    print(f"LEDGER_ENCRYPTION_KEY={enc}\nLEDGER_HMAC_KEY={mac}")
    return 0


def cmd_init_db(args) -> int:
    conn, _ = _open(args)
    conn.close()
    print(f"Database ready: {resolve_db_path(args.db)}")
    return 0


def cmd_ingest(args) -> int:
    schemas_dir = resolve_schemas_dir(args.schemas)
    schemas = load_schemas(schemas_dir)
    dispositions = load_dispositions(schemas_dir)
    conn, crypto = _open(args)
    try:
        sync_reference_data(conn, schemas, dispositions)
        s = Ingestor(conn, crypto, schemas).run(Path(args.path))
    finally:
        conn.close()
    print(f"files ingested: {s.files_ingested}   skipped (already ingested): {s.files_skipped}")
    print(f"lead rows: {s.lead_rows}   rejected: {s.lead_rows_rejected}   "
          f"duplicates merged: {s.duplicates_merged}")
    print(f"event lines: {s.events_read}   new events: {s.events_inserted}   "
          f"malformed: {s.events_malformed}   duplicate: {s.events_duplicate}")
    if s.orphans_before is not None:
        print(f"orphan events: {s.orphans_before} -> {s.orphans_after} "
              f"(resolved by newly ingested leads/events: "
              f"{max(0, s.orphans_before - s.orphans_after)} net)")
    return 0


def cmd_report(args) -> int:
    if not resolve_db_path(args.db).is_file():
        raise ConfigError(f"Database not found: {resolve_db_path(args.db)} - run `ingest` first "
                          f"(or pass --db).")
    conn, crypto = _open(args)
    out = Path(args.out)
    try:
        dq = write_all(conn, crypto, out)
    finally:
        conn.close()
    print(f"Reports written to {out.resolve()}: batch_summary.csv, daily_overview.csv, "
          f"lead_report.csv, rejections.csv, data_quality.json")
    print(f"orphan events: {dq['orphan_events']}   rejected lead rows: {dq['lead_rows_rejected']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m solution",
                                description="Interaction ledger & reporting pipeline.")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("-v", "--verbose", action="store_true", help="log each file processed")
    p.add_argument("--db", help="SQLite path (default: $LEDGER_DB or ./ledger.db)")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("ingest", help="ingest a drop (or a folder of drops, in name order)")
    s.add_argument("path", help="drop folder, e.g. data/drop_1, or parent of several drops")
    s.add_argument("--schemas", help="schemas dir (default: assignment-a-data/schemas)")
    s.set_defaults(func=cmd_ingest)

    s = sub.add_parser("report", help="write the five report files from everything ingested")
    s.add_argument("--out", required=True, help="output directory (created if missing)")
    s.set_defaults(func=cmd_report)

    s = sub.add_parser("init-db", help="create the database and register the keys")
    s.set_defaults(func=cmd_init_db)

    s = sub.add_parser("gen-keys", help="print a fresh encryption key and HMAC key")
    s.set_defaults(func=cmd_gen_keys)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    load_dotenv()
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except DecryptionError:
        print("error: stored data failed authentication (wrong key or tampered database).",
              file=sys.stderr)
        return 1
    except (OSError, sqlite3.Error, ValueError) as exc:
        # message only; never echo row data
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
