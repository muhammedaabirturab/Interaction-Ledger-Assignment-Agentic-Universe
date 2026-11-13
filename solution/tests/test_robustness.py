"""Idempotency, restart/crash safety, append-only ledger, adversarial inputs."""
from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from solution import ingest as ingest_mod
from solution.config import ConfigError
from solution.tests.conftest import OTHER_KEYS, Pipeline, write_drop
from solution.tests.test_ledger_rules import HDR, PH, call_events, ev, lead_csv


def db_fingerprint(path) -> str:
    """Hash of every row of the data tables (not ingest_runs, which logs invocations)."""
    c = sqlite3.connect(str(path))
    h = hashlib.sha256()
    for t in ("leads", "lead_fields", "lead_rejections", "events", "batches", "source_files"):
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})") if r[1] not in ("run_id", "ingested_at")]
        for row in c.execute(f"SELECT {','.join(cols)} FROM {t} ORDER BY 1,2"):
            h.update(repr(row).encode())
    c.close()
    return h.hexdigest()


# --------------------------------------------------------------- idempotency
def test_reingesting_same_drops_changes_nothing(gen_data, pipeline, tmp_path):
    data = gen_data(42)
    for d in ("drop_1", "drop_2"):
        pipeline.ingest(data / d)
    before = db_fingerprint(pipeline.db_path)
    rep_before = pipeline.report(tmp_path / "a")
    for d in ("drop_1", "drop_2", "drop_1"):
        s = pipeline.ingest(data / d)
        assert s.files_ingested == 0 and s.files_skipped > 0 and s.events_inserted == 0
    assert db_fingerprint(pipeline.db_path) == before
    assert pipeline.report(tmp_path / "b") == rep_before


def test_reports_regenerate_identically(gen_data, pipeline, tmp_path):
    pipeline.ingest(gen_data(7))
    assert pipeline.report(tmp_path / "a") == pipeline.report(tmp_path / "b")


def test_same_bytes_under_different_workflow_is_not_skipped(pipeline, tmp_path):
    fb = "customer_name,phone,order_id\r\nAsha,9876543210,O1\r\n"
    d = write_drop(tmp_path, "drop_1", {("policy_feedback", "batch_001"): fb})
    pipeline.ingest(d)
    # identical bytes to a different workflow dir => different idempotency key
    assert pipeline.ingest(write_drop(tmp_path, "drop_2",
                                      {("motor_renewal", "batch_001"): fb.replace("order_id", "policy_number")}
                                      )).files_ingested == 1


def test_restart_new_process_same_db(gen_data, make_pipeline, tmp_path):
    data = gen_data(42)
    a = make_pipeline("a.db")
    a.ingest(data / "drop_1")
    a.close()
    a2 = Pipeline(tmp_path / "a.db")                # "restart": fresh connection/process state
    try:
        a2.ingest(data / "drop_2")
        got = a2.report(tmp_path / "x")
    finally:
        a2.close()
    b = make_pipeline("b.db")
    b.ingest(data)
    assert got == b.report(tmp_path / "y")


# ---------------------------------------------------------------- crash safety
def test_crash_mid_file_rolls_back_whole_file_and_rerun_recovers(gen_data, make_pipeline, tmp_path, monkeypatch):
    data = gen_data(42)
    p = make_pipeline("crash.db")
    p.ingest(data / "drop_1")
    snapshot = db_fingerprint(p.db_path)

    real = ingest_mod.ingest_event_file
    calls = {"n": 0}

    def exploding(conn, crypto, path, sf_id):
        calls["n"] += 1
        stats = real(conn, crypto, path, sf_id)     # rows already inserted in the open txn...
        raise RuntimeError("simulated crash after inserting events")
    monkeypatch.setattr(ingest_mod, "ingest_event_file", exploding)
    with pytest.raises(RuntimeError):
        p.ingest(data / "drop_2")
    monkeypatch.setattr(ingest_mod, "ingest_event_file", real)

    # the lead files of drop_2 (earlier, fully committed) stay; the crashed event file left nothing
    c = p.conn
    assert c.execute("SELECT COUNT(*) FROM source_files WHERE kind='events'").fetchone()[0] == \
        sqlite3.connect(str(p.db_path)).execute("SELECT COUNT(*) FROM source_files WHERE kind='events' "
                                                "AND drop_name='drop_1'").fetchone()[0]
    assert c.execute("SELECT status FROM ingest_runs ORDER BY id DESC LIMIT 1").fetchone()[0] == "failed"
    p.ingest(data / "drop_2")                        # re-run recovers
    ref = make_pipeline("ref.db")
    ref.ingest(data / "drop_1")
    ref.ingest(data / "drop_2")
    assert db_fingerprint(p.db_path) != snapshot
    assert p.report(tmp_path / "a") == ref.report(tmp_path / "b")


def test_crash_during_lead_file_leaves_no_partial_leads(gen_data, make_pipeline, monkeypatch):
    data = gen_data(42)
    p = make_pipeline("crash2.db")

    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(ingest_mod, "persist_leads", boom)
    with pytest.raises(RuntimeError):
        p.ingest(data / "drop_1")
    for t in ("leads", "lead_fields", "lead_rejections", "source_files", "batches"):
        assert p.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] == 0
    assert not p.conn.in_transaction


# ---------------------------------------------------------------- append-only
def test_ledger_is_append_only(pipeline, tmp_path):
    pipeline.ingest(write_drop(tmp_path, "drop_1", None, call_events("1", "c1", "2026-10-01T10:00:00Z")))
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        pipeline.conn.execute("UPDATE events SET event_type='x'")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        pipeline.conn.execute("DELETE FROM events")
    assert pipeline.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2


def test_override_never_modifies_original_event(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="renewed")
    pipeline.ingest(write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1"))}, evs))
    before = pipeline.conn.execute("SELECT * FROM events ORDER BY id").fetchall()
    pipeline.ingest(write_drop(tmp_path, "drop_2", None, [ev(
        "o", "disposition.override", "c1", "2026-10-05T10:00:00Z", None, disposition_code="dnd",
        overridden_by="s", reason="r")], events_name="events_1001.jsonl"))
    after = pipeline.conn.execute("SELECT * FROM events ORDER BY id").fetchall()
    assert after[:len(before)] == before and len(after) == len(before) + 1


# ------------------------------------------------------------------ adversarial
def test_empty_files_are_fine(pipeline, tmp_path):
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): ""})
    (d / "events").mkdir()
    (d / "events" / "events_0001.jsonl").write_bytes(b"")
    s = pipeline.ingest(d)
    assert s.files_ingested == 2 and s.lead_rows == 0 and s.events_read == 0
    rep = pipeline.report(tmp_path / "o")
    assert rep["lead_report.csv"].count("\n") == 1                      # header only
    assert rep["batch_summary.csv"].splitlines()[1:] == ["motor_renewal,batch_001,0,0,0,0,0"]


def test_non_utf8_csv_fails_cleanly_and_leaves_nothing(pipeline, tmp_path):
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): HDR})
    (d / "leads" / "motor_renewal" / "batch_001.csv").write_bytes(
        b"customer_name,phone,policy_number\n\xff\xfe\x00bad,9876543210,P1\n")
    with pytest.raises(ValueError, match="not valid UTF-8"):
        pipeline.ingest(d)
    assert pipeline.conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 0


def test_garbage_bytes_in_events_are_just_malformed_lines(pipeline, tmp_path):
    d = write_drop(tmp_path, "drop_1", None, [])
    f = d / "events" / "events_0001.jsonl"
    f.write_bytes(b"\xff\xfe\x00\x01garbage\n" + call_events("1", "c1", "2026-10-01T10:00:00Z")[0].encode()
                  + b"\n{\"event_id\": \"" + b"x" * 5000 + b"\n")
    s = pipeline.ingest(d)
    assert (s.events_read, s.events_malformed, s.events_inserted) == (3, 2, 1)


def test_unknown_workflow_directory_is_rejected_before_anything_is_ingested(pipeline, tmp_path):
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1")),
                                         ("nonexistent_wf", "batch_002"): HDR})
    with pytest.raises(ConfigError, match="nonexistent_wf"):
        pipeline.ingest(d)
    assert pipeline.conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 0


def test_missing_input_path_and_empty_folder(pipeline, tmp_path):
    with pytest.raises(ConfigError):
        pipeline.ingest(tmp_path / "nope")
    (tmp_path / "empty").mkdir()
    with pytest.raises(ConfigError, match="No drops"):
        pipeline.ingest(tmp_path / "empty")


def test_conflicting_duplicate_event_ids_first_wins(pipeline, tmp_path):
    a = ev("same", "call.initiated", "c1", "2026-10-01T10:00:00Z")
    b = ev("same", "call.initiated", "c2", "2026-10-09T10:00:00Z")
    s = pipeline.ingest(write_drop(tmp_path, "drop_1", None, [a, b]))
    assert s.events_duplicate == 1
    assert pipeline.conn.execute("SELECT call_id FROM events").fetchall() == [("c1",)]


def test_large_event_batch_and_weird_order(pipeline, tmp_path):
    lines = []
    for i in range(3000):
        lines += call_events(f"e{i}", f"c{i}", f"2026-10-0{1 + i % 9}T10:{i % 60:02d}:00Z", PH)
    lines.reverse()                                    # completely reversed arrival order
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1"))}, lines)
    pipeline.ingest(d)
    rep = pipeline.report(tmp_path / "o")
    assert "motor_renewal,P1,batch_001,+91******3210,in_progress_dialed,3000,3000," in rep["lead_report.csv"]


def test_duplicate_headers_and_extra_columns(pipeline, tmp_path):
    text = ("phone,mobile,customer_name,policy_number,junk\r\n"
            "9876543210,1111111111,Asha,P1,zzz\r\n")      # first matching column wins
    pipeline.ingest(write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): text}))
    rep = pipeline.report(tmp_path / "o")
    assert "+91******3210" in rep["lead_report.csv"]


# --------------------------------------------------------------------- keys
def test_wrong_keys_on_existing_database_are_refused(make_pipeline):
    p = make_pipeline("k.db")
    p.close()
    with pytest.raises(ConfigError, match="do not match"):
        Pipeline(p.db_path, OTHER_KEYS)


def test_tampered_ciphertext_is_detected_on_report(pipeline, tmp_path):
    from solution.security import DecryptionError
    pipeline.ingest(write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1"))}))
    pipeline.conn.execute("UPDATE lead_fields SET value_enc = zeroblob(40) WHERE field_name='phone'")
    with pytest.raises(DecryptionError):
        pipeline.report(tmp_path / "o")


# ------------------------------------------------------------ hard kill (real process)
def test_hard_killed_process_then_rerun_matches_clean_run(gen_data, make_pipeline, tmp_path):
    import os
    import subprocess
    import sys
    import time
    from solution.tests.conftest import REPO, TEST_KEYS, b64
    data = gen_data(42)
    env = {**os.environ, "PYTHONPATH": str(REPO), "LEDGER_ENCRYPTION_KEY": b64(TEST_KEYS.encryption),
           "LEDGER_HMAC_KEY": b64(TEST_KEYS.hmac)}
    db = tmp_path / "killed.db"
    proc = subprocess.Popen([sys.executable, "-m", "solution", "--db", str(db), "ingest", str(data)],
                            cwd=tmp_path, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 30
    while time.time() < deadline and proc.poll() is None:       # wait until it is mid-way through
        try:
            c = sqlite3.connect(str(db), timeout=1)
            n = c.execute("SELECT COUNT(*) FROM source_files").fetchone()[0]
            c.close()
            if n >= 3:
                break
        except sqlite3.Error:
            pass
        time.sleep(0.02)
    proc.kill()
    proc.wait()
    r = subprocess.run([sys.executable, "-m", "solution", "--db", str(db), "ingest", str(data)],
                       cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    r = subprocess.run([sys.executable, "-m", "solution", "--db", str(db), "report", "--out", "rep"],
                       cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    clean = make_pipeline("clean.db")
    clean.ingest(data)
    want = clean.report(tmp_path / "want")
    got = {p.name: p.read_text(encoding="utf-8") for p in (tmp_path / "rep").iterdir()}
    assert got == want
    chk = sqlite3.connect(str(db))
    assert chk.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert chk.execute("PRAGMA foreign_key_check").fetchall() == []


def test_noop_reingest_leaves_no_audit_row(gen_data, pipeline):
    pipeline.ingest(gen_data(42) / "drop_1")
    runs = pipeline.conn.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0]
    pipeline.ingest(gen_data(42) / "drop_1")
    assert pipeline.conn.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0] == runs
