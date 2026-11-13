"""Write the five report files from the derived temp tables (deterministic output)."""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from solution.normalise import format_ist, mask_phone
from solution.reporting import derive
from solution.security import Crypto, pii_aad

BATCH_COLS = ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
              "in_progress_telephony_failed", "in_progress_dialed", "completed"]
DAILY_COLS = ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate",
              "success_leads"]
LEAD_COLS = ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
             "connected_calls", "last_call_at", "current_disposition", "ever_success"]
REJECT_COLS = ["workflow_id", "batch_id", "row_number", "reason"]

IST_DATE = "date(c.init_us / 1000000 + 19800, 'unixepoch')"


def _write_csv(path: Path, header: list[str], rows) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def batch_summary(conn: sqlite3.Connection, out: Path) -> None:
    rows = conn.execute("""
        SELECT b.workflow_id, b.batch_id, COUNT(f.lead_id),
               COALESCE(SUM(f.bucket = 'yet_to_reach_out'), 0),
               COALESCE(SUM(f.bucket = 'in_progress_telephony_failed'), 0),
               COALESCE(SUM(f.bucket = 'in_progress_dialed'), 0),
               COALESCE(SUM(f.bucket = 'completed'), 0)
        FROM batches b LEFT JOIN lead_facts f
             ON f.workflow_id = b.workflow_id AND f.batch_id = b.batch_id
        GROUP BY b.workflow_id, b.batch_id ORDER BY b.workflow_id, b.batch_id""").fetchall()
    _write_csv(out / "batch_summary.csv", BATCH_COLS, rows)


def daily_overview(conn: sqlite3.Connection, out: Path) -> None:
    rows = conn.execute(f"""
        SELECT c.workflow_id, {IST_DATE} AS d, COUNT(*), SUM(c.connected),
               COUNT(DISTINCT CASE WHEN c.is_success = 1 THEN c.lead_id END)
        FROM call_full c GROUP BY c.workflow_id, d ORDER BY c.workflow_id, d""").fetchall()
    _write_csv(out / "daily_overview.csv", DAILY_COLS,
               [(w, d, t, conn_, f"{conn_ / t:.4f}", s) for w, d, t, conn_, s in rows])


def lead_report(conn: sqlite3.Connection, crypto: Crypto, out: Path) -> None:
    cur = conn.execute("""
        SELECT f.lead_id, f.workflow_id, f.external_ref, f.batch_id, f.bucket, f.total_calls,
               f.connected_calls, f.last_call_us, cur.disp_code, f.ever_success, p.value_enc
        FROM lead_facts f
        LEFT JOIN lead_current cur ON cur.lead_id = f.lead_id
        LEFT JOIN lead_fields p ON p.lead_id = f.lead_id AND p.field_name = 'phone'
        ORDER BY f.workflow_id, f.external_ref, f.lead_id""")

    def rows():
        for (lid, wf, ref, batch, bucket, total, conn_, last_us, disp, success, enc) in cur:
            phone = crypto.decrypt(bytes(enc), pii_aad(lid, "phone"))
            yield (wf, ref, batch, mask_phone(phone), bucket, total, conn_,
                   format_ist(last_us) if last_us is not None else "",
                   disp or "", "true" if success else "false")
    _write_csv(out / "lead_report.csv", LEAD_COLS, rows())


def rejections(conn: sqlite3.Connection, out: Path) -> None:
    rows = conn.execute("""
        SELECT workflow_id, batch_id, row_number, reason FROM lead_rejections
        ORDER BY workflow_id, batch_id, row_number, source_file_id""").fetchall()
    _write_csv(out / "rejections.csv", REJECT_COLS, rows)


def data_quality(conn: sqlite3.Connection, out: Path) -> dict:
    sums = conn.execute("""
        SELECT COALESCE(SUM(rows_total),0), COALESCE(SUM(rows_rejected),0),
               COALESCE(SUM(duplicates_merged),0), COALESCE(SUM(field_warnings),0),
               COALESCE(SUM(events_total),0), COALESCE(SUM(events_malformed),0),
               COALESCE(SUM(events_duplicate),0) FROM source_files""").fetchone()
    by_reason = dict(conn.execute(
        "SELECT reason, COUNT(*) FROM lead_rejections GROUP BY reason ORDER BY reason"))
    total_ev = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    linked = conn.execute("SELECT COUNT(DISTINCT id) FROM ev_lead").fetchone()[0]
    unknown = conn.execute("""
        SELECT COUNT(DISTINCT x.id) FROM ev_lead x
        WHERE x.event_type IN ('disposition.set','disposition.override')
          AND NOT EXISTS (SELECT 1 FROM dispositions d
                          WHERE d.workflow_id = x.workflow_id AND d.code = x.disposition_code)"""
                           ).fetchone()[0]
    dq = {
        "lead_rows_total": sums[0],
        "lead_rows_rejected": sums[1],
        "rejections_by_reason": by_reason,
        "duplicate_lead_rows_merged": sums[2],
        "field_warnings": sums[3],
        "events_total": sums[4],
        "malformed_events": sums[5],
        "duplicate_events_dropped": sums[6],
        "orphan_events": total_ev - linked,
        "unknown_disposition_events": unknown,
    }
    (out / "data_quality.json").write_text(
        json.dumps(dq, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return dq


def write_all(conn: sqlite3.Connection, crypto: Crypto, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    derive.build(conn)
    try:
        batch_summary(conn, out)
        daily_overview(conn, out)
        lead_report(conn, crypto, out)
        rejections(conn, out)
        return data_quality(conn, out)
    finally:
        derive.drop(conn)
