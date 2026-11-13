"""Event semantics: linking, orphans, effective disposition, buckets, IST dates."""
from __future__ import annotations

import csv
import io
import json

from solution.tests.conftest import write_drop

HDR = "customer_name,phone,policy_number\r\n"
PH = "9876543210"


def lead_csv(*rows):
    return HDR + "".join(f"{n},{p},{ref}\r\n" for n, p, ref in rows)


def ev(eid, etype, call, ts, phone=PH, wf="motor_renewal", **payload):
    e = {"event_id": eid, "event_type": etype, "workflow_id": wf, "call_id": call,
         "occurred_at": ts, "payload": payload}
    if phone is not None:
        e["phone"] = phone
    return json.dumps(e)


def call_events(n, call, ts, phone=PH, connected=True, code=None, **kw):
    """initiated [+ connected + ended] [+ disposition.set]"""
    out = [ev(f"{n}a", "call.initiated", call, ts, phone)]
    if connected:
        out.append(ev(f"{n}b", "call.connected", call, ts, phone))
    if code:
        out.append(ev(f"{n}c", "disposition.set", call, ts, phone, disposition_code=code))
    return out


def rows_of(text):
    return list(csv.DictReader(io.StringIO(text)))


def run(pipeline, tmp_path, leads, events, drop="drop_1"):
    d = write_drop(tmp_path, drop, {("motor_renewal", "batch_001"): lead_csv(*leads)} if leads else None,
                   events)
    pipeline.ingest(d)
    rep = pipeline.report(tmp_path / f"out_{drop}")
    return rep, rows_of(rep["lead_report.csv"]), json.loads(rep["data_quality.json"])


def test_effective_disposition_override_wins_even_if_older_than_set(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="not_interested") + [
        ev("1o", "disposition.override", "c1", "2026-10-01T09:00:00Z", None,   # OLDER than set
           disposition_code="renewed", overridden_by="s1", reason="qa_review")]
    _, leads, _ = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert leads[0]["current_disposition"] == "renewed" and leads[0]["ever_success"] == "true"
    assert leads[0]["bucket"] == "completed"


def test_latest_override_by_occurred_at_not_file_order(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="renewed") + [
        # later in time but earlier in file; mixed timestamp forms (IST naive vs UTC)
        ev("1p", "disposition.override", "c1", "2026-10-02 12:00:00", None,
           disposition_code="not_interested", overridden_by="s", reason="r"),
        ev("1o", "disposition.override", "c1", "2026-10-02T04:00:00Z", None,   # = 09:30 IST, earlier
           disposition_code="callback_requested", overridden_by="s", reason="r")]
    evs = [evs[0], evs[1], evs[2], evs[4], evs[3]]
    _, leads, _ = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert leads[0]["current_disposition"] == "not_interested"
    assert leads[0]["ever_success"] == "false"          # override replaced the success


def test_latest_set_without_override(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="callback_requested") + [
        ev("1x", "disposition.set", "c1", "2026-10-01T11:00:00Z", PH, disposition_code="payment_link_sent")]
    _, leads, _ = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert leads[0]["current_disposition"] == "payment_link_sent"
    assert leads[0]["bucket"] == "in_progress_dialed"


def test_unknown_disposition_ignored_and_counted(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="renewed") + [
        ev("1u", "disposition.set", "c1", "2026-10-01T12:00:00Z", PH, disposition_code="RENEWD"),
        ev("1v", "disposition.override", "c1", "2026-10-01T13:00:00Z", None, disposition_code="nope",
           overridden_by="s", reason="r"),
        ev("1w", "disposition.set", "c1", "2026-10-01T14:00:00Z", PH, disposition_code="language_barrier")]
    _, leads, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert dq["unknown_disposition_events"] == 2
    assert leads[0]["current_disposition"] == "language_barrier"   # latest KNOWN set
    # a feedback-only code is unknown for motor_renewal
    evs2 = call_events("2", "c2", "2026-10-03T10:00:00Z", phone="9876543211", code="feedback_captured")
    _, _, dq2 = run(pipeline, tmp_path, [("B", "9876543211", "P2")], evs2, drop="drop_2")
    assert dq2["unknown_disposition_events"] == 3


def test_buckets(pipeline, tmp_path):
    people = [("Y", "9000000001", "yet"), ("C", "9000000002", "comp"), ("T", "9000000003", "tfail"),
              ("D", "9000000004", "dial"), ("M", "9000000005", "mixed")]
    evs = []
    evs += call_events("c", "call_c", "2026-10-01T10:00:00Z", "9000000002", code="dnd")
    evs += call_events("c2", "call_c2", "2026-10-02T10:00:00Z", "9000000002", connected=False)
    evs += [ev("t1", "call.initiated", "call_t1", "2026-10-01T10:00:00Z", "9000000003"),
            ev("t2", "call.telephony_failed", "call_t1", "2026-10-01T10:00:05Z", "9000000003", reason="x"),
            ev("t3", "call.initiated", "call_t2", "2026-10-02T10:00:00Z", "9000000003"),
            ev("t4", "call.telephony_failed", "call_t2", "2026-10-02T10:00:05Z", "9000000003", reason="x")]
    evs += call_events("d", "call_d", "2026-10-01T10:00:00Z", "9000000004", code="callback_requested")
    evs += [ev("m1", "call.initiated", "call_m1", "2026-10-01T10:00:00Z", "9000000005"),
            ev("m2", "call.telephony_failed", "call_m1", "2026-10-01T10:00:05Z", "9000000005", reason="x"),
            ev("m3", "call.initiated", "call_m2", "2026-10-02T10:00:00Z", "9000000005")]
    rep, leads, _ = run(pipeline, tmp_path, people, evs)
    got = {r["external_ref"]: r["bucket"] for r in leads}
    assert got == {"yet": "yet_to_reach_out", "comp": "completed", "tfail": "in_progress_telephony_failed",
                   "dial": "in_progress_dialed", "mixed": "in_progress_dialed"}
    batch = rows_of(rep["batch_summary.csv"])[0]
    assert batch["total_leads"] == "5" and batch["completed"] == "1"
    assert batch["yet_to_reach_out"] == "1" and batch["in_progress_telephony_failed"] == "1"
    assert batch["in_progress_dialed"] == "2"


def test_terminal_override_completes_and_removing_terminal_reopens(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="dnd") + [
        ev("1o", "disposition.override", "c1", "2026-10-02T10:00:00Z", None,
           disposition_code="callback_requested", overridden_by="s", reason="r")]
    _, leads, _ = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert leads[0]["bucket"] == "in_progress_dialed"      # override un-terminated the call


def test_daily_overview_ist_dates_connectivity_and_success_leads(pipeline, tmp_path):
    p1, p2 = "9000000001", "9000000002"
    evs = []
    # 20:00Z on Oct 1 == 01:30 IST Oct 2 ; naive 23:30 stays Oct 1 ; +05:30 form
    evs += call_events("a", "ca", "2026-10-01T20:00:00Z", p1, code="renewed")
    evs += call_events("b", "cb", "2026-10-01 23:30:00", p1, connected=False)
    evs += call_events("c", "cc", "2026-10-02T01:15:00+05:30", p2, code="renewed")
    evs += call_events("d", "cd", "2026-10-02T02:00:00+05:30", p2, code="renewed")   # same lead again
    rep, leads, _ = run(pipeline, tmp_path, [("A", p1, "P1"), ("B", p2, "P2")], evs)
    days = {r["date"]: r for r in rows_of(rep["daily_overview.csv"])}
    assert set(days) == {"2026-10-01", "2026-10-02"}
    assert days["2026-10-01"]["total_calls"] == "1" and days["2026-10-01"]["connected_calls"] == "0"
    assert days["2026-10-01"]["connectivity_rate"] == "0.0000" and days["2026-10-01"]["success_leads"] == "0"
    d2 = days["2026-10-02"]
    assert (d2["total_calls"], d2["connected_calls"]) == ("3", "3")
    assert d2["connectivity_rate"] == "1.0000"
    assert d2["success_leads"] == "2"                       # distinct leads, not calls (p2 has 2)
    a = next(r for r in leads if r["external_ref"] == "P1")
    assert a["last_call_at"] == "2026-10-02T01:30:00+05:30"   # IST rendering of 20:00Z


def test_connectivity_rate_four_decimals(pipeline, tmp_path):
    evs = []
    for i in range(16):
        evs += call_events(f"x{i}", f"c{i}", "2026-10-01T10:00:00Z", connected=i < 9)
    rep, _, _ = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert rows_of(rep["daily_overview.csv"])[0]["connectivity_rate"] == "0.5625"


def test_call_without_initiated_is_not_counted(pipeline, tmp_path):
    evs = [ev("1", "call.connected", "c1", "2026-10-01T10:00:00Z"),
           ev("2", "disposition.set", "c1", "2026-10-01T10:05:00Z", disposition_code="renewed")]
    rep, leads, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert leads[0]["total_calls"] == "0" and leads[0]["bucket"] == "yet_to_reach_out"
    assert leads[0]["ever_success"] == "false"
    assert rows_of(rep["daily_overview.csv"]) == []
    assert dq["orphan_events"] == 0                        # linked, just not a counted call


def test_orphans_then_reconciliation_when_lead_arrives(pipeline, tmp_path, make_pipeline):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z", code="renewed") + [
        ev("1o", "disposition.override", "c1", "2026-10-01T11:00:00Z", None, disposition_code="dnd",
           overridden_by="s", reason="r")]
    d1 = write_drop(tmp_path, "drop_1", None, evs)                       # events BEFORE the lead
    d2 = write_drop(tmp_path, "drop_2", {("motor_renewal", "batch_001"): lead_csv(("A", "+91 98765 43210", "P1"))})
    s1 = pipeline.ingest(d1)
    r1 = pipeline.report(tmp_path / "o1")
    dq1 = json.loads(r1["data_quality.json"])
    assert dq1["orphan_events"] == 4 and rows_of(r1["lead_report.csv"]) == [] \
        and rows_of(r1["daily_overview.csv"]) == []
    s2 = pipeline.ingest(d2)
    assert s2.events_inserted == 0 and s2.lead_rows == 1
    r2 = pipeline.report(tmp_path / "o2")
    assert json.loads(r2["data_quality.json"])["orphan_events"] == 0
    lead = rows_of(r2["lead_report.csv"])[0]
    assert lead["current_disposition"] == "dnd" and lead["bucket"] == "completed"
    # ... and identical to ingesting lead first
    other = make_pipeline("other.db")
    d3 = write_drop(tmp_path, "z_drop_a", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1"))})
    other.ingest(d3)
    other.ingest(d1)
    assert other.report(tmp_path / "o3") == r2
    assert s1.events_inserted == 4


def test_orphan_event_ledger_rows_are_kept(pipeline, tmp_path):
    run(pipeline, tmp_path, None, call_events("1", "c1", "2026-10-01T10:00:00Z", phone="9111111111"))
    n = pipeline.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert n == 2        # nothing silently discarded


def test_invalid_phone_event_is_orphan_even_if_call_has_other_linked_events(pipeline, tmp_path):
    evs = call_events("1", "c1", "2026-10-01T10:00:00Z") + [
        ev("1z", "call.ended", "c1", "2026-10-01T10:05:00Z", "12345")]
    _, _, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert dq["orphan_events"] == 1


def test_override_for_unknown_call_is_orphan(pipeline, tmp_path):
    evs = [ev("o", "disposition.override", "ghost", "2026-10-01T10:00:00Z", None, disposition_code="dnd",
              overridden_by="s", reason="r")]
    _, _, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert dq["orphan_events"] == 1 and dq["unknown_disposition_events"] == 0


def test_unknown_disposition_that_is_also_orphan_counts_only_as_orphan(pipeline, tmp_path):
    evs = [ev("o", "disposition.set", "c9", "2026-10-01T10:00:00Z", "9222222222", disposition_code="zzz")]
    _, _, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)
    assert dq["orphan_events"] == 1 and dq["unknown_disposition_events"] == 0


def test_workflow_isolation_same_phone_other_workflow(pipeline, tmp_path):
    evs = [ev("1", "call.initiated", "c1", "2026-10-01T10:00:00Z", PH, wf="policy_feedback")]
    _, leads, dq = run(pipeline, tmp_path, [("A", PH, "P1")], evs)        # lead only in motor_renewal
    assert dq["orphan_events"] == 1 and leads[0]["total_calls"] == "0"


def test_event_counters_malformed_duplicate_and_unknown_type(pipeline, tmp_path):
    good = call_events("1", "c1", "2026-10-01T10:00:00Z")
    lines = good + [good[0],                                  # duplicate
                    '{"event_id": "x1", "event_type": "call.init',   # truncated JSON
                    "[1,2,3]", "null", '{"event_type": "call.initiated"}',   # no event_id
                    '{"event_id":"bad_ts","event_type":"call.initiated","occurred_at":"soon"}',
                    "   ",                                     # blank: not counted
                    ev("u1", "call.mystery", "c1", "2026-10-01T10:00:00Z")]
    rep, leads, dq = run(pipeline, tmp_path, [("A", PH, "P1")], lines)
    assert dq["events_total"] == 2 + 1 + 5 + 1       # 2 good, 1 dup, 5 broken, 1 unknown type
    assert dq["malformed_events"] == 5 and dq["duplicate_events_dropped"] == 1
    assert leads[0]["total_calls"] == "1" and leads[0]["connected_calls"] == "1"
    assert dq["orphan_events"] == 0                    # unknown type is linked, stored, inert


def test_duplicate_across_drops_counted_once_and_not_reapplied(pipeline, tmp_path):
    e = call_events("1", "c1", "2026-10-01T10:00:00Z")
    d1 = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): lead_csv(("A", PH, "P1"))}, e)
    d2 = write_drop(tmp_path, "drop_2", None, e + call_events("2", "c2", "2026-10-02T10:00:00Z"),
                    events_name="events_1001.jsonl")
    pipeline.ingest(d1)
    pipeline.ingest(d2)
    rep = pipeline.report(tmp_path / "o")
    dq = json.loads(rep["data_quality.json"])
    assert dq["duplicate_events_dropped"] == 2 and dq["events_total"] == 6
    assert rows_of(rep["lead_report.csv"])[0]["total_calls"] == "2"
