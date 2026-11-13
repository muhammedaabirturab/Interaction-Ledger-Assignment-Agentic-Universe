"""Lead-file rules: header mapping, rejection order, warnings, blank rows, merging."""
from __future__ import annotations

import csv
import io
import json
import sqlite3

from solution.lead_ingest import parse_lead_csv
from solution.tests.conftest import SCHEMAS, TEST_KEYS, write_drop
from solution.workflows import load_schemas

SCH = load_schemas(SCHEMAS)
MOTOR = SCH["motor_renewal"]
HDR = "customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language\r\n"


def parse(text, schema=MOTOR):
    return list(parse_lead_csv(text, schema))


def test_first_failure_in_schema_order_wins():
    rows = parse(HDR + ",abc,\r\n" + "Asha,abc,\r\n" + "Asha,9876543210,\r\n" + ",9876543210,P1\r\n")
    assert [r.reason for r in rows] == [
        "missing_required:customer_name",   # name missing AND phone invalid AND ref missing
        "invalid_phone",                    # phone invalid AND ref missing -> phone first
        "missing_required:policy_number",
        "missing_required:customer_name"]


def test_missing_phone_is_missing_required_not_invalid():
    assert parse(HDR + "Asha,,P1\r\n")[0].reason == "missing_required:phone"


def test_whitespace_only_counts_as_missing():
    assert parse(HDR + "   ,9876543210,P1\r\n")[0].reason == "missing_required:customer_name"


def test_blank_rows_skipped_and_not_numbered():
    text = HDR + "A,9876543210,P1\r\n,,,,,\r\n   , ,,,,\r\n\r\nB,abc,P2\r\n"
    rows = parse(text)
    assert [(r.row_number, r.reason) for r in rows] == [(1, None), (2, "invalid_phone")]


def test_aliases_reordered_extra_columns_ignored():
    text = ("Language,Insured Name,Policy No,Mobile No,Expiry Date,Premium,agent_notes\r\n"
            'HI,Asha,P1,98765 43210,01-Dec-2026,"Rs. 1,200",VIP\r\n')
    r = parse(text)[0]
    assert r.reason is None
    assert r.values == {"customer_name": "Asha", "phone": "+919876543210", "policy_number": "P1",
                        "policy_expiry": "2026-12-01", "premium_amount": "1200.00",
                        "preferred_language": "hi"}


def test_header_matching_is_whitespace_and_underscore_insensitive():
    text = " POLICY_NUMBER ,Customer   Name,PHONE\r\nP1,Asha,9876543210\r\n"
    assert parse(text)[0].reason is None


def test_missing_required_column_rejects_every_row():
    rows = parse("customer_name,phone\r\nAsha,9876543210\r\n")
    assert [r.reason for r in rows] == ["missing_required:policy_number"]


def test_optional_invalid_warns_and_is_null_only_when_row_survives():
    ok = parse(HDR + "A,9876543210,P1,31/02/2026,N/A,Hindi\r\n")[0]
    assert ok.reason is None and ok.warnings == 3
    assert set(ok.values) == {"customer_name", "phone", "policy_number"}
    rejected = parse(HDR + "A,abc,P1,31/02/2026,N/A,Hindi\r\n")[0]
    assert rejected.reason == "invalid_phone"          # warnings irrelevant for rejected rows


def test_empty_optional_is_not_a_warning():
    r = parse(HDR + "A,9876543210,P1,,,\r\n")[0]
    assert r.warnings == 0


def test_empty_and_header_only_files():
    assert parse("") == []
    assert parse(HDR) == []


def test_short_rows_are_padded_not_crashing():
    r = parse(HDR + "A,9876543210,P1\r\n")[0]
    assert r.reason is None


# ----------------------------------------------------------------- merging (DB)
def lead_state(db_path):
    """Return {external_ref: (batch, {plain fields})}."""
    c = sqlite3.connect(str(db_path))
    out = {}
    for lid, ref, batch in c.execute("SELECT id, external_ref, batch_id FROM leads"):
        f = dict(c.execute("SELECT field_name, value_plain FROM lead_fields WHERE lead_id=? "
                           "AND value_plain IS NOT NULL", (lid,)))
        out[ref] = (batch, f)
    c.close()
    return out


def test_merge_last_non_null_wins_defaults_after_and_batch_is_permanent(pipeline, tmp_path):
    d1 = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): HDR
                                         + "Asha,9876543210,P1,01/12/2026,1000,\r\n"})
    d2 = write_drop(tmp_path, "drop_2", {("motor_renewal", "batch_002"): HDR
                                         + 'Asha R,+91 98765 43210,P1-NEW,,"Rs. 2,500",HI\r\n'
                                         + "Asha,09876543210,P1-NEW2,31/02/2026,N/A,xx\r\n"})
    pipeline.ingest(d1)
    ref1 = lead_state(pipeline.db_path)
    assert ref1["P1"] == ("batch_001", {"policy_expiry": "2026-12-01", "premium_amount": "1000.00",
                                        "preferred_language": "en"})        # default applied
    s = pipeline.ingest(d2)
    assert s.duplicates_merged == 2
    state = lead_state(pipeline.db_path)
    assert list(state) == ["P1-NEW2"]                    # reference is last-write-wins too
    batch, f = state["P1-NEW2"]
    assert batch == "batch_001"                          # batch_id never changes
    assert f["policy_expiry"] == "2026-12-01"            # blank/invalid later values do not erase
    assert f["premium_amount"] == "2500.00"              # later valid value wins; later N/A does not erase
    assert f["preferred_language"] == "hi"


def test_merge_quoted_amount(pipeline, tmp_path):
    d1 = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): HDR
                                         + "Asha,9876543210,P1,,1000,\r\n"
                                         + 'Asha,9876543210,P1,,"₹2,500",ta\r\n'})
    s = pipeline.ingest(d1)
    assert s.duplicates_merged == 1
    assert lead_state(pipeline.db_path)["P1"][1] == {"premium_amount": "2500.00",
                                                      "preferred_language": "ta"}


def test_identity_is_per_workflow(pipeline, tmp_path):
    fb = "customer_name,phone,order_id\r\nAsha,9876543210,O1\r\n"
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): HDR + "Asha,9876543210,P1\r\n",
                                        ("policy_feedback", "batch_002"): fb})
    s = pipeline.ingest(d)
    assert s.duplicates_merged == 0
    assert len(lead_state(pipeline.db_path)) == 2


def test_rejection_rows_have_correct_numbers_and_no_pii(pipeline, tmp_path):
    d = write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"): HDR
                                        + "SecretName,000,P1\r\n,,,,,\r\nGood,9876543210,P2\r\n"
                                        + "Other,9876543211,\r\n"})
    pipeline.ingest(d)
    rep = pipeline.report(tmp_path / "out")
    assert rep["rejections.csv"].splitlines() == [
        "workflow_id,batch_id,row_number,reason",
        "motor_renewal,batch_001,1,invalid_phone",
        "motor_renewal,batch_001,3,missing_required:policy_number"]
    assert "SecretName" not in rep["rejections.csv"] and "9876543211" not in rep["rejections.csv"]
    dq = json.loads(rep["data_quality.json"])
    assert dq["lead_rows_total"] == 3 and dq["lead_rows_rejected"] == 2
    assert dq["rejections_by_reason"] == {"invalid_phone": 1, "missing_required:policy_number": 1}
