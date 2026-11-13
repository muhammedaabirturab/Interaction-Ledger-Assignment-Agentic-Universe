"""Independent reference implementation of the Assignment A spec (test oracle).

Deliberately shares NO code with the pipeline: plain dicts, plaintext phones, one
in-memory pass over *all* input at once, different parsing mechanics (strptime,
manual prefix stripping).  Differential tests compare its output byte-for-byte
with the real pipeline's reports.
"""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))


def _hdr(s):
    return " ".join(re.sub(r"[_\s]+", " ", s.strip().lower()).split(" ")).strip()


def _phone(s):
    d = "".join(c for c in s if c in "0123456789")
    if len(d) == 12 and d[:2] == "91":
        d = d[2:]
    elif len(d) == 11 and d[0] == "0":
        d = d[1:]
    return "+91" + d if len(d) == 10 and d[0] in "6789" else None


def _date(s):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _money(s):
    t = s.strip()
    low = t.lower()
    for pre in ("₹", "rs.", "rs", "inr"):
        if low.startswith(pre):
            t = t[len(pre):].strip()
            break
    t = t.replace(",", "")
    if not re.fullmatch(r"\d+(\.\d+)?|\.\d+", t):
        return None
    return str(Decimal(t).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _ts(s):
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt


def run(drop_dirs: list[Path], schemas_dir: Path) -> dict[str, str]:
    schemas = {}
    for p in schemas_dir.glob("*.json"):
        d = json.loads(p.read_text(encoding="utf-8"))
        schemas[d["workflow_id"]] = d
    disp = {}
    with (schemas_dir / "dispositions.csv").open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            disp[(r["workflow_id"], r["code"])] = (r["is_terminal"] == "true", r["is_success"] == "true")

    # ---------------- leads
    leads = {}                       # (wf, phone) -> dict
    batches = set()
    rejections = []                  # (wf, batch, rownum, reason)
    rows_total = rejected = merged = warnings = 0
    for drop in drop_dirs:
        for wf_dir in sorted((drop / "leads").glob("*")):
            wf = wf_dir.name
            sch = schemas[wf]
            for f in sorted(wf_dir.glob("*.csv")):
                batch = f.stem
                batches.add((wf, batch))
                with f.open(newline="", encoding="utf-8-sig") as fh:
                    allrows = list(csv.reader(fh))
                if not allrows:
                    continue
                head = [_hdr(h) for h in allrows[0]]
                col = {}
                for fld in sch["fields"]:
                    keys = {_hdr(fld["name"])} | {_hdr(a) for a in fld.get("aliases", [])}
                    for i, h in enumerate(head):
                        if h in keys:
                            col[fld["name"]] = i
                            break
                n = 0
                for cells in allrows[1:]:
                    if not any(c.strip() for c in cells):
                        continue
                    n += 1
                    rows_total += 1
                    vals, w, reason = {}, 0, None
                    for fld in sch["fields"]:
                        i = col.get(fld["name"])
                        raw = cells[i].strip() if i is not None and i < len(cells) else ""
                        if raw == "":
                            if fld["required"]:
                                reason = "missing_required:" + fld["name"]
                                break
                            continue
                        t = fld["type"]
                        if t == "string":
                            v = raw
                        elif t == "phone":
                            v = _phone(raw)
                            if v is None:
                                reason = "invalid_phone"
                                break
                        elif t == "date":
                            v = _date(raw)
                        elif t == "decimal":
                            v = _money(raw)
                        else:
                            v = raw.lower() if raw.lower() in fld["values"] else None
                        if v is None:
                            w += 1
                        else:
                            vals[fld["name"]] = v
                    if reason:
                        rejected += 1
                        rejections.append((wf, batch, n, reason))
                        continue
                    warnings += w
                    key = (wf, vals["phone"])
                    if key not in leads:
                        leads[key] = {"batch": batch, "vals": dict(vals)}
                    else:
                        merged += 1
                        leads[key]["vals"].update(vals)
    for (wf, _), L in leads.items():
        ref = next(f["name"] for f in schemas[wf]["fields"] if f.get("reference"))
        L["ref"] = L["vals"][ref]

    # ---------------- events
    seen, evs = set(), []
    total = malformed = dups = 0
    for drop in drop_dirs:
        for f in sorted((drop / "events").glob("*.jsonl")):
            for line in f.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                total += 1
                try:
                    e = json.loads(line)
                    assert isinstance(e, dict) and e.get("event_id")
                except Exception:
                    malformed += 1                  # spec: bad JSON or no event_id - nothing else
                    continue
                if e["event_id"] in seen:
                    dups += 1
                    continue
                seen.add(e["event_id"])
                try:
                    e["_ts"] = _ts(e["occurred_at"])
                except Exception:
                    e["_ts"] = None                 # still a valid event; just cannot be ordered
                e["event_type"] = e.get("event_type") or ""
                e["payload"] = e.get("payload") if isinstance(e.get("payload"), dict) else {}
                evs.append(e)

    def phone_key(e):
        ph = e.get("phone")
        if ph is None or str(ph).strip() == "":
            return "none"
        p = _phone(str(ph))
        return (e.get("workflow_id"), p) if p and (e.get("workflow_id"), p) in leads else "orphan"

    # phone-linked events first; then the call's lead = lead of its EARLIEST linked call.initiated
    # (else its earliest linked event); only disposition.override may link through call_id.
    linked_by_phone = [(phone_key(e), e) for e in evs]
    linked_by_phone = [(k, e) for k, e in linked_by_phone if k not in ("none", "orphan")]
    call_lead = {}
    ordered = sorted(linked_by_phone, key=lambda t: (
        not (t[1]["event_type"] == "call.initiated" and t[1]["_ts"] is not None),
        t[1]["_ts"] is None,
        t[1]["_ts"] or datetime.min.replace(tzinfo=timezone.utc), t[1]["event_id"]))
    for k, e in ordered:
        if e.get("call_id"):
            call_lead.setdefault((e["workflow_id"], str(e["call_id"])), k)
    linked, orphans, unknown = [], 0, 0
    for e in evs:
        k = phone_key(e)
        if k == "none" and e["event_type"] == "disposition.override":
            k = call_lead.get((e.get("workflow_id"), str(e.get("call_id"))), "orphan")
        if k in ("none", "orphan"):
            orphans += 1
            continue
        if e["event_type"].startswith("disposition.") and                 (e["workflow_id"], e["payload"].get("disposition_code")) not in disp:
            unknown += 1
            continue
        linked.append((k, e))

    calls = {}
    for key, e in linked:
        if e["event_type"] == "call.initiated" and e["_ts"] is not None:
            cur = calls.get((e["workflow_id"], e["call_id"]))
            if cur is None or (e["_ts"], e["event_id"]) < cur["first"]:
                calls[(e["workflow_id"], e["call_id"])] = {"lead": key, "evs": [],
                                                           "first": (e["_ts"], e["event_id"])}
    for key, e in linked:
        c = calls.get((e["workflow_id"], e["call_id"]))
        if c and c["lead"] == key:
            c["evs"].append(e)
    for (wf, _), c in calls.items():
        c["init"] = c["first"][0]
        c["connected"] = any(e["event_type"] == "call.connected" for e in c["evs"])
        c["tfailed"] = any(e["event_type"] == "call.telephony_failed" for e in c["evs"])
        timed = [e for e in c["evs"] if e["_ts"] is not None]
        ov = [e for e in timed if e["event_type"] == "disposition.override"]
        st = [e for e in timed if e["event_type"] == "disposition.set"]
        pool = ov or st
        c["disp"] = max(pool, key=lambda e: (e["_ts"], e["event_id"]))["payload"][
            "disposition_code"] if pool else None
        c["term"] = bool(c["disp"]) and disp[(wf, c["disp"])][0]
        c["succ"] = bool(c["disp"]) and disp[(wf, c["disp"])][1]

    by_lead = {}
    for (wf, cid), c in calls.items():
        by_lead.setdefault(c["lead"], []).append((cid, c))

    def bucket(cs):
        if not cs:
            return "yet_to_reach_out"
        if any(c["term"] for _, c in cs):
            return "completed"
        if all(c["tfailed"] for _, c in cs):
            return "in_progress_telephony_failed"
        return "in_progress_dialed"

    out = {}

    def emit(name, header, rows):
        import io
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
        out[name] = buf.getvalue()

    # batch summary
    rows = []
    for wf, b in sorted(batches):
        bs = [bucket(by_lead.get(k, [])) for k, L in leads.items() if k[0] == wf and L["batch"] == b]
        rows.append([wf, b, len(bs)] + [bs.count(x) for x in (
            "yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed")])
    emit("batch_summary.csv", ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
                               "in_progress_telephony_failed", "in_progress_dialed", "completed"], rows)

    # daily overview
    days = {}
    for (wf, cid), c in calls.items():
        d = c["init"].astimezone(IST).date().isoformat()
        x = days.setdefault((wf, d), {"t": 0, "c": 0, "s": set()})
        x["t"] += 1
        x["c"] += c["connected"]
        if c["succ"]:
            x["s"].add(c["lead"])
    emit("daily_overview.csv", ["workflow_id", "date", "total_calls", "connected_calls",
                                "connectivity_rate", "success_leads"],
         [[wf, d, x["t"], x["c"], "%.4f" % (x["c"] / x["t"]), len(x["s"])]
          for (wf, d), x in sorted(days.items())])

    # lead report
    rows = []
    for k, L in leads.items():
        cs = by_lead.get(k, [])
        last = max(cs, key=lambda t: (t[1]["init"], t[0]))[1]["init"] if cs else None
        withd = [t for t in cs if t[1]["disp"]]
        cur = max(withd, key=lambda t: (t[1]["init"], t[0]))[1]["disp"] if withd else ""
        rows.append((k[0], L["ref"], L["batch"], "+91******" + k[1][-4:], bucket(cs), len(cs),
                     sum(c["connected"] for _, c in cs),
                     last.astimezone(IST).replace(microsecond=0).isoformat() if last else "",
                     cur, "true" if any(c["succ"] for _, c in cs) else "false"))
    rows.sort(key=lambda r: (r[0], r[1]))
    emit("lead_report.csv", ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
                             "total_calls", "connected_calls", "last_call_at", "current_disposition",
                             "ever_success"], rows)

    rejections.sort(key=lambda r: (r[0], r[1], r[2]))
    emit("rejections.csv", ["workflow_id", "batch_id", "row_number", "reason"], rejections)

    reasons = {}
    for r in rejections:
        reasons[r[3]] = reasons.get(r[3], 0) + 1
    out["data_quality.json"] = json.dumps({
        "lead_rows_total": rows_total, "lead_rows_rejected": rejected,
        "rejections_by_reason": dict(sorted(reasons.items())),
        "duplicate_lead_rows_merged": merged, "field_warnings": warnings,
        "events_total": total, "malformed_events": malformed,
        "duplicate_events_dropped": dups, "orphan_events": orphans,
        "unknown_disposition_events": unknown}, indent=2, ensure_ascii=False) + "\n"
    return out
