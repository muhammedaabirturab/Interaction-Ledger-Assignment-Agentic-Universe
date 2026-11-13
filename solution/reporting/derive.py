"""Derived facts, computed in SQL from the ledger on every call (nothing is cached).

Because linkage, calls, dispositions and buckets are *functions of the current
tables*, reports always reflect everything ingested so far and an orphan whose
lead arrives later is picked up automatically - no mutable reconciliation state.

Temp tables built by :func:`build` (all in the TEMP schema, main DB untouched):

ev_lead      one row per *linked* event: (id, workflow_id, call_id, event_type,
             occurred_us, event_id, disposition_code, lead_id)
calls        one row per counted call (earliest linked call.initiated)
call_facts   per call: connected / telephony_failed flags, effective disposition
lead_facts   per lead: counts, bucket, last call, current disposition, ever_success
"""
from __future__ import annotations

import sqlite3

_LINK_STEPS = [
    # 1. events linked by phone: same workflow + same phone HMAC as a lead.
    """CREATE TEMP TABLE ev_lead AS
       SELECT e.id AS id, e.workflow_id AS workflow_id, e.call_id AS call_id,
              e.event_type AS event_type, e.occurred_us AS occurred_us, e.event_id AS event_id,
              e.disposition_code AS disposition_code, l.id AS lead_id
       FROM events e JOIN leads l ON l.workflow_id = e.workflow_id AND l.phone_hash = e.phone_hash
       WHERE e.phone_state = 'valid'""",
    # 2. a call's lead as seen through its phone-bearing events.
    """CREATE TEMP TABLE call_lead AS
       SELECT workflow_id, call_id, MIN(lead_id) AS lead_id
       FROM ev_lead WHERE call_id IS NOT NULL GROUP BY workflow_id, call_id""",
    # 3. events with no phone at all (overrides) link through their call_id.
    """INSERT INTO ev_lead
       SELECT e.id, e.workflow_id, e.call_id, e.event_type, e.occurred_us, e.event_id,
              e.disposition_code, cl.lead_id
       FROM events e JOIN call_lead cl ON cl.workflow_id = e.workflow_id AND cl.call_id = e.call_id
       WHERE e.phone_state = 'none'""",
    "CREATE INDEX temp.ix_ev_lead_call ON ev_lead(workflow_id, call_id)",
]

_CALL_STEPS = [
    # 4. calls: counted only when a linked call.initiated exists; earliest wins.
    """CREATE TEMP TABLE calls AS
       SELECT workflow_id, call_id, lead_id, occurred_us AS init_us FROM (
         SELECT workflow_id, call_id, lead_id, occurred_us,
                ROW_NUMBER() OVER (PARTITION BY workflow_id, call_id
                                   ORDER BY occurred_us, event_id) AS rn
         FROM ev_lead WHERE event_type = 'call.initiated' AND call_id IS NOT NULL) WHERE rn = 1""",
    "CREATE UNIQUE INDEX temp.ux_calls ON calls(workflow_id, call_id)",
    # 5. effective disposition: latest known override, else latest known set.
    #    Unknown codes are ignored; events of the call linked to a different lead are ignored.
    """CREATE TEMP TABLE call_disp AS
       SELECT workflow_id, call_id, code FROM (
         SELECT x.workflow_id AS workflow_id, x.call_id AS call_id, x.disposition_code AS code,
                ROW_NUMBER() OVER (PARTITION BY x.workflow_id, x.call_id
                                   ORDER BY (x.event_type = 'disposition.override') DESC,
                                            x.occurred_us DESC, x.event_id DESC) AS rn
         FROM ev_lead x
         JOIN calls c ON c.workflow_id = x.workflow_id AND c.call_id = x.call_id
                     AND c.lead_id = x.lead_id
         WHERE x.event_type IN ('disposition.set', 'disposition.override')
           AND EXISTS (SELECT 1 FROM dispositions d
                       WHERE d.workflow_id = x.workflow_id AND d.code = x.disposition_code))
       WHERE rn = 1""",
    """CREATE TEMP TABLE call_facts AS
       SELECT c.workflow_id AS workflow_id, c.call_id AS call_id, c.lead_id AS lead_id,
              c.init_us AS init_us,
              MAX(x.event_type = 'call.connected')        AS connected,
              MAX(x.event_type = 'call.telephony_failed') AS tfailed
       FROM calls c JOIN ev_lead x ON x.workflow_id = c.workflow_id AND x.call_id = c.call_id
                                  AND x.lead_id = c.lead_id
       GROUP BY c.workflow_id, c.call_id""",
    "CREATE UNIQUE INDEX temp.ux_call_facts ON call_facts(workflow_id, call_id)",
    """CREATE TEMP TABLE call_full AS
       SELECT f.workflow_id AS workflow_id, f.call_id AS call_id, f.lead_id AS lead_id,
              f.init_us AS init_us, f.connected AS connected, f.tfailed AS tfailed,
              d.code AS disp_code,
              COALESCE(dc.is_terminal, 0) AS is_terminal, COALESCE(dc.is_success, 0) AS is_success
       FROM call_facts f
       LEFT JOIN call_disp d ON d.workflow_id = f.workflow_id AND d.call_id = f.call_id
       LEFT JOIN dispositions dc ON dc.workflow_id = f.workflow_id AND dc.code = d.code""",
    "CREATE INDEX temp.ix_call_full_lead ON call_full(lead_id)",
    # 6. per-lead facts. Bucket = first matching rule; no calls => yet_to_reach_out.
    """CREATE TEMP TABLE lead_facts AS
       SELECT l.id AS lead_id, l.workflow_id AS workflow_id, l.batch_id AS batch_id,
              l.external_ref AS external_ref,
              COUNT(c.call_id) AS total_calls,
              COALESCE(SUM(c.connected), 0) AS connected_calls,
              COALESCE(MAX(c.is_success), 0) AS ever_success,
              CASE WHEN COUNT(c.call_id) = 0 THEN 'yet_to_reach_out'
                   WHEN MAX(c.is_terminal) = 1 THEN 'completed'
                   WHEN MIN(c.tfailed) = 1 THEN 'in_progress_telephony_failed'
                   ELSE 'in_progress_dialed' END AS bucket,
              MAX(c.init_us) AS last_call_us
       FROM leads l LEFT JOIN call_full c ON c.lead_id = l.id
       GROUP BY l.id""",
    "CREATE UNIQUE INDEX temp.ux_lead_facts ON lead_facts(lead_id)",
    # current disposition: effective disposition of the most recent call that has one
    """CREATE TEMP TABLE lead_current AS
       SELECT lead_id, disp_code FROM (
         SELECT lead_id, disp_code,
                ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY init_us DESC, call_id DESC) AS rn
         FROM call_full WHERE disp_code IS NOT NULL) WHERE rn = 1""",
    "CREATE UNIQUE INDEX temp.ux_lead_current ON lead_current(lead_id)",
]

_STEPS = _LINK_STEPS + _CALL_STEPS

_TABLES = ["lead_current", "lead_facts", "call_full", "call_facts", "call_disp",
           "calls", "call_lead", "ev_lead"]


def drop(conn: sqlite3.Connection) -> None:
    for t in _TABLES:
        conn.execute(f"DROP TABLE IF EXISTS temp.{t}")


def build(conn: sqlite3.Connection) -> None:
    drop(conn)
    for sql in _STEPS:
        conn.execute(sql)


def orphan_count(conn: sqlite3.Connection) -> int:
    """Events (ledger rows) that cannot currently be linked to any lead.
    Only needs the linking steps, so it skips the (heavier) call/lead aggregation."""
    drop(conn)
    for sql in _LINK_STEPS:
        conn.execute(sql)
    try:
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        linked = conn.execute("SELECT COUNT(DISTINCT id) FROM ev_lead").fetchone()[0]
        return total - linked
    finally:
        drop(conn)
