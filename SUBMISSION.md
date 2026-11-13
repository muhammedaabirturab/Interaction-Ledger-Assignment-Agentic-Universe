# Submission

## Candidate
- **Name:** Muhammed Aabir Turabi (Global Academy of Technology, Bangalore — final-year CSE)
- **Assignment chosen:** A (Data) — Interaction Ledger & Reporting Pipeline
- **Time spent (honest estimate):** `<<< fill in your own number >>>`

## How to run
Full detail and troubleshooting: [`solution/README.md`](solution/README.md).
```bash
python -m venv .venv && source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r solution/requirements.txt                  # only third-party dep: cryptography (+pytest)
python -m solution gen-keys > .env                        # two different secrets; .env is git-ignored

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/
pytest                                                    # 145 tests, ~30 s
```
Use a fresh DB per dataset (`rm ledger.db` or `--db other.db`): a ledger is cumulative by design.

## Design
**Stack:** Python 3.11, raw `sqlite3` (WAL), `cryptography` for AES-256-GCM, stdlib `hmac`/`csv`/`json`.
No ORM or pandas: the work is bulk inserts plus set-based SQL, and 600k events should not sit in memory.

**Flow.** `ingest` discovers drops, and for each file (leads first, then events) opens one transaction:
check the file's SHA-256 key, parse, write rows + the `source_files` marker, commit.
`report` is a pure function of the DB: SQL builds temp tables for linked events → calls → effective
dispositions → per-lead facts, and Python only formats the five files.

**Schema** (details in `solution/README.md`):
- `workflows`, `dispositions` – schemas/catalogue loaded from `assignment-a-data/schemas`.
- `source_files` – one row per ingested file with its counters; `UNIQUE(kind, workflow_id, sha256)` is the idempotency key.
- `batches` (PK workflow+file stem) → `leads` (UNIQUE `(workflow_id, phone_hash)`, FK to batch, `external_ref`)
  → `lead_fields` (EAV; each row holds either `value_plain` or `value_enc`, enforced by CHECK; `phone` and
  `customer_name` are always `value_enc`).
- `lead_rejections` – `(file, row_number, reason)`, no values.
- `events` – the ledger. `UNIQUE(event_id)`, no PII (phone only as HMAC), `UPDATE`/`DELETE` blocked by triggers.

**Key idea – links are derived, not stored.** `events` has no `lead_id`. An event's lead is the lead whose
`(workflow_id, phone_hash)` matches; a phone-less override takes the lead of its call's other events. So
when drop 2 brings the lead, previously orphaned events link on the next report with *no* update to the
ledger and no reconciliation job; "drop_1 then drop_2" and "everything at once" are equal by construction
(and tested). Orphans stay in `events`; they are only excluded from reports and counted.

**PII.** `phone_hash = HMAC-SHA256(hmac_key, "phone:v1:+91…")` for matching; name and phone are AES-256-GCM
encrypted (random nonce, AAD = `lead:<id>:<field>`) because `phone_masked` needs the last four digits.
Separate keys from env/`.env`; a key fingerprint in `meta` blocks running against a DB made with other keys.
Tests scan the raw `.db`/`-wal` bytes for every generated name/phone.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Idempotency key = SHA-256 of file bytes (per kind + workflow) | Re-ingest is a no-op even if the folder is renamed/copied; changed bytes are treated as new data | Key by path (breaks on rename; stale on edit) |
| Event→lead link derived at query time | No mutable reconciliation state; order-independent | Store `lead_id` and back-fill on lead arrival (needs UPDATE on an append-only table) |
| A call's lead = lead of its earliest linked `call.initiated`; other events count only if they link to that same lead | Cross-lead call_id collisions can't corrupt a call | Group by `call_id` only |
| Phone-less events (any type) link via `call_id`; events with a present-but-invalid phone are orphans | Spec says events link via normalised phone; overrides are the stated exception | Fallback to call for invalid-phone events too |
| Override beats any set regardless of time; latest by `occurred_at`, ties by `event_id` | Spec: "latest override, else latest set" | Latest of both |
| Unknown-code disposition events are ignored for the *effective* disposition (a bad override doesn't hide a good set) | "count it and ignore it" | Treat as clearing the disposition |
| Orphan / unknown-disposition counts computed from final state, orphan checked before unknown | Spec step order + "reflect the final state" | Count at ingest time |
| `malformed` also covers: non-object JSON, missing `event_type`, unparseable `occurred_at` | Such an event cannot be ordered or interpreted; spec only names bad JSON/no `event_id` | New counter (the JSON contract has no slot for it) |
| Unknown `event_type` is stored and inert (not malformed/orphan unless unlinkable) | Never lose a valid event | Count as malformed |
| Phone is always required for a lead even if a schema marks it optional | Identity is `(workflow, phone)` | Reject only if `required` |
| Required non-phone field present-but-invalid → reject `invalid_value:<field>` | Spec defines no reason; unreachable with shipped schemas | Treat as null (would create ref-less leads) |
| Duplicate header after normalisation: first matching column wins | Deterministic | Error / last wins |
| Decimal rounding ROUND_HALF_UP, comma = thousands separator anywhere between digits | Currency convention; lenient parse | Banker's rounding |
| Batches with zero leads still appear in `batch_summary` (zeros) | Ops can see an all-rejected batch | Omit |
| `report` needs the encryption key | `phone_masked` is derived by decrypting, not by storing last-4 in clear | Store `phone_last4` plaintext |
| Rejection reason keys in `data_quality.json` listed only when non-zero, sorted | Matches a straightforward counter | Always list all |
| CSV must be UTF-8 (BOM ok); otherwise the file fails and nothing from it is ingested | Silent mojibake would corrupt names | Guess encodings |
| An unknown `leads/<workflow>/` folder aborts the run before anything is ingested | Avoids partial, order-dependent state | Skip with warning |

## Failure modes considered
- **Dirty rows:** every case in the spec's "messy" list is covered by rule tests and by differential tests on 14 seeds.
- **Crash / kill mid-run:** per-file transactions; tested with an injected exception after the inserts, and with a real `kill` of the CLI process; re-run converges to the same reports and `PRAGMA integrity_check` is `ok`.
- **Re-runs:** identical files skipped (no audit row either); duplicate events across drops dropped by `event_id`; reports regenerate byte-identically.
- **Wrong/missing keys, tampered DB:** exit 2 on missing/invalid keys or key mismatch; GCM tag failure → exit 1.
- **Bad files:** empty files, non-UTF-8 CSV (rejected, nothing ingested), binary garbage in JSONL (counted malformed).
- **Not handled / known gaps:** one writer at a time (SQLite; others wait up to 30 s); a *changed* file with the same name is simply ingested as new data (no "amended file" workflow); event `payload` is stored verbatim so free-text PII there would be stored; SQLite file itself is not encrypted; no Postgres path; `ingest_runs` rows left `running` mean a killed process.

## Testing
`pytest` → **145 passed** (~30 s, Python 3.11.9, also re-run in a fresh venv built from `solution/requirements.txt`).
- `tests/oracle.py`: independent re-implementation of the spec (shares no code, different parsing mechanics). Differential tests compare all five files byte-for-byte on seeds 42, 7, 2026, 31337 (incremental), 42 and 7 (one-shot == incremental); outside pytest I also ran 10 more seeds (1, 2, 3, 99, 12345, 777, 2024, 5, 8675309, 0) and the `--scale large` seed 42 — all identical.
- Rule tests: phone/date/decimal/header/timestamp tables, rejection ordering, blank-row numbering, merge semantics, overrides, buckets, IST rollover, orphans-before-leads, unknown dispositions, malformed/duplicate counters.
- Robustness/security/CLI as listed above.
- I mutation-checked key rules (override precedence, `any` vs `all` telephony failure, the `91`/`0` phone prefixes); one surviving mutant exposed a missing test, which I added.
- **Not tested:** Postgres; concurrent writers; multi-GB inputs.

## Results
Windows 11 laptop, Python 3.11.9, SQLite 3.45.1, `--seed 42 --scale large` (143,990 lead rows, 632,602 event lines, 144 MB), fresh DB:

| Step | Wall time | Peak RSS |
|---|---|---|
| `ingest drop_1` (106,748 lead rows + 308,063 events) | 20.3 s | 229 MB |
| `ingest drop_2` (37,242 lead rows + 324,539 events) | 27.8 s | 336 MB |
| `report` (all five files) | 12.9 s | 368 MB |
| **Total** | **≈ 61 s** (< 2 min target) | |
| re-ingest both drops (all skipped; dominated by hashing 144 MB) | 5.1 s | |

DB size 161 MB. First attempt was 73 s / 141 s / 27 s: profiling showed the two secondary indexes on `events` and a full orphan recount per run dominated; both were removed/made cheap (a recount now only runs when something new was ingested).
Things in the data worth knowing: after drop 1 there were 87,444 orphan events and 34,424 after drop 2 (53,020 resolved by late-arriving leads; the rest belong to rejected leads, unparseable phones and never-uploaded numbers); 1,005 unknown-disposition events; 631 malformed lines; 18,490 duplicate event lines (cross-drop included).

## If I had another day
1. Postgres backend + migrations (Alembic) and a concurrency test with two writers.
2. "Amended file" policy (same name, new bytes): diff-and-apply instead of treating as new data.
3. Property-based tests (hypothesis) for the normalisers and for merge order-independence.
4. Production notes for 100k leads/day: a scheduler running `ingest` per delivered drop (idempotent, so safe to retry), alerting on non-zero exit and on `ingest_runs.status='running'` older than N minutes, `report` per morning, backfills = re-ingest into a scratch DB and diff reports, and key rotation by re-encrypting `lead_fields` (the HMAC key is the harder one: rotation means rehashing all phones, which needs the encrypted copy).
5. Incremental report maintenance (aggregate only dates/leads touched by new events) instead of a full 13 s recompute.
