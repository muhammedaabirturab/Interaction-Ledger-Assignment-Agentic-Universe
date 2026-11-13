# Assignment A solution: Interaction Ledger & Reporting Pipeline

A small, dependency-light pipeline that ingests messy lead CSVs and call-event JSONL "drops" into
SQLite, protects PII at rest, and produces the five client reports. All business rules follow
`assignment-a-data/README.md`; the choices the spec left open are listed in `../SUBMISSION.md`.

* Python 3.11+ (needs `datetime.fromisoformat` handling of `Z`), SQLite 3.25+ (window functions).
* One third-party dependency: `cryptography` (AES-256-GCM). No ORM, no pandas — see "Why".

## Quick start (from a clean clone)

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r solution/requirements.txt

# 1. keys (two different secrets; never commit them)
python -m solution gen-keys > .env   # writes LEDGER_ENCRYPTION_KEY=... and LEDGER_HMAC_KEY=... (see .env.example)

# 2. data (generated, git-ignored)
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# 3. run
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/

# 4. tests
pytest
```

`.env` is read from the current directory (real environment variables win). Use a fresh database
per dataset: delete `ledger.db` (or pass `--db other.db`) before ingesting a *different* seed,
because a ledger is cumulative by design.

### Commands

| Command | Purpose |
|---|---|
| `python -m solution ingest <path>` | `<path>` is one drop (`leads/`, `events/`) or a folder of drops (ingested in natural name order: `ingest data` == `drop_1` then `drop_2`). |
| `python -m solution report --out <dir>` | Writes the five files from everything ingested so far. |
| `python -m solution init-db` | Creates the DB and registers the key fingerprints. |
| `python -m solution gen-keys` | Prints two fresh random keys. |
| global flags | `--db PATH` (or `$LEDGER_DB`, default `./ledger.db`), `-v` (log each file), `--schemas DIR` on `ingest` (or `$LEDGER_SCHEMAS_DIR`). |

Exit codes: `0` ok, `1` runtime/ingest failure (corrupt file, wrong key for stored data, I/O),
`2` configuration/usage error (missing/invalid keys, bad path, unknown workflow folder, DB missing for
`report`). Output and logs contain counts and file names only, never row values.

## Architecture

```
solution/
  cli.py            argparse CLI, exit codes
  config.py         env/.env loading, key validation
  security.py       HMAC-SHA256 phone hashing, AES-256-GCM encrypt/decrypt
  db.py             DDL, connection pragmas, key-fingerprint check
  workflows.py      workflow schemas + disposition catalogue loader
  normalise.py      pure functions: header, phone, date, decimal, enum, timestamp
  lead_ingest.py    CSV -> validated rows -> create/merge leads
  event_ingest.py   JSONL -> ledger rows (malformed / duplicate handling)
  ingest.py         drop discovery, file-level idempotency, one transaction per file
  reporting/
    derive.py       SQL that derives links, calls, dispositions, buckets (temp tables)
    writers.py      the five report files
  tests/            145 tests incl. an independent oracle (tests/oracle.py)
```

Data flow:

```
 lead CSV  --normalise/validate--> leads + lead_fields (PII encrypted) / lead_rejections
 event JSONL --parse/dedupe-----> events (append-only; phone stored only as HMAC)
                                     |
   report: events JOIN leads (workflow + phone HMAC, or call_id for phone-less overrides)
           -> ev_lead -> calls -> call_facts/call_disp -> lead_facts -> 5 report files
```

## Database design

| Table | Role | Keys / constraints |
|---|---|---|
| `workflows`, `dispositions` | schema + catalogue loaded from `assignment-a-data/schemas` | PK `(workflow_id, code)` |
| `ingest_runs` | audit of each run that changed something | |
| `source_files` | one row per ingested file + its counters | **UNIQUE `(kind, workflow_id, sha256)`** = idempotency key |
| `batches` | one per `(workflow_id, batch_id=file stem)` | PK; FK to first file |
| `leads` | identity `(workflow_id, phone_hash)` | UNIQUE identity; FK to `batches`; `external_ref` (reference field, last-write-wins) |
| `lead_fields` | non-null field values per lead (EAV, so any schema works) | PK `(lead_id, field_name)`; CHECK exactly one of `value_plain` / `value_enc` |
| `lead_rejections` | `(file, row_number, reason)` — no values | UNIQUE `(source_file_id, row_number)` |
| `events` | **append-only ledger** | UNIQUE `event_id`; `UPDATE`/`DELETE` blocked by triggers |

Key decisions:

* **Linkage is derived, not stored.** An event has no `lead_id` column. The event's lead is whatever
  `(workflow_id, phone_hash)` currently matches (or, for phone-less overrides, the lead of its call's
  other events). A lead that arrives later therefore resolves its orphans automatically, the ledger is never
  updated, and "incremental == one-shot" holds by construction rather than by a reconciliation job.
  Orphans are *retained* in `events`; they are simply excluded from reports and counted.
* **No secondary indexes on `events`.** Reports scan the ledger once and probe `leads`'s unique index;
  extra indexes only slowed the 600k-row append path (measured: removing them plus a cheaper orphan recount took `ingest drop_1` on the large set from ~74 s to ~20 s).
* **File = transaction.** A file's rows and its `source_files` marker commit together, so a crash
  (tested with a real `kill`) leaves the file wholly applied or not at all, and re-running retries it.
* **EAV `lead_fields`** keeps one schema for every workflow and makes the "overwrite only non-null
  fields" merge a single upsert per field.

## PII

Fields with `"pii": true` (`customer_name`, `phone`) are never stored in plaintext.

* `phone_hash = HMAC-SHA256(LEDGER_HMAC_KEY, "phone:v1:" + "+91XXXXXXXXXX")` — deterministic, so leads and
  events match without the number being stored. Events keep *only* this hash (invalid/absent phones are
  recorded as a state, nothing else).
* Reversible values (name, phone, needed for `phone_masked`) are AES-256-GCM encrypted with
  `LEDGER_ENCRYPTION_KEY`, random 96-bit nonce, and associated data `lead:<id>:<field>` so a ciphertext moved
  to another row fails authentication.
* Two independent keys; startup rejects missing, malformed, short or identical keys. A key fingerprint
  stored in `meta` makes the pipeline refuse to run against a DB created with different keys (otherwise
  hashes would silently stop matching).
* Reports: only `+91******NNNN`. `rejections.csv` has no values. Logs/terminal: counts and file names.
* Tests scan the raw `.db`/`-wal` bytes for every name and phone in the generated data (with a positive control).

Honest limits: SQLite itself is not encrypted (column-level only); the HMAC space of Indian mobiles is
small, so security rests entirely on keeping `LEDGER_HMAC_KEY` secret; event `payload` is stored verbatim,
so free-text PII placed in a payload by an upstream system would be stored.

## Idempotency & incremental processing

* Each input file is keyed by `(kind, workflow, SHA-256 of bytes)`; already-seen files are skipped, so
  re-running `ingest` (or ingesting the same event file twice, or in two drops) changes nothing — not even
  an audit row.
* Event dedupe is by `event_id` across all drops (first occurrence wins); duplicate lead rows are merged
  deterministically in drop/file/row order.
* Reports are pure functions of the DB, recomputed in SQL each time; they are byte-identical on an unchanged DB.
* Drop-2 does not reprocess drop-1's files; only the report aggregation is recomputed (≈13 s on the large set).

## Testing

```bash
pytest                    # 145 tests, ~30 s

```

* `tests/oracle.py` is an independent re-implementation of the spec (no shared code). Differential tests
  compare all five report files byte-for-byte against it on 4 seeds in CI plus the large dataset, and
  incremental vs one-shot ingest.
* Rule tests: phone/date/decimal/header/timestamp edge cases, rejection ordering, blank-row numbering,
  merge semantics, overrides, buckets, IST date rollover, orphans arriving first, unknown dispositions.
* Robustness: re-ingest, restart, simulated crash mid-file, real `kill`, append-only triggers, empty/corrupt
  files, wrong keys, tampered ciphertext, reversed 3000-call event stream, CLI exit codes.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `LEDGER_ENCRYPTION_KEY is not set` (exit 2) | create `.env` from `.env.example` or export both keys (`python -m solution gen-keys`). |
| `configured keys do not match ... this database` | you changed keys; use the original keys or a new `--db`. |
| `Database not found` on `report` | run `ingest` first, or pass the same `--db`. |
| `no schema for workflow 'x'` | the `leads/<x>/` folder has no `x.json` in `assignment-a-data/schemas`. |
| `not valid UTF-8` | the CSV is not UTF-8; re-export it as UTF-8 (nothing from that file is ingested). |
| Numbers look doubled after switching datasets | the ledger is cumulative; delete `ledger.db` or use `--db`. |

## Why these choices

* **Raw `sqlite3` over SQLAlchemy/pandas:** the workload is bulk insert + set-based SQL; an ORM adds
  overhead and indirection, and pandas would hold 600k events in memory. Fewer dependencies, faster, easier to audit.
* **Derived links over a reconciliation job:** removes a whole class of ordering bugs (see above).
* **SQL aggregation:** window functions pick the effective disposition, earliest `call.initiated`, and the
  latest call deterministically (ties broken by event/call id).
