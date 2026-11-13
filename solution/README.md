# Assignment A solution: Interaction Ledger & Reporting Pipeline

**Start here.** This file is the technical guide to the solution. Where things are:

| File | What it is |
|---|---|
| `README.md` (repo root) | The original assignment brief from Agentic Universe, deliberately left unmodified. |
| `assignment-a-data/README.md` | The Assignment A specification (the business rules this solution implements). |
| **`solution/README.md`** (this file) | How to install, run, test and understand the solution. |
| [`SUBMISSION.md`](../SUBMISSION.md) | Design summary, every decision/assumption the spec left open, failure modes, test results, performance. |
| [`AI_USAGE.md`](../AI_USAGE.md) | Disclosure of how AI was used and how the output was verified. |

## What it does

A pipeline that ingests messy **lead CSV** uploads and **call-event JSONL** drops into SQLite, keeps customer PII
out of plaintext, maintains an **append-only interaction ledger**, and produces the five client reports
(`batch_summary.csv`, `daily_overview.csv`, `lead_report.csv`, `rejections.csv`, `data_quality.json`).
It is **idempotent** (re-ingesting a drop changes nothing) and **incremental** (drop 1 then drop 2 gives exactly the
reports of ingesting everything at once). Events that arrive before their lead are kept and attach automatically
when the lead arrives.

## Prerequisites

* **Python 3.11 or newer** (`datetime.fromisoformat` must accept `Z` timestamps) and `pip`.
* SQLite 3.25+ (window functions) — bundled with Python.
* One third-party runtime dependency: `cryptography` (AES-256-GCM). `pytest` for tests. Pinned ranges and the tested
  versions are in `solution/requirements.txt`. No Docker or Postgres is needed (SQLite is used).

Verified on Windows 11 with Python 3.11.9 / SQLite 3.45.1 / cryptography 50.0.2 / pytest 9.2.0. The Linux/macOS commands
below use only standard tooling but were **not** run on those systems.

## Setup and run (from a clean clone, repo root)

<details open><summary><b>Windows PowerShell</b></summary>

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1          # if blocked: Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
python -m pip install -r solution/requirements.txt

# keys: two different secrets, written to the git-ignored .env  (-Encoding ascii matters, see Troubleshooting)
python -m solution gen-keys | Out-File -Encoding ascii .env

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/
```
</details>

<details open><summary><b>Linux / macOS (bash)</b></summary>

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r solution/requirements.txt

python -m solution gen-keys > .env     # two different secrets, written to the git-ignored .env

python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/
```
</details>

Notes:

* Run everything from the repository root. `.env` is read from the current directory; real environment
  variables take precedence. Instead of `.env` you can set `LEDGER_ENCRYPTION_KEY` and `LEDGER_HMAC_KEY` directly
  (PowerShell: `$env:LEDGER_HMAC_KEY = "..."`). `.env.example` shows the format with placeholders only.
* `python -m solution ingest data` (the parent folder) ingests `drop_1` then `drop_2` in one command — the
  result is identical to two separate commands.
* The database is `./ledger.db` (override with `--db PATH` or `LEDGER_DB`). **A ledger is cumulative**: use a new
  `--db` (or delete `ledger.db`) before ingesting a *different* dataset or seed.
* `ledger.db`, `.env`, `data/` and `reports/` are git-ignored. Never commit them.

### Commands

| Command | Purpose |
|---|---|
| `python -m solution ingest <path> [--schemas DIR]` | `<path>` is one drop (has `leads/` and/or `events/`) or a folder of drops (ingested in natural name order). Prints a count summary, never row values. |
| `python -m solution report --out <dir>` | Writes the five files from everything ingested so far (creates `<dir>`). Needs the DB and both keys. |
| `python -m solution init-db` | Creates the DB and registers the key fingerprints. |
| `python -m solution gen-keys` | Prints two fresh random keys (`LEDGER_ENCRYPTION_KEY=…`, `LEDGER_HMAC_KEY=…`). |
| top-level options (go **before** the sub-command) | `--db PATH` (default `$LEDGER_DB` or `./ledger.db`), `-v` (log each file), `--version`. Example: `python -m solution --db other.db -v ingest data`. |

Exit codes: `0` ok · `1` runtime/ingest failure (corrupt file, stored data fails authentication, I/O) · `2` configuration
or usage error (missing/invalid/identical keys, keys not matching the DB, bad path, unknown workflow folder, no DB for
`report`).

## Expected output

After the commands above with seed 42 (`--scale small`), `reports/` contains:

| File | Content | Rows (seed 42) |
|---|---|---|
| `batch_summary.csv` | per `(workflow_id, batch_id)`: `total_leads` and the four bucket counts | 6 |
| `daily_overview.csv` | per workflow and IST date with ≥1 call: `total_calls`, `connected_calls`, `connectivity_rate` (4 d.p.), `success_leads` | 33 |
| `lead_report.csv` | one row per lead: masked phone (`+91******NNNN`), bucket, call counts, `last_call_at` (IST), current disposition, `ever_success` | 1,892 |
| `rejections.csv` | `workflow_id,batch_id,row_number,reason` — **no PII** | 108 |
| `data_quality.json` | counters (see below) | — |

`data_quality.json` for seed 42 small (any correct run of the same seed must match exactly):

```json
{"lead_rows_total": 2057, "lead_rows_rejected": 108,
 "rejections_by_reason": {"invalid_phone": 42, "missing_required:customer_name": 25, "missing_required:order_id": 13,
                          "missing_required:phone": 14, "missing_required:policy_number": 14},
 "duplicate_lead_rows_merged": 57, "field_warnings": 122, "events_total": 8745, "malformed_events": 8,
 "duplicate_events_dropped": 237, "orphan_events": 462, "unknown_disposition_events": 18}
```

(The file itself is pretty-printed.) Re-running `ingest` on the same drops prints `files ingested: 0 … skipped (already
ingested)`; running `report` again rewrites byte-identical files.

## Tests and verification

```bash
python -m pytest                      # 155 tests, about 30-90 s depending on the machine
python solution/tools/verify_seeds.py 1111 2222 3333 4444 5555 6666 7777 8888
python solution/tools/verify_seeds.py 4242 --scale large      # optional: several minutes, uses a temp folder
```

(Same commands in PowerShell and bash.) `verify_seeds.py` generates a **fresh** dataset per seed and checks that
(1) incremental ingest == one-shot ingest, (2) both equal the independent oracle `solution/tests/oracle.py`
byte-for-byte on all five files, (3) re-ingest is a no-op and (4) reports are stable. It prints `PASS`/`FAIL` per seed and exits
non-zero on any failure.

What the 155 tests cover: phone/date/decimal/header/timestamp rules; rejection order and row numbering;
merge/last-write-wins and defaults; event linking, orphans arriving before leads, effective dispositions, buckets,
IST date rollover, call attribution edge cases; idempotent re-ingest, restart, injected crash and a real process kill;
append-only triggers; empty/corrupt files; key validation and tampered ciphertext; **no plaintext PII in the database
files**; CLI exit codes and PII-free output; and differential tests against the oracle. The oracle is a second
implementation by the same author, so it is evidence of internal consistency rather than an independent authority.

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
  tools/verify_seeds.py   fresh-seed verification script
  tests/            155 tests + oracle.py (independent reference implementation)
```

```
 lead CSV    --normalise/validate--> leads + lead_fields (PII encrypted) / lead_rejections
 event JSONL --parse/dedupe-------> events (append-only; phone stored only as an HMAC)
                                       |
 report: events JOIN leads on (workflow, phone HMAC)  [phone-less disposition.override: via its call]
         -> ev_lead -> calls -> call_facts / call_disp -> lead_facts -> the 5 files
```

### Database

| Table | Role | Keys / constraints |
|---|---|---|
| `workflows`, `dispositions` | schemas + catalogue loaded from `assignment-a-data/schemas` | PK `(workflow_id, code)` |
| `ingest_runs` | audit of each run that ingested something | |
| `source_files` | one row per ingested file + its counters | **UNIQUE `(kind, workflow_id, sha256)`** = idempotency key |
| `batches` | one per `(workflow_id, batch_id = file stem)` | PK |
| `leads` | identity `(workflow_id, phone_hash)`; `external_ref` | UNIQUE identity; FK to `batches` |
| `lead_fields` | non-null field values (EAV, so any schema works) | PK `(lead_id, field_name)`; CHECK exactly one of `value_plain` / `value_enc` |
| `lead_rejections` | `(file, row_number, reason)` — no values | UNIQUE `(source_file_id, row_number)` |
| `events` | **append-only ledger** (`occurred_us` is NULL if the timestamp was unparseable) | UNIQUE `event_id`; UPDATE/DELETE blocked by triggers |

### Key design decisions (full table with alternatives in [`SUBMISSION.md`](../SUBMISSION.md))

* **Event→lead links are derived at report time, never stored.** A late-arriving lead therefore picks up its earlier
  events with no update to the ledger and no reconciliation job, and "incremental == one-shot" holds by construction.
  Orphans stay in `events`; they are only excluded from reports and counted.
* **Only `disposition.override` may link through `call_id`.** Every other event links by normalised phone; phone-less
  or invalid-phone events are orphans. A call's lead is the lead of its earliest *timed* linked `call.initiated`
  (ties: `event_id`), otherwise of its earliest linked event.
* **One transaction per input file**, keyed by the file's SHA-256, so a crash leaves a file either fully applied or not at all.
* **No secondary indexes on `events`**: reports scan the ledger once; extra indexes only slowed the append path.
* **Raw `sqlite3`, no ORM/pandas**: bulk inserts plus set-based SQL; fewer dependencies and no 600k-row in-memory frames.

### PII

Fields marked `"pii": true` (`customer_name`, `phone`) are never stored in plaintext.

* `phone_hash = HMAC-SHA256(LEDGER_HMAC_KEY, "phone:v1:" + "+91XXXXXXXXXX")` — deterministic matching key; events
  store **only** this hash.
* Name and phone are AES-256-GCM encrypted (`LEDGER_ENCRYPTION_KEY`, random nonce, associated data `lead:<id>:<field>`).
* Two independent keys; startup rejects missing, malformed, short or identical keys, and a key fingerprint stored in `meta`
  makes the program refuse to open a database created with different keys.
* Reports show only `+91******NNNN`; `rejections.csv`, logs and terminal output contain no row values.

## Known limitations

* SQLite is not encrypted at file level (column-level only); the HMAC input space (Indian mobiles) is small, so
  security rests on keeping `LEDGER_HMAC_KEY` secret. Event `payload` is stored verbatim, so free-text PII placed there by an
  upstream system would be stored.
* One writer at a time; no Postgres path; a *changed* file re-sent under the same name is ingested as new data (no amend workflow).
* Verified only on Windows 11; large-dataset timings vary a lot with machine load (see `SUBMISSION.md`).
* The `.env` loader does not strip a UTF-8 byte-order mark (see Troubleshooting).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `LEDGER_ENCRYPTION_KEY is not set` (exit 2) although `.env` exists | On Windows PowerShell 5.1, `gen-keys > .env` writes a UTF-8 BOM that the loader does not accept. Recreate it with `python -m solution gen-keys \| Out-File -Encoding ascii .env` (or save the file as plain UTF-8 **without** BOM), or set the two variables in the shell. |
| `LEDGER_ENCRYPTION_KEY is not set` (no `.env`) | Create `.env` as shown above or export both keys. |
| `configured keys do not match … this database` (exit 2) | You changed keys; use the original keys or a new `--db`. |
| `Database not found` on `report` | Run `ingest` first, or pass the same `--db` (it is a top-level option, before `report`). |
| `no schema for workflow 'x'` (exit 2) | `leads/<x>/` has no `x.json` in `assignment-a-data/schemas` (or pass `--schemas`). Nothing is ingested. |
| `not valid UTF-8` (exit 1) | The CSV is not UTF-8; re-export it as UTF-8. Nothing from that file is ingested. |
| `Database schema version … expected …` | `ledger.db` was made by an older build; delete it and re-ingest. |
| Counts look doubled after switching datasets | The ledger is cumulative; delete `ledger.db` or use a new `--db`. |
