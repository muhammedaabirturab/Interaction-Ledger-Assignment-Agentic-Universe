# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

> **How to read this document.** It was drafted from the full record of the working session, and every
> statement below is limited to what that record shows. "**The candidate**" is Muhammed Aabir Turabi;
> "**Claude**" is the AI. Where the record does not show something (for example how closely the candidate
> read a particular file), this document does not claim it.

## Tools used

- **Claude Code** (Anthropic's coding agent) running in the Claude desktop app, model **Claude Sonnet 5.5**.
  Claude could read and write files, run shell commands (Bash/PowerShell), run Python/pytest, and use git locally.
- No other AI tool (Copilot, ChatGPT, Cursor, ...) appears in the session record. This document covers that
  session only.

## What I used them for

**In plain terms: Claude wrote essentially all of the source code, the tests, and the documentation text in this
repository. The candidate specified the task, made the decisions listed in the next section, reviewed the results and
supplied the review findings that led to three rounds of fixes.** Nothing here should be read as the AI having
worked unsupervised: each phase below was started by a candidate prompt and ended with the candidate looking at
the result.

| Area | What Claude did |
|---|---|
| Reading the spec | Read the assignment README, `docs/GLOSSARY.md`, both lead schemas, `dispositions.csv` and the data generator, and wrote the requirement list and design from them. |
| Architecture / design | Proposed the design: raw `sqlite3`; an append-only `events` ledger with **derived** (not stored) event-to-lead links; one transaction per input file with a SHA-256 idempotency key; HMAC phone matching plus AES-256-GCM for names/phones; reports computed in SQL. The candidate's brief asked it to weigh alternatives; the reasoning is in `solution/README.md` and `SUBMISSION.md`. |
| Implementation | All modules under `solution/` (CLI, config, security, db, normalisation, lead/event ingest, derived-facts SQL, report writers). |
| Testing | Wrote the tests, the independent oracle (`solution/tests/oracle.py`), and `solution/tools/verify_seeds.py`; ran them and measured performance. |
| Debugging | Profiled the first slow large-dataset run, found the causes and fixed them; fixed the defects found in review (below). |
| Documentation | Wrote `solution/README.md`, `SUBMISSION.md`, `.env.example`, `.gitignore` additions and this file. |
| Git | Created the branch and local commit and pushed it when asked (see below). |

## Key tasks given to the AI (paraphrased from the session record)

1. **The brief.** A long specification from the candidate: act as a team of architects/engineers/QA/reviewers; understand the
   whole spec first; evaluate alternatives; prefer Python 3.11 + SQLite with minimal dependencies; no hard-coded
   outputs; PII not stored in plaintext (keyed HMAC for matching, authenticated encryption, separate keys, secrets
   from the environment); idempotent and incremental ingestion; five exact report files; a CLI; unit,
   integration, regression and adversarial tests; measure large-dataset performance; documentation; an independent
   self-review loop; and **do not push anything without explicit permission**. The brief also said the original
   assignment spec wins over the brief if they disagree.
2. **Locating the assignment.** Claude could not find the assignment files on the machine. The candidate then supplied
   the folder, their own (empty) private repository URL, and finally the assignment repository URL
   (`AryamannNingombam/sde-assignment-junior-2026`), which Claude cloned and read.
3. **"Commit the working tree"**, then **"push it to my private repo"** and add the candidate (by e-mail) as a
   collaborator. Claude pushed; it could not add the collaborator (no GitHub CLI or login) and told the candidate how to do it by
   hand. It also noticed the repository was publicly readable at that moment and told the candidate to make it private.
4. **Review round 2.** The candidate reviewed the implementation and sent eight review points, three of them
   specific technical findings (phone-less event linking, `call_lead`/`MIN(lead_id)`, malformed-event
   classification). Claude was told to inspect the source, fix confirmed defects, add regression tests, run everything and
   report evidence.
5. **Review round 3.** The candidate identified one remaining edge case in `call_lead` (an untimed `call.initiated`
   being prioritised) and stated the intended rule and the regression test to add.
6. **Final documentation pass** (this one). The candidate had earlier said they would write this file personally, then
   asked Claude to draft it from the record; the candidate should read and correct it before submitting.

## What I wrote or decided myself

The following are candidate actions or decisions that the record shows; they are *not* AI choices:

- **Chose Assignment A**, defined the task, the quality bar and the working rules (listed above), and chose the
  preferred stack and the "no frontend", "no hard-coded values", "don't push without asking" constraints.
- **Directed the process**: required reading the spec before coding, a self-review loop, real execution of tests,
  and honest reporting; made Claude look for the assignment repository rather than guess its contents.
- **Accepted the branch name** (`feat/interaction-ledger-pipeline`) when the app asked, and **authorised** the commit and the push,
  each by an explicit message.
- **Performed the code review that found the three round-2 defects and the round-3 edge case** (below), and specified
  the correct behaviour for each. Those were the candidate's findings; Claude's part was to verify them against
  the code, fix them and test them.
- **Set the documentation rules** for the final pass: no commit, no push, no visibility change, no changes to
  original assignment files, and no code changes unless a documentation issue exposed a real defect.
- Stays responsible for everything submitted, including the interview walk-through.

The core design (derived links, per-file transactions, EAV `lead_fields`, no ORM) was proposed by Claude. The
record shows no instruction from the candidate changing that design; the candidate's design-level interventions were the
review findings below.

**Suggestions accepted as generated:** the architecture above; the module layout; the PII scheme; the oracle-based
differential testing approach; the report SQL (after the fixes below).

**Suggestions Claude deliberately did not follow from the brief**: the brief offered a sample directory layout
(`ingestion/`, `processing/`, `models.py`, ...) and optional SQLAlchemy/pandas/extra docs (`ARCHITECTURE.md`, `SECURITY.md`...). Claude
chose a flatter layout, raw `sqlite3` and no extra documents, explaining why in the README. It also left the original
root `README.md` untouched, because the assignment says to keep provided files unmodified; the solution is
documented in `solution/README.md` and `SUBMISSION.md`.

## Where the AI was wrong or unhelpful

All of these are real events from the session:

1. **Event linking (found by the candidate's review).** Claude linked *every* phone-less event through `call_id`;
   the spec allows that only for `disposition.override`. Other phone-less events must stay orphans. Fixed in
   `derive.py`; regression test `test_phone_less_events_other_than_override_stay_orphans`.
2. **Call attribution (found by the candidate's review).** The SQL picked `MIN(lead_id)` across a call's events to link
   overrides, while calls were attributed to the lead of the earliest `call.initiated` — two different rules that
   could split one call between two leads, and `SUBMISSION.md` described only the second. Replaced by one rule;
   regression tests with conflicting leads and `call_id` collisions across workflows.
3. **Malformed events (found by the candidate's review).** Claude had also counted a missing `event_type` or an
   unparseable `occurred_at` as "malformed". The spec defines malformed as invalid JSON or no `event_id`, so those extras could shift the
   expected `malformed_events` / `orphan_events` / `duplicate_events_dropped` counts. Now exactly the spec definition; such events are stored and
   handled normally (an untimed event simply cannot create or date a call).
4. **Untimed `call.initiated` priority (found by the candidate's review, round 3).** The fix for (2) still ranked an untimed initiated
   above timed events of another lead. Corrected, with a regression test that fails on the previous SQL.
5. **The AI's own oracle shared the misreadings in (1)–(3).** The oracle was meant to be independent but was written
   by the same AI with the same reading of the spec, so it agreed with the buggy pipeline. It had to be corrected as
   well. This is an inherent limit of AI-written verification.
6. **Slow first large run.** 73 s + 141 s + 27 s. Profiling showed two unneeded secondary indexes on `events` and a
   full orphan recount on every run. Both were fixed (the later measured totals are in `SUBMISSION.md`).
7. **Weak tests / over-claims in its own work.** Early tests included assertions that could not fail (a tautological
   "positive control" in the PII scan, an `or`-assertion, a leftover `if False`); these were rewritten. A mutation spot-check
   (deliberately breaking the `91`/`0` phone-prefix rules) left one mutant alive, exposing a missing test, which was added. Two
   documentation claims were also wrong and were corrected: a "2.5x faster" figure that conflated two changes, and a
   statement that the machine was slower "on a pure-Python loop", which had not actually been measured.
8. **A git mistake.** While copying the assignment files into the candidate's repository folder, Claude copied the
   assignment's `.git` directory over the candidate's empty clone. It noticed this from `git log`/`git remote -v`. The
   consequence is that the repository history contains the assignment's initial commit and `origin` still points to the
   assignment's repository; the candidate's repository is the extra remote `mine`. Nothing was pushed to the assignment's repository.
9. **Windows PowerShell key file (found in this documentation pass).** `python -m solution gen-keys > .env` in
   Windows PowerShell 5.1 writes a UTF-8 byte-order mark that the `.env` loader does not handle, so the first key is
   reported as unset. This is a code defect, not fixed in this pass (instructions: report, do not change code); the README documents the
   working command. See the report accompanying this change.

## How I verified AI-generated code

Everything below was executed in the session; numbers are the ones observed.

- **Test suite:** 145 tests at first completion, 151 after review round 2, **155** after round 3 (all passing at each point;
  final composition in `SUBMISSION.md`).
- **Independent oracle** (`solution/tests/oracle.py`): a separate in-memory implementation of the spec; reports are
  compared byte-for-byte. Used in pytest and by `solution/tools/verify_seeds.py`, which also checks incremental ==
  one-shot ingest and that re-ingest changes nothing. Seeds run include 42, 7, 2026, 31337, 1, 2, 3, 99, 12345, 777,
  2024, 5, 8675309, 0, 101–909 (step 101), 31415, 1111–8888 (step 1111) at small scale and 42 and 4242 at large scale.
  Limit: the oracle is also AI-written (see item 5 above).
- **Regression proof:** each review-round test was also run against the previous implementation to confirm it fails
  there (four of six in round 2; the targeted test in round 3; the others are guards).
- **Crash safety:** an injected failure mid-file and a real process `kill` followed by a re-run, compared with a clean run, plus
  `PRAGMA integrity_check`.
- **PII:** a test scans the raw database and WAL bytes for every generated name and phone (with a positive control).
- **Performance:** measured on the large dataset in two sessions with very different machine speed; both are reported.
- **Clean environment:** the project was copied to a fresh folder, a new virtual environment was built from
  `solution/requirements.txt`, and the documented commands and tests were run.
- **Human review:** the candidate's review rounds above. Beyond those, this document does not claim any further manual
  verification by the candidate.

## Limits of the AI-generated output (and what to treat with caution)

- The AI made many small spec-interpretation decisions where the spec is ambiguous (listed with reasons in
  `SUBMISSION.md`). They are reasonable and tested, but they are the AI's calls unless the candidate overrode them.
- It cannot know the hidden expected outputs; correctness is evidenced by an independent re-implementation, not by the
  grader's reference.
- Verified on **Windows 11 / Python 3.11 only**; the Linux/macOS instructions were not run.
- AI self-assessment is biased toward its own work; the defects above were found by human review, not by the AI's own
  checks.
