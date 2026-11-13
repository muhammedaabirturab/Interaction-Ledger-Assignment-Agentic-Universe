"""Lead CSV ingestion: header mapping, validation, rejection, merge, persistence.

Everything that interprets a row (``parse_lead_csv``/``validate_row``) is pure and
database-free so it can be unit-tested; ``persist_leads`` does the DB work.
"""
from __future__ import annotations

import csv
import io
import sqlite3
from dataclasses import dataclass, field
from typing import Iterator

from solution.normalise import (normalise_header, normalise_phone, parse_date,
                                parse_decimal, parse_enum)
from solution.security import Crypto, pii_aad
from solution.workflows import Field, WorkflowSchema

CHUNK = 500


@dataclass
class ParsedRow:
    row_number: int
    reason: str | None                 # rejection reason, or None if accepted
    values: dict[str, str] = field(default_factory=dict)  # non-null normalised values
    warnings: int = 0


@dataclass
class LeadStats:
    rows_total: int = 0
    rows_rejected: int = 0
    duplicates_merged: int = 0
    field_warnings: int = 0
    new_leads: int = 0


# ------------------------------------------------------------------ parsing
def map_headers(header: list[str], schema: WorkflowSchema) -> dict[str, int]:
    """field name -> column index (first matching column wins); unmatched columns ignored."""
    normalised = [normalise_header(h) for h in header]
    mapping: dict[str, int] = {}
    for f in schema.fields:
        for idx, key in enumerate(normalised):
            if key in f.header_keys:
                mapping[f.name] = idx
                break
    return mapping


def iter_data_rows(text: str) -> tuple[list[str] | None, Iterator[tuple[int, list[str]]]]:
    """Return (header, iterator of (row_number, cells)). Blank rows are skipped and
    do not consume a row number."""
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)

    def rows() -> Iterator[tuple[int, list[str]]]:
        n = 0
        for cells in reader:
            if all(not c.strip() for c in cells):
                continue
            n += 1
            yield n, cells
    return header, rows()


def _convert(f: Field, raw: str) -> str | None:
    """Normalise a non-empty trimmed value; None = present but invalid."""
    if f.type == "string":
        return raw
    if f.type == "phone":
        return normalise_phone(raw)
    if f.type == "date":
        d = parse_date(raw)
        return d.isoformat() if d else None
    if f.type == "decimal":
        d = parse_decimal(raw)
        return str(d) if d is not None else None
    if f.type == "enum":
        return parse_enum(raw, list(f.values))
    raise ValueError(f"Unsupported field type {f.type!r} in schema")


def validate_row(schema: WorkflowSchema, mapping: dict[str, int], cells: list[str],
                 row_number: int) -> ParsedRow:
    """Walk the fields in schema order; the first failure is the row's only reason."""
    values: dict[str, str] = {}
    warnings = 0
    for f in schema.fields:
        idx = mapping.get(f.name)
        raw = cells[idx].strip() if idx is not None and idx < len(cells) else ""
        if raw == "":
            if f.required or f.type == "phone":   # phone is the lead identity: always needed
                return ParsedRow(row_number, f"missing_required:{f.name}")
            continue
        converted = _convert(f, raw)
        if converted is None:
            if f.type == "phone":
                return ParsedRow(row_number, "invalid_phone")
            if f.required:
                return ParsedRow(row_number, f"invalid_value:{f.name}")
            warnings += 1
            continue
        values[f.name] = converted
    return ParsedRow(row_number, None, values, warnings)


def parse_lead_csv(text: str, schema: WorkflowSchema) -> Iterator[ParsedRow]:
    header, rows = iter_data_rows(text)
    if header is None:
        return
    mapping = map_headers(header, schema)
    for row_number, cells in rows:
        yield validate_row(schema, mapping, cells, row_number)


# -------------------------------------------------------------- persistence
def _existing_leads(conn: sqlite3.Connection, workflow_id: str,
                    hashes: list[bytes]) -> dict[bytes, int]:
    found: dict[bytes, int] = {}
    for i in range(0, len(hashes), CHUNK):
        part = hashes[i:i + CHUNK]
        q = ",".join("?" * len(part))
        for pid, ph in conn.execute(
                f"SELECT id, phone_hash FROM leads WHERE workflow_id=? AND phone_hash IN ({q})",
                [workflow_id, *part]):
            found[bytes(ph)] = pid
    return found


def persist_leads(conn: sqlite3.Connection, crypto: Crypto, schema: WorkflowSchema,
                  batch_id: str, source_file_id: int, parsed: list[ParsedRow]) -> LeadStats:
    """Apply parsed rows: create leads, merge duplicates (non-null last-write-wins),
    apply defaults to still-null fields of *new* leads, record rejections."""
    stats = LeadStats(rows_total=len(parsed))
    phone_field = next(f for f in schema.fields if f.type == "phone")
    ref_field = schema.reference_field
    fields_by_name = {f.name: f for f in schema.fields}

    conn.execute("INSERT OR IGNORE INTO batches VALUES (?,?,?)",
                 (schema.workflow_id, batch_id, source_file_id))

    rejections = []
    accepted: list[tuple[bytes, ParsedRow]] = []
    for p in parsed:
        if p.reason is not None:
            stats.rows_rejected += 1
            rejections.append((source_file_id, schema.workflow_id, batch_id, p.row_number, p.reason))
        else:
            stats.field_warnings += p.warnings
            accepted.append((crypto.phone_hash(p.values[phone_field.name]), p))
    conn.executemany("INSERT INTO lead_rejections(source_file_id, workflow_id, batch_id, row_number, reason)"
                     " VALUES (?,?,?,?,?)", rejections)

    existing = _existing_leads(conn, schema.workflow_id, list({h for h, _ in accepted}))
    next_id = (conn.execute("SELECT COALESCE(MAX(id), 0) FROM leads").fetchone()[0]) + 1

    # identity hash -> [lead_id, is_new, merged non-null field values]
    state: dict[bytes, list] = {}
    for h, p in accepted:
        entry = state.get(h)
        if entry is None:
            if h in existing:
                entry = [existing[h], False, {}]
                stats.duplicates_merged += 1
            else:
                entry = [next_id, True, {}]
                next_id += 1
                stats.new_leads += 1
            state[h] = entry
        else:
            stats.duplicates_merged += 1
        entry[2].update(p.values)   # only non-null values are present -> last write wins

    new_leads, new_fields, upd_leads, upd_fields = [], [], [], []
    for h, (lead_id, is_new, vals) in state.items():
        ref = vals[ref_field.name]
        if is_new:
            new_leads.append((lead_id, schema.workflow_id, h, ref, batch_id))
            for f in schema.fields:      # defaults apply AFTER merging, to still-null fields
                if f.name not in vals and f.default is not None:
                    vals[f.name] = f.default
        else:
            upd_leads.append((ref, lead_id))
        for name, val in vals.items():
            f = fields_by_name[name]
            if f.reference:
                continue
            if not is_new and f.type == "phone":
                continue                 # identity: unchanged by definition
            row = (lead_id, name, None, crypto.encrypt(val, pii_aad(lead_id, name))) if f.pii \
                else (lead_id, name, val, None)
            (new_fields if is_new else upd_fields).append(row)

    conn.executemany("INSERT INTO leads(id, workflow_id, phone_hash, external_ref, batch_id)"
                     " VALUES (?,?,?,?,?)", new_leads)
    conn.executemany("UPDATE leads SET external_ref=? WHERE id=?", upd_leads)
    conn.executemany("INSERT INTO lead_fields VALUES (?,?,?,?)", new_fields)
    conn.executemany("INSERT INTO lead_fields VALUES (?,?,?,?) ON CONFLICT(lead_id, field_name) "
                     "DO UPDATE SET value_plain=excluded.value_plain, value_enc=excluded.value_enc",
                     upd_fields)
    return stats
