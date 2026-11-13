"""Workflow lead schemas and the disposition catalogue (read from the schemas dir)."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from solution.normalise import normalise_header


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool
    pii: bool
    reference: bool
    values: tuple[str, ...]
    default: str | None
    header_keys: frozenset[str]  # normalised name + aliases


@dataclass(frozen=True)
class WorkflowSchema:
    workflow_id: str
    description: str
    fields: tuple[Field, ...]
    raw_json: str

    @property
    def reference_field(self) -> Field:
        return next(f for f in self.fields if f.reference)


@dataclass(frozen=True)
class Disposition:
    workflow_id: str
    code: str
    label: str
    is_terminal: bool
    is_success: bool


def _parse_schema(path: Path) -> WorkflowSchema:
    text = path.read_text(encoding="utf-8")
    doc = json.loads(text)
    fields = []
    for f in doc["fields"]:
        keys = {normalise_header(f["name"])} | {normalise_header(a) for a in f.get("aliases", [])}
        fields.append(Field(
            name=f["name"], type=f["type"], required=bool(f.get("required", False)),
            pii=bool(f.get("pii", False)), reference=bool(f.get("reference", False)),
            values=tuple(f.get("values", ())), default=f.get("default"),
            header_keys=frozenset(keys)))
    schema = WorkflowSchema(doc["workflow_id"], doc.get("description", ""), tuple(fields), text)
    if sum(f.reference for f in fields) != 1:
        raise ValueError(f"{path.name}: exactly one field must be marked reference")
    if sum(f.type == "phone" for f in fields) != 1:
        raise ValueError(f"{path.name}: exactly one phone field is required for lead identity")
    return schema


def load_schemas(schemas_dir: Path) -> dict[str, WorkflowSchema]:
    out: dict[str, WorkflowSchema] = {}
    for p in sorted(schemas_dir.glob("*.json")):
        s = _parse_schema(p)
        out[s.workflow_id] = s
    if not out:
        raise ValueError(f"No workflow schemas (*.json) found in {schemas_dir}")
    return out


def load_dispositions(schemas_dir: Path) -> list[Disposition]:
    path = schemas_dir / "dispositions.csv"
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [Disposition(r["workflow_id"].strip(), r["code"].strip(), r["label"].strip(),
                            r["is_terminal"].strip().lower() == "true",
                            r["is_success"].strip().lower() == "true")
                for r in csv.DictReader(fh)]
