"""Configuration: environment variables, optional ``.env`` file, validation."""
from __future__ import annotations

import base64
import binascii
import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCHEMAS_DIR = REPO_ROOT / "assignment-a-data" / "schemas"
DEFAULT_DB = "ledger.db"


class ConfigError(Exception):
    """Invalid or missing configuration (maps to CLI exit code 2)."""


def load_dotenv(path: Path | None = None) -> None:
    """Minimal ``.env`` loader: KEY=VALUE lines; real environment wins."""
    path = path or Path.cwd() / ".env"
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _decode_key(name: str, min_bytes: int, exact: int | None = None) -> bytes:
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise ConfigError(f"{name} is not set. Run `python -m solution gen-keys` and put the "
                          f"values in your environment or .env (see .env.example).")
    try:
        key = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
    except (binascii.Error, ValueError) as exc:
        raise ConfigError(f"{name} is not valid base64.") from exc
    if exact is not None and len(key) != exact:
        raise ConfigError(f"{name} must decode to exactly {exact} bytes (got {len(key)}).")
    if len(key) < min_bytes:
        raise ConfigError(f"{name} must decode to at least {min_bytes} bytes (got {len(key)}).")
    return key


@dataclass(frozen=True)
class Keys:
    encryption: bytes  # AES-256-GCM key (reversible PII)
    hmac: bytes        # HMAC-SHA256 key (deterministic matching)


def load_keys() -> Keys:
    enc = _decode_key("LEDGER_ENCRYPTION_KEY", 32, exact=32)
    mac = _decode_key("LEDGER_HMAC_KEY", 32)
    if enc == mac:
        raise ConfigError("LEDGER_ENCRYPTION_KEY and LEDGER_HMAC_KEY must be different keys.")
    return Keys(encryption=enc, hmac=mac)


def resolve_db_path(cli_value: str | None) -> Path:
    return Path(cli_value or os.environ.get("LEDGER_DB") or DEFAULT_DB)


def resolve_schemas_dir(cli_value: str | None) -> Path:
    p = Path(cli_value or os.environ.get("LEDGER_SCHEMAS_DIR") or DEFAULT_SCHEMAS_DIR)
    if not p.is_dir():
        raise ConfigError(f"Schemas directory not found: {p}")
    return p
