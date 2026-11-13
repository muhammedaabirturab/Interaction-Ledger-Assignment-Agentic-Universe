"""Pure value-normalisation functions (no I/O, no database).

Every function returns ``None`` when the input cannot be interpreted, so the
caller decides whether that means "reject the row" or "warn and store null".
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_US = timedelta(microseconds=1)

# ---------------------------------------------------------------- headers
_HEADER_SPLIT = re.compile(r"[_\s]+")


def normalise_header(raw: str) -> str:
    """Lower-case, trim, and collapse every run of ``_``/whitespace to one space."""
    return _HEADER_SPLIT.sub(" ", raw.strip().lower()).strip()


# ----------------------------------------------------------------- phones
_NON_DIGIT = re.compile(r"[^0-9]")


def normalise_phone(raw: object) -> Optional[str]:
    """Return ``+91XXXXXXXXXX`` for a valid Indian mobile number, else ``None``."""
    if raw is None:
        return None
    digits = _NON_DIGIT.sub("", str(raw))
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


def mask_phone(canonical: str) -> str:
    """``+919876543210`` -> ``+91******3210`` (only the last four digits survive)."""
    return "+91******" + canonical[-4:]


# ------------------------------------------------------------------ dates
_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$")
_DMY = re.compile(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$")
_DMON = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")


def parse_date(raw: str) -> Optional[date]:
    """Parse YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (day-first)."""
    s = raw.strip()
    try:
        m = _ISO.match(s)
        if m:
            return date(int(m[1]), int(m[2]), int(m[3]))
        m = _DMY.match(s)
        if m:
            return date(int(m[3]), int(m[2]), int(m[1]))
        m = _DMON.match(s)
        if m and m[2].lower() in _MONTHS:
            return date(int(m[3]), _MONTHS[m[2].lower()], int(m[1]))
    except ValueError:  # impossible calendar date, e.g. 31/02
        return None
    return None


# --------------------------------------------------------------- decimals
_MONEY = re.compile(r"^(?:₹|rs\.?|inr)?\s*(\d[\d,]*(?:\.\d+)?|\.\d+)$", re.IGNORECASE)
_CENT = Decimal("0.01")


def parse_decimal(raw: str) -> Optional[Decimal]:
    """Parse a non-negative amount with optional currency prefix, 2 d.p. (half-up)."""
    m = _MONEY.match(raw.strip())
    if not m:
        return None
    try:
        return Decimal(m[1].replace(",", "")).quantize(_CENT, rounding=ROUND_HALF_UP)
    except ArithmeticError:
        return None


# ------------------------------------------------------------------ enums
def parse_enum(raw: str, values: list[str]) -> Optional[str]:
    v = raw.strip().lower()
    return v if v in {x.lower() for x in values} else None


# ------------------------------------------------------------- timestamps
def parse_timestamp_us(raw: object) -> Optional[int]:
    """Event timestamp -> integer microseconds since the UTC epoch.

    ``...Z`` and ``...+05:30`` are taken as written; a *naive* timestamp means IST.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return (dt - _EPOCH) // _US


def format_ist(us: int) -> str:
    """Microseconds since epoch -> ``2026-10-14T17:55:24+05:30`` (second precision)."""
    dt = (_EPOCH + timedelta(microseconds=us)).astimezone(IST)
    return dt.replace(microsecond=0).isoformat()
