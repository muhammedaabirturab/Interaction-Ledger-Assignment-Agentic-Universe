from datetime import date
from decimal import Decimal

import pytest

from solution.normalise import (format_ist, mask_phone, normalise_header, normalise_phone,
                                parse_date, parse_decimal, parse_enum, parse_timestamp_us)


@pytest.mark.parametrize("raw,expected", [
    ("9876543210", "+919876543210"),
    ("+91 98765 43210", "+919876543210"),
    ("09876543210", "+919876543210"),
    ("91-98765-43210", "+919876543210"),
    ("(+91) 9876543210", "+919876543210"),
    ("+919876543210", "+919876543210"),
    ("  98765.43210 ", "+919876543210"),
    ("6000000000", "+916000000000"),
    # invalid
    ("5876543210", None),          # prefix 5
    ("987654321", None),           # 9 digits
    ("98765432101", None),         # 11 digits not starting with 0
    ("919876543210 ext 1", None),  # extra digits
    ("022-2345-6789", None),       # landline (0 + 2xxxxxxxxx)
    ("+1 415 555 0134", None),
    ("N/A", None),
    ("", None),
    ("0919876543210", None),       # 13 digits
    ("91" + "5876543210", None),   # 12 digits but local part invalid
    ("449876543210", None),         # 12 digits NOT starting with 91 must not be trimmed
    ("19876543210", None),         # 11 digits NOT starting with 0 must not be trimmed
    ("٩٨٧٦٥٤٣٢١٠", None),          # non-ASCII digits are not digits
])
def test_phone(raw, expected):
    assert normalise_phone(raw) == expected


def test_phone_none_and_mask():
    assert normalise_phone(None) is None
    assert mask_phone("+919876543210") == "+91******3210"


@pytest.mark.parametrize("raw,expected", [
    ("Policy  No", "policy no"), ("  POLICY_NO ", "policy no"), ("policy__\t no", "policy no"),
    ("mobile_no", "mobile no"), ("Customer Name", "customer name"), ("_phone_", "phone"),
])
def test_header_normalisation(raw, expected):
    assert normalise_header(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("2026-12-01", date(2026, 12, 1)),
    ("01/12/2026", date(2026, 12, 1)),          # day-first
    ("01-12-2026", date(2026, 12, 1)),
    ("01-Dec-2026", date(2026, 12, 1)),
    ("01-dEC-2026", date(2026, 12, 1)),         # case-insensitive month
    ("29/02/2028", date(2028, 2, 29)),          # leap day ok
    ("13/01/2026", date(2026, 1, 13)),
    ("31/02/2026", None), ("29/02/2027", None), ("2026-13-01", None),
    ("soon", None), ("TBD", None), ("", None), ("01-Foo-2026", None), ("2026/12/01", None),
    ("1/2/26", None),
])
def test_dates(raw, expected):
    assert parse_date(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("12345", Decimal("12345.00")), ("12345.5", Decimal("12345.50")),
    ("₹12,345", Decimal("12345.00")), ("₹ 1,23,456.789", Decimal("123456.79")),
    ("Rs. 4500", Decimal("4500.00")), ("rs 4500", Decimal("4500.00")),
    ("RS.4500", Decimal("4500.00")), ("INR 9999.9", Decimal("9999.90")),
    ("inr9", Decimal("9.00")), ("0", Decimal("0.00")), ("0.005", Decimal("0.01")),
    ("N/A", None), ("-", None), ("twelve thousand", None), ("-5", None), ("", None),
    ("Rs", None), ("₹", None), ("1.2.3", None), ("$5", None), ("NaN", None), ("inf", None),
])
def test_decimal(raw, expected):
    assert parse_decimal(raw) == expected


def test_enum():
    vals = ["en", "hi"]
    assert parse_enum("HI", vals) == "hi"
    assert parse_enum(" en ", vals) == "en"
    assert parse_enum("Hindi", vals) is None


def test_timestamps_three_forms_agree():
    utc = parse_timestamp_us("2026-10-03T04:31:00Z")
    off = parse_timestamp_us("2026-10-03T10:01:00+05:30")
    naive = parse_timestamp_us("2026-10-03 10:01:00")          # naive == IST
    assert utc == off == naive


@pytest.mark.parametrize("bad", [None, "", "yesterday", 17, "2026-13-40T00:00:00Z"])
def test_timestamp_invalid(bad):
    assert parse_timestamp_us(bad) is None


def test_format_ist_rolls_date_forward():
    us = parse_timestamp_us("2026-10-01T20:00:00Z")            # 01:30 next day in IST
    assert format_ist(us) == "2026-10-02T01:30:00+05:30"
