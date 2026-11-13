"""PII protection, key handling and the command-line interface."""
from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from solution.config import ConfigError, Keys, load_keys
from solution.normalise import normalise_phone
from solution.security import Crypto, DecryptionError
from solution.tests.conftest import OTHER_KEYS, REPO, TEST_KEYS, b64


# ------------------------------------------------------------------ crypto
def test_hmac_is_deterministic_and_key_dependent():
    a, b = Crypto(TEST_KEYS), Crypto(OTHER_KEYS)
    assert a.phone_hash("+919876543210") == Crypto(TEST_KEYS).phone_hash("+919876543210")
    assert a.phone_hash("+919876543210") != a.phone_hash("+919876543211")
    assert a.phone_hash("+919876543210") != b.phone_hash("+919876543210")
    assert len(a.phone_hash("+919876543210")) == 32


def test_encryption_roundtrip_nonce_and_aad_binding():
    c = Crypto(TEST_KEYS)
    blob1, blob2 = c.encrypt("Asha Rao", "lead:1:name"), c.encrypt("Asha Rao", "lead:1:name")
    assert blob1 != blob2                                       # random nonce
    assert b"Asha" not in blob1
    assert c.decrypt(blob1, "lead:1:name") == "Asha Rao"
    with pytest.raises(DecryptionError):
        c.decrypt(blob1, "lead:2:name")                         # moved to another slot
    with pytest.raises(DecryptionError):
        Crypto(OTHER_KEYS).decrypt(blob1, "lead:1:name")        # wrong key
    tampered = bytearray(blob1)
    tampered[-1] ^= 1
    with pytest.raises(DecryptionError):
        c.decrypt(bytes(tampered), "lead:1:name")


def test_unicode_pii_roundtrip():
    c = Crypto(TEST_KEYS)
    assert c.decrypt(c.encrypt("अनन्या ₹", "x"), "x") == "अनन्या ₹"


# -------------------------------------------------------------- key config
def test_load_keys_validation(monkeypatch):
    monkeypatch.delenv("LEDGER_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("LEDGER_HMAC_KEY", raising=False)
    with pytest.raises(ConfigError, match="LEDGER_ENCRYPTION_KEY is not set"):
        load_keys()
    monkeypatch.setenv("LEDGER_ENCRYPTION_KEY", b64(b"short"))
    monkeypatch.setenv("LEDGER_HMAC_KEY", b64(bytes(64)))
    with pytest.raises(ConfigError, match="exactly 32"):
        load_keys()
    monkeypatch.setenv("LEDGER_ENCRYPTION_KEY", "!!!not base64!!!")
    with pytest.raises(ConfigError, match="base64"):
        load_keys()
    monkeypatch.setenv("LEDGER_ENCRYPTION_KEY", b64(bytes(range(32))))
    monkeypatch.setenv("LEDGER_HMAC_KEY", b64(bytes(range(32))))
    with pytest.raises(ConfigError, match="different"):
        load_keys()
    monkeypatch.setenv("LEDGER_HMAC_KEY", b64(bytes(range(16))))
    with pytest.raises(ConfigError, match="at least 32"):
        load_keys()
    monkeypatch.setenv("LEDGER_HMAC_KEY", b64(bytes(range(100, 164))))
    assert isinstance(load_keys(), Keys)


# ----------------------------------------------------- PII absent at rest/out
def collect_pii(data_dir: Path) -> tuple[set[str], set[str]]:
    names, phones = set(), set()
    for f in data_dir.glob("drop_*/leads/*/*.csv"):
        with f.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        head = [h.strip().lower().replace("_", " ") for h in rows[0]]
        for r in rows[1:]:
            for h, v in zip(head, r):
                if h in ("customer name", "name", "insured name") and v.strip():
                    names.add(v.strip())
                if h in ("phone", "mobile", "mobile no", "contact"):
                    p = normalise_phone(v)
                    if p:
                        phones.add(p)
    return names, phones


def test_no_plaintext_pii_in_database_files(gen_data, make_pipeline, tmp_path):
    data = gen_data(42)
    p = make_pipeline("pii.db")
    p.ingest(data)
    p.report(tmp_path / "rep")
    # PRAGMA wal_checkpoint, then scan the main file AND any leftover wal bytes
    p.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    p.close()
    blob = b"".join(f.read_bytes() for f in tmp_path.glob("pii.db*"))
    names, phones = collect_pii(data)
    assert len(names) > 100 and len(phones) > 1000
    leaked_names = [n for n in names if n.encode() in blob]
    leaked_phones = [ph for ph in phones if ph.encode() in blob or ph[3:].encode() in blob]
    assert leaked_names == [] and leaked_phones == []
    # positive control: the scanner does find plaintext when it is there
    import sqlite3
    control = tmp_path / "control.db"
    c = sqlite3.connect(str(control))
    c.execute("CREATE TABLE t(x)")
    c.execute("INSERT INTO t VALUES (?)", (sorted(names)[0],))
    c.commit()
    c.close()
    assert sorted(names)[0].encode() in control.read_bytes()


def test_reports_contain_only_masked_phones_and_no_names(gen_data, pipeline, tmp_path):
    data = gen_data(42)
    pipeline.ingest(data)
    pipeline.report(tmp_path / "rep")
    names, phones = collect_pii(data)
    for f in (tmp_path / "rep").iterdir():
        text = f.read_text(encoding="utf-8")
        assert not [n for n in names if n in text], f.name
        assert not [ph for ph in phones if ph[3:] in text or ph in text], f.name
    masked = (tmp_path / "rep" / "lead_report.csv").read_text(encoding="utf-8")
    assert all(re.fullmatch(r"\+91\*{6}\d{4}", r["phone_masked"])
               for r in csv.DictReader(masked.splitlines()))


def test_stored_pii_is_recoverable_with_the_right_key(pipeline, tmp_path):
    from solution.security import pii_aad
    from solution.tests.conftest import write_drop
    from solution.tests.test_ledger_rules import lead_csv
    pipeline.ingest(write_drop(tmp_path, "drop_1", {("motor_renewal", "batch_001"):
                                                    lead_csv(("Asha Rao", "98765 43210", "P1"))}))
    lid, enc = pipeline.conn.execute("SELECT lead_id, value_enc FROM lead_fields "
                                     "WHERE field_name='customer_name'").fetchone()
    assert pipeline.crypto.decrypt(bytes(enc), pii_aad(lid, "customer_name")) == "Asha Rao"
    assert pipeline.conn.execute("SELECT COUNT(*) FROM lead_fields WHERE value_plain LIKE '%Asha%' "
                                 "OR value_plain LIKE '%98765%'").fetchone()[0] == 0


# --------------------------------------------------------------------- CLI
def cli(args, cwd, extra_env=None, keys=True):
    env = {k: v for k, v in os.environ.items() if not k.startswith("LEDGER_")}
    env["PYTHONPATH"] = str(REPO)
    if keys:
        env["LEDGER_ENCRYPTION_KEY"] = b64(TEST_KEYS.encryption)
        env["LEDGER_HMAC_KEY"] = b64(TEST_KEYS.hmac)
    env.update(extra_env or {})
    return subprocess.run([sys.executable, "-m", "solution", *args], cwd=cwd, env=env,
                          capture_output=True, text=True)


def test_cli_end_to_end_matches_spec_commands_and_leaks_no_pii(gen_data, tmp_path):
    data = gen_data(42)
    for d in ("drop_1", "drop_2"):
        r = cli(["ingest", str(data / d)], tmp_path)
        assert r.returncode == 0, r.stderr
    r2 = cli(["ingest", str(data / "drop_2")], tmp_path)           # idempotent re-run
    assert r2.returncode == 0 and "skipped (already ingested): 3" in r2.stdout
    r3 = cli(["report", "--out", "reports/"], tmp_path)
    assert r3.returncode == 0, r3.stderr
    assert sorted(p.name for p in (tmp_path / "reports").iterdir()) == [
        "batch_summary.csv", "daily_overview.csv", "data_quality.json", "lead_report.csv",
        "rejections.csv"]
    names, phones = collect_pii(data)
    combined = r.stdout + r.stderr + r2.stdout + r2.stderr + r3.stdout + r3.stderr
    assert not [n for n in names if n in combined] and not [p for p in phones if p[3:] in combined]
    assert (tmp_path / "ledger.db").exists()


def test_cli_exit_codes(gen_data, tmp_path):
    data = gen_data(42)
    assert cli(["ingest", str(data / "drop_1")], tmp_path, keys=False).returncode == 2
    r = cli(["ingest", str(tmp_path / "missing")], tmp_path)
    assert r.returncode == 2 and "not a directory" in r.stderr
    assert cli(["ingest", str(data / "drop_1")], tmp_path,
               {"LEDGER_ENCRYPTION_KEY": "bad"}).returncode == 2
    assert cli(["ingest", str(data / "drop_1")], tmp_path,
               {"LEDGER_SCHEMAS_DIR": str(tmp_path / "nope")}).returncode == 2
    assert cli([], tmp_path).returncode == 2                               # usage error (argparse)
    ok = cli(["ingest", str(data / "drop_1")], tmp_path)
    assert ok.returncode == 0
    other = {"LEDGER_ENCRYPTION_KEY": b64(OTHER_KEYS.encryption), "LEDGER_HMAC_KEY": b64(OTHER_KEYS.hmac)}
    r = cli(["report", "--out", "o"], tmp_path, other)
    assert r.returncode == 2 and "do not match" in r.stderr


def test_cli_corrupt_csv_returns_1_without_leaking_content(tmp_path):
    d = tmp_path / "drop_1" / "leads" / "motor_renewal"
    d.mkdir(parents=True)
    (d / "batch_001.csv").write_bytes(b"customer_name,phone,policy_number\nSecretPerson\xff,9876543210,P\n")
    r = cli(["ingest", str(tmp_path / "drop_1")], tmp_path)
    assert r.returncode == 1 and "SecretPerson" not in r.stderr + r.stdout and "9876543210" not in r.stderr


def test_cli_loads_dotenv_and_gen_keys(tmp_path):
    out = cli(["gen-keys"], tmp_path, keys=False).stdout.split()
    assert [o.split("=")[0] for o in out] == ["LEDGER_ENCRYPTION_KEY", "LEDGER_HMAC_KEY"]
    (tmp_path / ".env").write_text("\n".join(out) + "\n", encoding="utf-8")
    r = cli(["init-db"], tmp_path, keys=False)
    assert r.returncode == 0, r.stderr


def test_gitignore_protects_secrets_and_databases():
    text = (REPO / ".gitignore").read_text(encoding="utf-8")
    for needle in (".env", "*.db", "-wal", "-shm", ".venv"):
        assert needle in text
    example = (REPO / ".env.example").read_text(encoding="utf-8")
    assert "LEDGER_ENCRYPTION_KEY=" in example and "LEDGER_HMAC_KEY=" in example
    assert re.search(r"LEDGER_(ENCRYPTION|HMAC)_KEY=\S{20,}", example) is None   # placeholders only


def test_report_without_database_is_a_config_error(tmp_path):
    r = cli(["report", "--out", "o"], tmp_path)
    assert r.returncode == 2 and "run `ingest` first" in r.stderr
