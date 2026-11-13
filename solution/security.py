"""PII protection: HMAC for deterministic matching, AES-256-GCM for reversible storage.

* ``phone_hash``  = HMAC-SHA256(hmac_key, "phone:v1:" + canonical phone).
  Deterministic, so leads and events can be joined without storing the number.
* ``encrypt``/``decrypt`` = AES-256-GCM with a random 96-bit nonce and
  associated data binding the ciphertext to its (lead, field) slot, so a
  ciphertext copied to another row fails authentication.
Two independent keys are used (key separation).
"""
from __future__ import annotations

import hashlib
import hmac
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from solution.config import Keys

NONCE_LEN = 12


class DecryptionError(Exception):
    """Ciphertext failed authentication (wrong key, tampering, or wrong slot)."""


class Crypto:
    def __init__(self, keys: Keys):
        self._aead = AESGCM(keys.encryption)
        self._hmac_key = keys.hmac
        self._phone_cache: dict[str, bytes] = {}

    def phone_hash(self, canonical_phone: str) -> bytes:
        h = self._phone_cache.get(canonical_phone)
        if h is None:
            h = hmac.digest(self._hmac_key, b"phone:v1:" + canonical_phone.encode(), hashlib.sha256)
            if len(self._phone_cache) < 500_000:
                self._phone_cache[canonical_phone] = h
        return h

    def encrypt(self, plaintext: str, aad: str) -> bytes:
        nonce = os.urandom(NONCE_LEN)
        return nonce + self._aead.encrypt(nonce, plaintext.encode("utf-8"), aad.encode())

    def decrypt(self, blob: bytes, aad: str) -> str:
        try:
            return self._aead.decrypt(blob[:NONCE_LEN], blob[NONCE_LEN:], aad.encode()).decode("utf-8")
        except InvalidTag as exc:
            raise DecryptionError("authentication failed") from exc

    def key_check_values(self) -> tuple[bytes, bytes]:
        """Non-secret probes stored in ``meta`` to detect a wrong key on an existing DB."""
        return (hmac.digest(self._hmac_key, b"key-check:v1", hashlib.sha256),
                self.encrypt("key-check", "meta:enc_check"))


def pii_aad(lead_id: int, field: str) -> str:
    return f"lead:{lead_id}:{field}"
