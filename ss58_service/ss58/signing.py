"""Signing helpers for SS58 entries."""

from __future__ import annotations

import binascii
import logging
import json

import nacl.signing
from nacl.exceptions import BadSignatureError

from nacl.signing import SigningKey

from .schemas import EntryData

LOGGER = logging.getLogger("ss58.signing")


class Ed25519Signer:
    """Minimal Ed25519 signer that returns hex signatures."""

    def __init__(self, seed: bytes) -> None:
        if len(seed) != 32:
            raise ValueError("Ed25519 seed must be 32 bytes")
        self._signing_key = SigningKey(seed)
        self.public_hex = self._signing_key.verify_key.encode().hex()

    def sign(self, payload: bytes) -> str:
        """Return a hex-encoded signature prefixed with 0x."""
        sig = self._signing_key.sign(payload).signature
        return "0x" + sig.hex()


def load_signer(seed_hex: str | None) -> Ed25519Signer:
    """Construct signer from a hex seed string."""
    if not seed_hex:
        raise ValueError("SS58_ED25519_SEED_HEX is required for signing")

    normalized = seed_hex.strip().lower()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    try:
        seed = binascii.unhexlify(normalized)
    except binascii.Error as exc:  # pragma: no cover - defensive
        LOGGER.error("Failed to decode SS58_ED25519_SEED_HEX: %s", exc)
        raise ValueError("SS58_ED25519_SEED_HEX must be hex-encoded")

    return Ed25519Signer(seed)


def canonical_data_bytes(data: EntryData) -> bytes:
    """Return canonical JSON bytes for signing/verifying."""
    canonical = json.dumps(data.model_dump(), separators=(",", ":"), sort_keys=True)
    return canonical.encode("utf-8")


def verify_signature(public_hex: str, data: EntryData, signature_hex: str) -> None:
    """Verify an EntryData signature or raise ValueError."""
    sig = signature_hex.lower()
    if sig.startswith("0x"):
        sig = sig[2:]
    try:
        signature = bytes.fromhex(sig)
    except Exception as exc:
        raise ValueError(f"Invalid signature encoding: {exc}")

    try:
        pub = bytes.fromhex(public_hex[2:] if public_hex.startswith("0x") else public_hex)
        vk = nacl.signing.VerifyKey(pub)
        vk.verify(canonical_data_bytes(data), signature)
    except BadSignatureError as exc:
        raise ValueError(f"Signature verification failed: {exc}")
    except Exception as exc:
        raise ValueError(f"Verification error: {exc}")
