"""Epistula authentication and signing utilities.

Adds helpers to load signing keys from a Bittensor-style wallet directory
(`~/.bittensor/wallets/<wallet>/hotkeys/<hotkey>`) so callers can specify
wallet and hotkey names (e.g., "coldkey1" / "hotkey1") without depending on
the full Bittensor stack. The created keypair is compatible with Substrate
`sr25519` verification used by miners.
"""

import os
import json
import time
import base58
from typing import Dict, List, Optional
from hashlib import blake2b, sha256

from loguru import logger
from sn_uuid import uuid7


class EpistulaVerifier:
    """Handles verification and signing for the Epistula protocol."""

    def __init__(
        self,
        *,
        miner_hotkey: Optional[str] = None,
        whitelist: Optional[List[str]] = None,
        allowed_delta_ms: int = 8000,
    ) -> None:
        self.MINER_HOTKEY = miner_hotkey or os.environ.get("MINER_HOTKEY")
        if not self.MINER_HOTKEY:
            raise ValueError("MINER_HOTKEY environment variable must be set")

        if whitelist is None:
            env_val = os.environ.get("EPISTULA_WHITELIST", "").strip()
            whitelist = [a for a in (x.strip() for x in env_val.split(",")) if a]
        self.whitelist = set(whitelist or [])

        self.ALLOWED_DELTA_MS = allowed_delta_ms
        logger.info(
            "Initialized EpistulaVerifier for miner hotkey: {} (whitelist {} entries)",
            self.MINER_HOTKEY,
            len(self.whitelist),
        )

    def ss58_decode(self, address: str, valid_ss58_format: Optional[int] = None) -> str:
        """
        Decodes given SS58 encoded address to an account ID
        Parameters
        ----------
        address: e.g. EaG2CRhJWPb7qmdcJvy3LiWdh26Jreu9Dx6R1rXxPmYXoDk
        valid_ss58_format

        Returns
        -------
        Decoded string AccountId
        """

        if address.startswith("0x"):
            return address

        if address == "":
            raise ValueError("Empty address provided")

        checksum_prefix = b"SS58PRE"

        address_decoded = base58.b58decode(address)

        if address_decoded[0] & 0b0100_0000:
            ss58_format_length = 2
            ss58_format = (
                ((address_decoded[0] & 0b0011_1111) << 2)
                | (address_decoded[1] >> 6)
                | ((address_decoded[1] & 0b0011_1111) << 8)
            )
        else:
            ss58_format_length = 1
            ss58_format = address_decoded[0]

        if ss58_format in [46, 47]:
            raise ValueError(f"{ss58_format} is a reserved SS58 format")

        if valid_ss58_format is not None and ss58_format != valid_ss58_format:
            raise ValueError("Invalid SS58 format")

        if len(address_decoded) in [3, 4, 6, 10]:
            checksum_length = 1
        elif len(address_decoded) in [
            5,
            7,
            11,
            34 + ss58_format_length,
            35 + ss58_format_length,
        ]:
            checksum_length = 2
        elif len(address_decoded) in [8, 12]:
            checksum_length = 3
        elif len(address_decoded) in [9, 13]:
            checksum_length = 4
        elif len(address_decoded) in [14]:
            checksum_length = 5
        elif len(address_decoded) in [15]:
            checksum_length = 6
        elif len(address_decoded) in [16]:
            checksum_length = 7
        elif len(address_decoded) in [17]:
            checksum_length = 8
        else:
            raise ValueError("Invalid address length")

        checksum = blake2b(
            checksum_prefix + address_decoded[0:-checksum_length]
        ).digest()

        if checksum[0:checksum_length] != address_decoded[-checksum_length:]:
            raise ValueError("Invalid checksum")

        return address_decoded[
            ss58_format_length : len(address_decoded) - checksum_length
        ].hex()

    def convert_ss58_to_hex(self, ss58_address: str) -> str:
        """Convert SS58 address to hex format."""
        address_bytes = self.ss58_decode(ss58_address)
        if isinstance(address_bytes, str):
            address_bytes = bytes.fromhex(address_bytes)
        return address_bytes.hex()

    @staticmethod
    def sign(
        *,
        body: bytes,
        signed_by_keypair: "object",
        signed_for: Optional[str] = None,
    ) -> Dict[str, str]:
        """Create Epistula headers for a request.

        - Signature covers: sha256(body).hexdigest() . uuid . timestamp . signed_for
        - Compatible with EpistulaVerifier.verify_signature
        - `signed_by_keypair` must provide `.sign(str)->bytes` and `.ss58_address`.
        """
        timestamp = str(int(time.time() * 1000))
        uuid_str = str(uuid7())
        body_hash = sha256(body).hexdigest()
        message = f"{body_hash}.{uuid_str}.{timestamp}.{signed_for or ''}"
        signature = signed_by_keypair.sign(message)
        sig_hex = "0x" + signature.hex()
        return {
            "Epistula-Request-Signature": sig_hex,
            "Epistula-Timestamp": timestamp,
            "Epistula-Uuid": uuid_str,
            "Epistula-Signed-By": getattr(signed_by_keypair, "ss58_address"),
            "Epistula-Signed-For": signed_for or "",
        }

    async def verify_signature(
        self,
        signature: str,
        body: bytes,
        timestamp: str,
        uuid_str: str,
        signed_for: Optional[str],
        signed_by: str,
        now: int,
        path: str = "",
    ) -> Optional[str]:
        if self.whitelist and signed_by not in self.whitelist:
            logger.error(f"Signed-by hotkey not allowed: {signed_by}")
            return "Sender not allowed"

        if signed_for and signed_for != self.MINER_HOTKEY:
            logger.error(
                f"Request signed for {signed_for} but expected {self.MINER_HOTKEY}"
            )
            return "Invalid signed_for address"

        logger.debug(f"Verifying signature with params:")
        logger.debug(f"signature: {signature}")
        logger.debug(f"body hash: {sha256(body).hexdigest()}")
        logger.debug(f"timestamp: {timestamp}")
        logger.debug(f"uuid: {uuid_str}")
        logger.debug(f"signed_for: {signed_for}")
        logger.debug(f"signed_by: {signed_by}")
        logger.debug(f"current time: {now}")

        if not isinstance(signature, str):
            return "Invalid Signature"
        try:
            timestamp = int(timestamp)
        except (ValueError, TypeError):
            return "Invalid Timestamp"
        if not isinstance(signed_by, str):
            return "Invalid Sender key"
        if not isinstance(uuid_str, str):
            return "Invalid uuid"
        if not isinstance(body, bytes):
            return "Body is not of type bytes"

        if timestamp + self.ALLOWED_DELTA_MS < now:
            return "Request is too stale"

        try:
            from substrateinterface import Keypair  # Lazy import to avoid hard dep at import time
            keypair = Keypair(ss58_address=signed_by)
        except Exception as e:
            logger.error(f"Invalid Keypair for signed_by '{signed_by}': {e}")
            return "Invalid Keypair"

        message = (
            f"{sha256(body).hexdigest()}.{uuid_str}.{timestamp}.{signed_for or ''}"
        )
        logger.debug(f"Constructed message for verification: {message}")

        try:
            signature_bytes = bytes.fromhex(signature[2:])  # Remove '0x' prefix
            logger.debug(f"Parsed signature bytes (hex): {signature_bytes.hex()}")
        except ValueError as e:
            logger.error(f"Failed to parse signature: {e}")
            return "Invalid Signature Format"

        verified = keypair.verify(message, signature_bytes)
        logger.debug(f"Signature verification result: {verified}")

        if not verified:
            return "Signature Mismatch"

        return None


class _SignerAdapter:
    """Adapter to normalize different keypair implementations.

    Expects underlying object to expose `ss58_address` and a `sign(message)`
    method which may return bytes or hex string. Returns bytes consistently.
    """

    def __init__(self, key: "object") -> None:
        self._key = key

    @property
    def ss58_address(self) -> str:
        return getattr(self._key, "ss58_address")

    def sign(self, message: str) -> bytes:
        sig = self._key.sign(message)
        if isinstance(sig, bytes):
            return sig
        if isinstance(sig, str):
            s = sig[2:] if sig.startswith("0x") else sig
            try:
                return bytes.fromhex(s)
            except Exception:
                try:
                    import base64

                    return base64.b64decode(sig)
                except Exception:
                    pass
        raise TypeError("Unsupported signature return type from keypair.sign()")


def _read_hotkey_file(wallets_dir: str, wallet_name: str, hotkey_name: str) -> Dict[str, str]:
    """Read a Bittensor-style hotkey keyfile and return its JSON content.

    Accepts files created by `btcli` or our test helpers. The file typically
    contains fields like `secretPhrase`, `secretSeed`, `ss58Address`.
    """
    import pathlib

    p = pathlib.Path(wallets_dir).expanduser().resolve() / wallet_name / "hotkeys" / hotkey_name
    if not p.exists():
        raise FileNotFoundError(f"Hotkey file not found: {p}")
    try:
        data = json.loads(p.read_text())
        if not isinstance(data, dict):
            raise ValueError("Hotkey file content must be a JSON object")
        return data  # type: ignore[return-value]
    except Exception as e:
        raise RuntimeError(f"Failed to read hotkey file at {p}: {e}")


def _read_hotkey_file_by_path(path: str) -> Dict[str, str]:
    """Read a hotkey keyfile from an absolute or user path and return its JSON content."""
    import pathlib

    p = pathlib.Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"Hotkey file not found: {p}")
    try:
        data = json.loads(p.read_text())
        if not isinstance(data, dict):
            raise ValueError("Hotkey file content must be a JSON object")
        return data  # type: ignore[return-value]
    except Exception as e:
        raise RuntimeError(f"Failed to read hotkey file at {p}: {e}")


def _create_keypair(secret_phrase: Optional[str], secret_seed: Optional[str]) -> "object":
    """Create a sr25519-compatible keypair from mnemonic or seed.

    Tries `substrateinterface.Keypair` first. If unavailable, attempts
    `bittensor_wallet.Keypair`. Raises with a clear error if neither is
    installed in the environment.
    """
    last_error: Optional[Exception] = None

    try:
        from substrateinterface import Keypair, KeypairType  # type: ignore

        if secret_phrase:
            return Keypair.create_from_mnemonic(secret_phrase, crypto_type=KeypairType.SR25519)
        if secret_seed:
            seed = secret_seed[2:] if secret_seed.startswith("0x") else secret_seed
            return Keypair.create_from_seed(seed_hex=seed, crypto_type=KeypairType.SR25519)
    except Exception as e:  # pragma: no cover - environment-dependent
        last_error = e

    try:  # pragma: no cover - optional path
        from bittensor_wallet import Keypair  # type: ignore

        if secret_phrase:
            return Keypair.create_from_mnemonic(secret_phrase)
        if secret_seed:
            seed = secret_seed[2:] if secret_seed.startswith("0x") else secret_seed
            return Keypair.create_from_seed(seed)
    except Exception as e:
        last_error = e

    raise RuntimeError(
        "Unable to construct sr25519 keypair. Install `substrate-interface` or `bittensor-wallet`. "
        f"Last error: {last_error}"
    )


def load_keypair_from_wallet(
    *,
    wallet_name: str,
    hotkey_name: str,
    wallets_dir: Optional[str] = None,
) -> "object":
    """Load a signing keypair from ~/.bittensor wallet layout.

    - Defaults to `wallets_dir=~/.bittensor/wallets`.
    - Returns an object with `.ss58_address` and `.sign(message)`.
    """
    wallets_dir = wallets_dir or os.environ.get("BT_WALLETS_DIR", "~/.bittensor/wallets")
    payload = _read_hotkey_file(wallets_dir, wallet_name, hotkey_name)

    secret_phrase = payload.get("secretPhrase")
    secret_seed = payload.get("secretSeed")
    if not (secret_phrase or secret_seed):
        raise RuntimeError("Hotkey file missing `secretPhrase` or `secretSeed`")

    key = _create_keypair(secret_phrase, secret_seed)
    try:
        expected = payload.get("ss58Address")
        if expected:
            actual = getattr(key, "ss58_address")
            if actual != expected:
                logger.warning("Keypair ss58 mismatch: file=%s != derived=%s", expected, actual)
    except Exception:
        pass
    return _SignerAdapter(key)


def load_keypair_from_env() -> Optional["object"]:
    """Environment-based loader for convenience in services.

    Respects the following env vars (first match wins for each):
    - Wallet name: `EPISTULA_WALLET_NAME`, `BT_WALLET_NAME`, `WALLET_NAME`
    - Hotkey name: `EPISTULA_HOTKEY_NAME`, `BT_HOTKEY_NAME`, `HOTKEY_NAME`
    - Wallets dir: `BT_WALLETS_DIR` (default: `~/.bittensor/wallets`)
    - Direct mnemonic: `EPISTULA_SIGNING_MNEMONIC`, `ORCHESTRATOR_SIGNING_MNEMONIC`
    """
    key_path = os.environ.get("EPISTULA_HOTKEY_PATH")
    if key_path:
        try:
            payload = _read_hotkey_file_by_path(key_path)
            key = _create_keypair(payload.get("secretPhrase"), payload.get("secretSeed"))
            return _SignerAdapter(key)
        except Exception as e:
            logger.error(f"Failed to load EPISTULA_HOTKEY_PATH: {e}")

    mnemonic = os.environ.get("EPISTULA_SIGNING_MNEMONIC") or os.environ.get("ORCHESTRATOR_SIGNING_MNEMONIC")
    if mnemonic:
        key = _create_keypair(mnemonic, None)
        return _SignerAdapter(key)

    wallet_name = (
        os.environ.get("EPISTULA_WALLET_NAME")
        or os.environ.get("BT_WALLET_NAME")
        or os.environ.get("WALLET_NAME")
        or "coldkey"
    )
    hotkey_name = (
        os.environ.get("EPISTULA_HOTKEY_NAME")
        or os.environ.get("BT_HOTKEY_NAME")
        or os.environ.get("HOTKEY_NAME")
        or "hotkey"
    )
    wallets_dir = os.environ.get("BT_WALLETS_DIR") or "~/.bittensor/wallets"

    try:
        return load_keypair_from_wallet(
            wallet_name=wallet_name, hotkey_name=hotkey_name, wallets_dir=wallets_dir
        )
    except Exception:
        pass

    try:
        import pathlib
        base = pathlib.Path(wallets_dir).expanduser().resolve()
        candidates = sorted(base.glob("*/hotkeys/*"))
        for cand in candidates:
            try:
                payload = json.loads(cand.read_text())
                if not isinstance(payload, dict):
                    continue
                if payload.get("secretPhrase") or payload.get("secretSeed"):
                    key = _create_keypair(payload.get("secretPhrase"), payload.get("secretSeed"))
                    logger.warning(f"Loaded Epistula hotkey via auto-discovery: {cand}")
                    return _SignerAdapter(key)
            except Exception:
                continue
    except Exception:
        pass

    return None
