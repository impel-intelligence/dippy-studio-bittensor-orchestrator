"""CLI tool to verify the full SS58 log chain signatures."""

from __future__ import annotations

import argparse
import sys
from typing import List

from .repository import EntryRepository
from .signing import verify_signature
from .schemas import Entry


def verify_chain(head_cid: str, public_key_hex: str, gateway_base: str, timeout_seconds: int = 20) -> List[str]:
    """Verify signatures for every entry starting from head and return addresses (oldest->newest)."""
    repo = EntryRepository(gateway_base=gateway_base, timeout_seconds=timeout_seconds, verify_key_hex=None)
    ordered_entries: List[Entry] = []
    for cid, entry in repo.traverse(head_cid):
        verify_signature(public_key_hex, entry.data, entry.signature)
        ordered_entries.append(entry)
    addresses: List[str] = []
    for entry in reversed(ordered_entries):
        addresses.extend(entry.data.addresses)
    return addresses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify SS58 append-only log signatures.")
    parser.add_argument("--head", required=True, help="Head CID to verify from")
    parser.add_argument("--public-key-hex", required=True, help="Ed25519 public key hex (0x allowed)")
    parser.add_argument(
        "--gateway",
        default="https://gateway.pinata.cloud/ipfs",
        help="Gateway base URL used to fetch entries",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    try:
        addresses = verify_chain(
            head_cid=args.head,
            public_key_hex=args.public_key_hex,
            gateway_base=args.gateway,
            timeout_seconds=args.timeout,
        )
    except Exception as exc:
        sys.stderr.write(f"Verification failed: {exc}\n")
        return 1

    sys.stdout.write(f"Verified chain ok. Addresses ({len(addresses)}):\\n")
    for addr in addresses:
        sys.stdout.write(addr + "\\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
