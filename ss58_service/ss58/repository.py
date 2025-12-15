"""Repository for reading entries from IPFS via Pinata gateway."""

from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Set, Tuple

import requests

from .schemas import Entry
from .signing import verify_signature

LOGGER = logging.getLogger("ss58.repository")


class EntryRepository:
    """Fetch and traverse log entries via Pinata gateway."""

    def __init__(
        self,
        *,
        gateway_base: str = "https://gateway.pinata.cloud/ipfs",
        timeout_seconds: int = 20,
        verify_key_hex: str | None = None,
    ) -> None:
        self.gateway_base = gateway_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.verify_key_hex = verify_key_hex

    def fetch(self, cid: str) -> Entry:
        url = f"{self.gateway_base}/{cid}"
        resp = requests.get(url, timeout=self.timeout_seconds)
        try:
            resp.raise_for_status()
        except Exception:
            LOGGER.error("Failed to fetch entry from gateway (cid=%s): %s", cid, resp.text[:200])
            raise
        data = resp.json()
        return Entry.model_validate(data)

    def traverse(self, head_cid: str) -> Iterable[Tuple[str, Entry]]:
        """Yield (cid, Entry) following prev pointers until null."""
        cid = head_cid
        while cid:
            entry = self.fetch(cid)
            yield cid, entry
            cid = entry.data.prev or ""

    def collect_addresses(self, head_cid: Optional[str]) -> Tuple[List[str], Set[str]]:
        """Return ordered list (newest to oldest) and a set of unique addresses.

        Verifies signatures when a public key is configured.
        """
        ordered: List[str] = []
        seen: Set[str] = set()
        if not head_cid:
            return ordered, seen
        for cid, entry in self.traverse(head_cid):
            if self.verify_key_hex:
                verify_signature(self.verify_key_hex, entry.data, entry.signature)
            for addr in entry.data.addresses:
                ordered.append(addr)
                seen.add(addr)
        return ordered, seen
