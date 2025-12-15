"""Core service logic for SS58 append-only log management."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from .config import HeadState, Settings
from .pinata import PinataClient
from .schemas import AddResponse, Entry, EntryData
from .repository import EntryRepository
from .signing import Ed25519Signer, canonical_data_bytes
from .store import HeadStore

LOGGER = logging.getLogger("ss58.service")


class SS58Service:
    """Handles creation and pinning of SS58 log entries."""

    def __init__(
        self,
        *,
        settings: Settings,
        signer: Ed25519Signer,
        pinata: PinataClient,
        head_store: HeadStore,
        repository: EntryRepository,
    ) -> None:
        self.settings = settings
        self.signer = signer
        self.public_key_hex = signer.public_hex
        self.pinata = pinata
        self.head_store = head_store
        self.repository = repository

    def _sign_data(self, data: EntryData) -> str:
        return self.signer.sign(canonical_data_bytes(data))

    def _pin_entry(self, entry: Entry, name: Optional[str] = None) -> str:
        payload: Dict[str, Any] = entry.model_dump()
        return self.pinata.pin_json(payload, name=name)

    def _build_entry(self, addresses: List[str], prev: Optional[str]) -> Tuple[Entry, str, HeadState]:
        now = int(time.time())
        data = EntryData(addresses=addresses, timestamp=now, prev=prev)
        signature = self._sign_data(data)
        entry = Entry(data=data, signature=signature)
        name = f"ss58-entry-{now}"
        cid = self._pin_entry(entry, name=name)
        head_state = self.head_store.write(cid)
        LOGGER.info("Pinned entry to IPFS (cid=%s prev=%s addresses=%d)", cid, prev, len(addresses))
        return entry, cid, head_state

    def create_genesis(self) -> AddResponse:
        """Create a blank genesis entry (no addresses, prev=None)."""
        entry, cid, head_state = self._build_entry([], prev=None)
        return AddResponse(
            cid=cid,
            prev=None,
            timestamp=entry.data.timestamp,
            addresses=entry.data.addresses,
            signature=entry.signature,
        )

    def append(self, addresses: List[str]) -> AddResponse:
        """Append a new list of addresses to the log (deduplicated)."""
        head_state = self.head_store.read()
        prev = head_state.cid if head_state else None

        # Collect existing addresses to avoid duplicates
        _, existing = self.repository.collect_addresses(prev)
        new_addrs = [a for a in addresses if a not in existing]
        if not new_addrs:
            LOGGER.info("No new addresses to append; skipping pin")
            return AddResponse(
                cid=prev or "",
                prev=prev,
                timestamp=int(time.time()),
                addresses=[],
                signature="",
            )

        entry, cid, updated_head = self._build_entry(new_addrs, prev=prev)
        return AddResponse(
            cid=cid,
            prev=prev,
            timestamp=entry.data.timestamp,
            addresses=entry.data.addresses,
            signature=entry.signature,
        )

    def head(self) -> HeadState | None:
        return self.head_store.read()

    def dump_addresses(self) -> List[str]:
        head_state = self.head_store.read()
        ordered, _ = self.repository.collect_addresses(head_state.cid if head_state else None)
        # Return oldest->newest for readability
        return list(reversed(ordered))
