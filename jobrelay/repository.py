"""DuckDB persistence for inference jobs."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from .duckdb_manager import JobRelayDuckDBManager


LOGGER = logging.getLogger("jobrelay.repository")


class InferenceJobRepository:
    """Repository delegating persistence operations to the DuckDB manager."""

    def __init__(self, manager: JobRelayDuckDBManager):
        self._manager = manager

    def insert_job(self, record: Dict[str, object]) -> None:
        self._manager.insert_job(record)

    def update_job(self, job_id: UUID, updates: Dict[str, object]) -> None:
        self._manager.update_job(job_id, updates)

    def fetch_job(self, job_id: UUID) -> Optional[Dict[str, object]]:
        return self._manager.fetch_job(job_id)

    def fetch_all(self) -> List[Dict[str, object]]:
        return self._manager.fetch_all()

    def fetch_for_hotkey(
        self,
        miner_hotkey: str,
        *,
        since: datetime | None = None,
    ) -> List[Dict[str, object]]:
        return self._manager.fetch_for_hotkey(miner_hotkey, since=since)

    def delete_expired(self, now: datetime) -> int:
        return self._manager.delete_expired(now)

    def checkpoint(self) -> None:
        self._manager.checkpoint()

    def close(self) -> None:
        self._manager.close()

    def sync_to_gcs(self) -> Optional[str]:
        """Expose manager sync for callers that exercised snapshots."""

        return self._manager.sync_to_gcs()

    def nuclear_wipe(self) -> dict[str, int]:
        """Proxy destructive wipe to the underlying manager."""

        return self._manager.nuclear_wipe()
