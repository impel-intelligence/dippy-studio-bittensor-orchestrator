"""DuckDB persistence for inference jobs."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID


LOGGER = logging.getLogger("jobrelay.repository")


class InferenceJobRepository:
    """Repository delegating persistence operations to the primary manager."""

    def __init__(self, primary):
        self._primary = primary

    def insert_job(self, record: Dict[str, object]) -> None:
        self._primary.insert_job(record)

    def update_job(self, job_id: UUID, updates: Dict[str, object]) -> None:
        self._primary.update_job(job_id, updates)

    def fetch_job(self, job_id: UUID) -> Optional[Dict[str, object]]:
        return self._primary.fetch_job(job_id)

    def fetch_all(self, limit: int | None = None) -> List[Dict[str, object]]:
        return self._primary.fetch_all(limit=limit)

    def fetch_for_hotkey(
        self,
        miner_hotkey: str,
        *,
        since: datetime | None = None,
    ) -> List[Dict[str, object]]:
        return self._primary.fetch_for_hotkey(miner_hotkey, since=since)

    def delete_expired(self, now: datetime) -> int:
        return self._primary.delete_expired(now)

    def checkpoint(self) -> None:
        self._primary.checkpoint()

    def purge_local(self, skip_snapshots: bool = False) -> dict[str, object]:
        return self._primary.purge_local(skip_snapshots=skip_snapshots)

    def close(self) -> None:
        self._primary.close()

    def sync_to_gcs(self) -> Optional[str]:
        """Expose primary sync for callers that exercised snapshots."""

        return self._primary.sync_to_gcs()

    def nuclear_wipe(self) -> dict[str, int]:
        """Proxy destructive wipe to the underlying primary manager."""

        return self._primary.nuclear_wipe()

    def flush(self) -> Optional[str]:
        if hasattr(self._primary, "flush"):
            return self._primary.flush()
        LOGGER.info("flush not supported by primary manager")
        return None
