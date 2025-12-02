"""Background processing primitives for queued DuckDB writes and maintenance."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, Optional, List
from uuid import UUID

from .config import Settings
from .duckdb_manager import JobRelayDuckDBManager
from .repository import InferenceJobRepository


LOGGER = logging.getLogger("jobrelay.background")


class JobCommandType(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    CLEANUP = "cleanup"
    SNAPSHOT = "snapshot"
    SHUTDOWN = "shutdown"


@dataclass
class JobCommand:
    command_type: JobCommandType
    job_id: Optional[UUID] = None
    payload: Optional[Dict[str, object]] = None
    updates: Optional[Dict[str, object]] = None


class BackgroundJobProcessor:
    """Serialises write operations against DuckDB and runs periodic tasks."""

    _TIME_FIELDS = {
        "creation_timestamp",
        "last_updated_at",
        "miner_received_at",
        "completed_at",
        "expires_at",
        "prepared_at",
        "dispatched_at",
        "response_timestamp",
    }

    def __init__(
        self,
        repository: InferenceJobRepository,
        settings: Settings,
        manager: JobRelayDuckDBManager,
    ):
        self._repository = repository
        self._settings = settings
        self._manager = manager
        self._queue: asyncio.Queue[JobCommand] = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="jobrelay-db")
        self._worker_task: Optional[asyncio.Task] = None
        self._periodic_tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        if self._worker_task is not None:
            return
        self._worker_task = asyncio.create_task(self._worker_loop(), name="jobrelay-worker")
        self._periodic_tasks.append(
            asyncio.create_task(self._cleanup_loop(), name="jobrelay-cleanup")
        )
        if not self._settings.gcs_bucket:
            raise ValueError("JOBRELAY_GCS_BUCKET must be configured for snapshot management")
        self._periodic_tasks.append(
            asyncio.create_task(self._snapshot_loop(), name="jobrelay-snapshot")
        )

    async def stop(self) -> None:
        self._stop_event.set()
        await self._queue.put(JobCommand(command_type=JobCommandType.SHUTDOWN))
        if self._worker_task:
            await self._worker_task
        for task in self._periodic_tasks:
            task.cancel()
        if self._periodic_tasks:
            await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
        self._executor.shutdown(wait=True)
        self._repository.close()

    async def enqueue_insert(self, record: Dict[str, object]) -> None:
        await self._queue.put(JobCommand(command_type=JobCommandType.INSERT, payload=record))

    async def enqueue_update(self, job_id: UUID, updates: Dict[str, object]) -> None:
        await self._queue.put(
            JobCommand(command_type=JobCommandType.UPDATE, job_id=job_id, updates=updates)
        )

    async def trigger_cleanup(self) -> None:
        await self._queue.put(JobCommand(command_type=JobCommandType.CLEANUP))

    async def trigger_snapshot(self) -> None:
        await self._queue.put(JobCommand(command_type=JobCommandType.SNAPSHOT))

    async def wait_for_idle(self) -> None:
        await self._queue.join()

    async def nuclear_wipe(self) -> Dict[str, int]:
        """Drain outstanding work and delegate to the manager for a full wipe."""

        await self.wait_for_idle()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._repository.nuclear_wipe,
        )

    async def purge_local(self, *, skip_snapshots: bool = False) -> Dict[str, object]:
        """Clear local DuckDB state without deleting remote snapshots."""

        await self.wait_for_idle()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._repository.purge_local,
            skip_snapshots,
        )

    async def _worker_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            command = await self._queue.get()
            try:
                if command.command_type is JobCommandType.SHUTDOWN:
                    break
                await loop.run_in_executor(self._executor, self._execute_sync, command)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed processing command %s: %s", command.command_type, exc)
            finally:
                self._queue.task_done()

    def _execute_sync(self, command: JobCommand) -> None:
        if command.command_type is JobCommandType.INSERT and command.payload is not None:
            self._repository.insert_job(command.payload)
            LOGGER.debug("Inserted job %s", command.payload.get("job_id"))
            return
        if (
            command.command_type is JobCommandType.UPDATE
            and command.job_id is not None
            and command.updates
        ):
            self._repository.update_job(command.job_id, command.updates)
            LOGGER.debug("Updated job %s", command.job_id)
            return
        if command.command_type is JobCommandType.CLEANUP:
            now = datetime.now(timezone.utc)
            deleted = self._repository.delete_expired(now)
            if deleted:
                LOGGER.info("Deleted %s expired jobs", deleted)
            return
        if command.command_type is JobCommandType.SNAPSHOT:
            self._run_snapshot_sync()
            return

    async def _cleanup_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self._settings.cleanup_interval_seconds)
                await self.trigger_cleanup()
        except asyncio.CancelledError:  # noqa: PERF203
            return

    async def _snapshot_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self._settings.effective_sync_interval)
                await self.trigger_snapshot()
        except asyncio.CancelledError:  # noqa: PERF203
            return

    def _run_snapshot_sync(self) -> None:
        try:
            result = self._manager.sync_to_gcs()
            if result:
                LOGGER.info("Snapshot synced to %s", result)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Snapshot upload failed: %s", exc)

    def prepare_insert_payload(self, job_id: UUID, record: Dict[str, object]) -> Dict[str, object]:
        record = dict(record)
        ttl = timedelta(days=self._settings.ttl_days)
        creation = record.get("creation_timestamp")
        creation_dt = self._ensure_datetime(creation)
        if not record.get("last_updated_at"):
            record["last_updated_at"] = creation_dt
        if not record.get("expires_at"):
            record["expires_at"] = creation_dt + ttl
        record.setdefault("job_id", job_id)
        return record

    def prepare_update_payload(self, updates: Dict[str, object]) -> Dict[str, object]:
        now = datetime.now(timezone.utc)
        prepared: Dict[str, object] = {}
        for key, value in updates.items():
            if value is None:
                continue
            if key in self._TIME_FIELDS:
                prepared[key] = self._ensure_datetime(value)
            else:
                prepared[key] = value
        prepared.setdefault("last_updated_at", now)
        return prepared

    @staticmethod
    def _ensure_datetime(value: Optional[object]) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        return datetime.now(timezone.utc)
