from __future__ import annotations

import logging
import secrets
import time
import uuid
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from fastapi import HTTPException, status

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.common.job_store import (
    AuditStatus,
    Job,
    JobRequest,
    JobResponse,
    JobStatus,
    JobType,
)
from orchestrator.schemas.job import JobRecord

logger = logging.getLogger(__name__)


class JobWaitTimeoutError(TimeoutError):
    """Raised when a job does not reach a terminal state within the allotted time."""


class JobWaitCancelledError(Exception):
    """Raised when waiting for a job is cancelled (for example, client disconnect)."""


_TERMINAL_JOB_STATUSES = {
    JobStatus.SUCCESS,
    JobStatus.FAILED,
    JobStatus.TIMEOUT,
}


class JobService:

    def __init__(self, job_relay: BaseJobRelayClient) -> None:
        if job_relay is None:
            raise ValueError("job_relay client must be provided")
        self.job_relay = job_relay

    async def create_job(
        self,
        job_type: JobType,
        payload: Any,
        hotkey: str,
        *,
        job_id: uuid.UUID | None = None,
    ) -> Job:
        try:
            prepared_payload = self._prepare_payload(payload)
            secret = secrets.token_hex(32)

            prepared_payload["callback_secret"] = secret
            seed = self._normalize_seed(prepared_payload.get("seed"))
            if seed is None:
                seed = secrets.randbelow(2**32)
            prepared_payload["seed"] = seed

            job_identifier = job_id or uuid.uuid4()
            created_at = time.time()

            relay_payload = {
                "job_type": self._job_type_to_str(job_type),
                "miner_hotkey": hotkey,
                "payload": prepared_payload,
                "creation_timestamp": self._timestamp_to_iso(created_at),
                "status": JobStatus.PENDING.value,
                "audit_status": AuditStatus.NOT_AUDITED.value,
                "verification_status": "nonverified",
                "is_audit_job": False,
                "callback_secret": secret,
                "prompt_seed": seed,
            }

            await self.job_relay.create_job(job_identifier, relay_payload)

            job_request = JobRequest(
                job_type=job_type,
                payload=deepcopy(prepared_payload),
                timestamp=created_at,
            )
            return Job(
                job_request=job_request,
                hotkey=hotkey,
                job_id=job_identifier,
                status=JobStatus.PENDING,
                callback_secret=secret,
                prompt_seed=seed,
                audit_status=AuditStatus.NOT_AUDITED,
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("job.create_failed job_type=%s", job_type)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc

    async def update_job(self, job_id: uuid.UUID, payload: Any) -> Job:
        try:
            response_timestamp = time.time()
            updates = {
                "response_payload": payload,
                "response_timestamp": self._timestamp_to_iso(response_timestamp),
                "completed_at": self._timestamp_to_iso(response_timestamp),
                "last_updated_at": self._timestamp_to_iso(response_timestamp),
                "status": JobStatus.SUCCESS.value,
                "failure_reason": None,
            }
            await self._sync_job_update(job_id, updates)
            return await self.get_job(job_id)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("job.update_failed job_id=%s", job_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc

    async def get_job(self, job_id: uuid.UUID) -> Job:
        try:
            record = await self.job_relay.fetch_job(job_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("jobrelay.fetch_failed job_id=%s", job_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch job from relay",
            ) from exc

        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found",
            )

        return self._job_from_record(record)

    async def mark_job_prepared(self, job_id: uuid.UUID, prepared_at: float | None = None) -> Job:
        timestamp = prepared_at if prepared_at is not None else time.time()
        updates = {
            "prepared_at": self._timestamp_to_iso(timestamp),
            "failure_reason": None,
            "last_updated_at": self._timestamp_to_iso(timestamp),
        }
        await self._sync_job_update(job_id, updates)
        return await self.get_job(job_id)

    async def mark_job_dispatched(self, job_id: uuid.UUID, dispatched_at: float | None = None) -> Job:
        timestamp = dispatched_at if dispatched_at is not None else time.time()
        iso_timestamp = self._timestamp_to_iso(timestamp)
        updates = {
            "dispatched_at": iso_timestamp,
            "miner_received_at": iso_timestamp,
            "last_updated_at": iso_timestamp,
        }
        await self._sync_job_update(job_id, updates)
        return await self.get_job(job_id)

    async def mark_job_failure(self, job_id: uuid.UUID, reason: str) -> Job:
        timestamp = self._timestamp_to_iso(time.time())
        updates = {
            "status": JobStatus.FAILED.value,
            "failure_reason": reason,
            "last_updated_at": timestamp,
        }
        await self._sync_job_update(job_id, updates)
        return await self.get_job(job_id)

    async def dump_jobs(
        self,
        *,
        statuses: Optional[Iterable[JobStatus]] = None,
        limit: Optional[int] = None,
    ) -> list[JobRecord]:
        if limit is not None and limit <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Parameter 'limit' must be a positive integer",
            )

        records = await self._list_jobs()
        jobs: List[Job] = []
        allowed_statuses = {status for status in statuses} if statuses else None
        for record in records:
            job = self._job_from_record(record)
            if allowed_statuses is not None and job.status not in allowed_statuses:
                continue
            jobs.append(job)

        if limit is not None:
            jobs.sort(key=lambda item: item.job_request.timestamp, reverse=True)
            jobs = jobs[:limit]

        return [JobRecord.from_store(job) for job in jobs]

    async def get_job_totals(self) -> dict[str, int]:
        """Return aggregate counts of jobs by basic lifecycle buckets."""

        records = await self._list_jobs()
        completed_states = {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.TIMEOUT}
        completed = 0
        for record in records:
            status = self._parse_job_status(record.get("status"))
            if status in completed_states:
                completed += 1

        return {"total": len(records), "completed": completed}

    async def wait_for_terminal_state(
        self,
        job_id: uuid.UUID,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float = 1.0,
        is_disconnected: Callable[[], Awaitable[bool]] | None = None,
    ) -> Job:
        """Poll until a job reaches a terminal status or the timeout expires."""

        timeout = float(timeout_seconds)
        if timeout < 0.0:
            timeout = 0.0
        poll_interval = float(poll_interval_seconds)
        if poll_interval <= 0.0:
            poll_interval = 0.1

        deadline = time.monotonic() + timeout

        while True:
            if is_disconnected is not None and await is_disconnected():
                logger.info("job.wait_cancelled job_id=%s", job_id)
                raise JobWaitCancelledError("Client disconnected while waiting for job completion")

            job = await self.get_job(job_id)
            if job.status in _TERMINAL_JOB_STATUSES:
                return job

            now = time.monotonic()
            if timeout == 0.0 or now >= deadline:
                raise JobWaitTimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

            remaining = deadline - now
            sleep_for = poll_interval if timeout == float("inf") else min(poll_interval, max(remaining, 0.0))
            if sleep_for <= 0.0:
                raise JobWaitTimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

            await asyncio.sleep(sleep_for)

    async def _sync_job_update(self, job_id: uuid.UUID, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        try:
            await self.job_relay.update_job(job_id, updates)
        except Exception as exc:  # noqa: BLE001
            logger.exception("jobrelay.update_failed job_id=%s", job_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update job in relay",
            ) from exc

    async def _list_jobs(self) -> list[Dict[str, Any]]:
        try:
            return await self.job_relay.list_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.exception("jobrelay.list_failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to list jobs from relay",
            ) from exc

    @staticmethod
    def _prepare_payload(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return deepcopy(payload)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job payload must be a JSON object",
        )

    @staticmethod
    def _normalize_seed(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        try:
            seed = int(value)
        except (TypeError, ValueError):
            return None
        if seed < 0:
            return None
        return seed

    @staticmethod
    def _job_type_to_str(job_type: JobType) -> str:
        return getattr(job_type, "value", job_type)

    @staticmethod
    def _timestamp_to_iso(timestamp: float | None) -> Optional[str]:
        if timestamp is None:
            return None
        try:
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
        return dt.isoformat()

    @staticmethod
    def _iso_to_timestamp(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).timestamp()
        if isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
            return parsed.astimezone(timezone.utc).timestamp()
        return None

    @staticmethod
    def _parse_uuid(value: Any) -> Optional[uuid.UUID]:
        if isinstance(value, uuid.UUID):
            return value
        if isinstance(value, str) and value:
            try:
                return uuid.UUID(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_job_type(value: Any) -> JobType:
        if isinstance(value, JobType):
            return value
        try:
            return JobType(str(value))
        except Exception:  # noqa: BLE001
            return JobType.GENERATE

    @staticmethod
    def _parse_job_status(value: Any) -> JobStatus:
        if isinstance(value, JobStatus):
            return value
        try:
            return JobStatus(str(value))
        except Exception:  # noqa: BLE001
            return JobStatus.PENDING

    @staticmethod
    def _parse_audit_status(value: Any) -> AuditStatus:
        if isinstance(value, AuditStatus):
            return value
        try:
            return AuditStatus(str(value))
        except Exception:  # noqa: BLE001
            return AuditStatus.NOT_AUDITED

    def _job_from_record(self, record: Dict[str, Any]) -> Job:
        job_id = self._parse_uuid(record.get("job_id")) or uuid.uuid4()
        job_type = self._parse_job_type(record.get("job_type"))

        request_payload = deepcopy(record.get("payload") or {})
        creation_ts = self._iso_to_timestamp(record.get("creation_timestamp"))
        if creation_ts is None:
            creation_ts = time.time()
        job_request = JobRequest(
            job_type=job_type,
            payload=request_payload,
            timestamp=creation_ts,
        )

        response_payload = record.get("response_payload")
        response_timestamp = self._iso_to_timestamp(
            record.get("response_timestamp") or record.get("completed_at")
        )
        job_response: Optional[JobResponse] = None
        if response_payload is not None:
            job_response = JobResponse(
                payload=deepcopy(response_payload),
                timestamp=response_timestamp or time.time(),
            )

        callback_secret = record.get("callback_secret") or request_payload.get("callback_secret")
        prompt_seed = self._normalize_seed(record.get("prompt_seed") or request_payload.get("seed"))

        prepared_at = self._iso_to_timestamp(record.get("prepared_at"))
        dispatched_at = self._iso_to_timestamp(
            record.get("dispatched_at") or record.get("miner_received_at")
        )

        failure_reason = record.get("failure_reason")
        audit_id = self._parse_uuid(record.get("audit_target_job_id"))

        return Job(
            job_request=job_request,
            hotkey=str(record.get("miner_hotkey") or ""),
            job_id=job_id,
            status=self._parse_job_status(record.get("status")),
            job_response=job_response,
            callback_secret=callback_secret,
            prompt_seed=prompt_seed,
            prepared_at=prepared_at,
            dispatched_at=dispatched_at,
            failure_reason=failure_reason,
            audit_status=self._parse_audit_status(record.get("audit_status")),
            audit_id=audit_id,
        )
