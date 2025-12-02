from __future__ import annotations

import hashlib
import logging
import secrets
import time
import uuid
import asyncio
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.common.datetime import parse_datetime, parse_timestamp, timestamp_to_iso
from orchestrator.common.job_store import (
    AuditStatus,
    Job,
    JobRequest,
    JobResponse,
    JobStatus,
    JobType,
)
from orchestrator.schemas.job import JobRecord
from orchestrator.services.exceptions import (
    JobNotFound,
    JobQueryError,
    JobRelayError,
    JobServiceError,
    JobValidationError,
)
from sn_uuid import uuid7

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

            job_identifier = job_id or uuid7()
            created_at = time.time()

            relay_payload = {
                "job_type": self._job_type_to_str(job_type),
                "miner_hotkey": hotkey,
                "payload": prepared_payload,
                "creation_timestamp": timestamp_to_iso(created_at),
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
        except JobServiceError:
            raise
        except Exception as exc:
            logger.exception("job.create_failed job_type=%s", job_type)
            raise JobRelayError("Failed to create job in relay") from exc

    async def update_job(self, job_id: uuid.UUID, payload: Any) -> Job:
        try:
            response_timestamp = time.time()
            updates = {
                "response_payload": payload,
                "response_timestamp": timestamp_to_iso(response_timestamp),
                "completed_at": timestamp_to_iso(response_timestamp),
                "last_updated_at": timestamp_to_iso(response_timestamp),
                "status": JobStatus.SUCCESS.value,
                "failure_reason": None,
            }
            await self._sync_job_update(job_id, updates)
            return await self.get_job(job_id)
        except JobServiceError:
            raise
        except Exception as exc:
            logger.exception("job.update_failed job_id=%s", job_id)
            raise JobRelayError("Failed to update job in relay") from exc

    async def get_job_record(self, job_id: uuid.UUID) -> Dict[str, Any]:
        """Return the raw job record from the relay without any transformation."""

        return await self._fetch_job_record(job_id)

    async def fetch_masked_job_record(self, job_id: uuid.UUID) -> Dict[str, Any]:
        """Return the job record with sensitive fields sanitized for privacy."""

        record = await self._fetch_job_record(job_id)
        return self._sanitize_job_record(record)

    async def list_recent_completed_job_records(
        self,
        *,
        max_results: int = 1000,
        lookback_days: float | int | None = None,
    ) -> list[Dict[str, Any]]:
        """Return sanitized recent completed jobs ordered by completion time."""

        records = await self.list_recent_completed_jobs(
            max_results=max_results,
            lookback_days=lookback_days,
        )
        sanitized: list[Dict[str, Any]] = []
        for record in records:
            record_dict = record.model_dump(mode="json")
            sanitized.append(self._sanitize_job_record(record_dict))
        return sanitized

    async def get_job(self, job_id: uuid.UUID) -> Job:
        record = await self._fetch_job_record(job_id)
        return self._job_from_record(record)

    async def mark_job_prepared(self, job_id: uuid.UUID, prepared_at: float | None = None) -> Job:
        timestamp = prepared_at if prepared_at is not None else time.time()
        updates = {
            "prepared_at": timestamp_to_iso(timestamp),
            "failure_reason": None,
            "last_updated_at": timestamp_to_iso(timestamp),
        }
        await self._sync_job_update(job_id, updates)
        return await self.get_job(job_id)

    async def mark_job_dispatched(self, job_id: uuid.UUID, dispatched_at: float | None = None) -> Job:
        timestamp = dispatched_at if dispatched_at is not None else time.time()
        iso_timestamp = timestamp_to_iso(timestamp)
        updates = {
            "dispatched_at": iso_timestamp,
            "miner_received_at": iso_timestamp,
            "last_updated_at": iso_timestamp,
        }
        await self._sync_job_update(job_id, updates)
        return await self.get_job(job_id)

    async def mark_job_failure(self, job_id: uuid.UUID, reason: str) -> Job:
        timestamp = timestamp_to_iso(time.time())
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
            raise JobQueryError("Parameter 'limit' must be a positive integer")

        fetch_limit = max(max_results * 3, max_results)
        records = await self._list_jobs(limit=fetch_limit)
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

    async def list_recent_completed_jobs(
        self,
        *,
        max_results: int = 100,
        lookback_days: float | int | None = 7.0,
    ) -> list[JobRecord]:
        """Return the most recent completed jobs bounded by lookback window."""

        if max_results <= 0:
            raise JobQueryError("Parameter 'max_results' must be a positive integer")

        fetch_limit = max(max_results * 3, max_results)
        records = await self._list_jobs(limit=fetch_limit)
        cutoff_dt: datetime | None = None
        if lookback_days is not None:
            try:
                normalized_days = float(lookback_days)
            except (TypeError, ValueError):
                normalized_days = 0.0
            if normalized_days > 0.0:
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=normalized_days)

        completed_items: list[tuple[datetime, Job]] = []
        completed_states = _TERMINAL_JOB_STATUSES
        for record in records:
            status = self._parse_job_status(record.get("status"))
            if status not in completed_states:
                continue
            completed_dt = parse_datetime(
                record.get("completed_at") or record.get("response_timestamp")
            )
            if completed_dt is None:
                continue
            if cutoff_dt is not None and completed_dt < cutoff_dt:
                continue
            job = self._job_from_record(record)
            completed_items.append((completed_dt, job))

        completed_items.sort(key=lambda item: item[0], reverse=True)
        limited = completed_items[:max_results]
        return [JobRecord.from_store(job) for _, job in limited]

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
            raise JobRelayError("Failed to update job in relay") from exc

    async def _fetch_job_record(self, job_id: uuid.UUID) -> Dict[str, Any]:
        try:
            record = await self.job_relay.fetch_job(job_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("jobrelay.fetch_failed job_id=%s", job_id)
            raise JobRelayError("Failed to fetch job from relay") from exc

        if record is None:
            raise JobNotFound(f"Job {job_id} not found")
        return record

    async def _list_jobs(self, *, limit: int | None = None) -> list[Dict[str, Any]]:
        try:
            if limit is not None and limit > 0 and hasattr(self.job_relay, "list_recent_jobs"):
                return await self.job_relay.list_recent_jobs(limit=limit)  # type: ignore[arg-type]
            return await self.job_relay.list_jobs(limit=limit)
        except Exception as exc:  # noqa: BLE001
            logger.exception("jobrelay.list_failed")
            raise JobRelayError("Failed to list jobs from relay") from exc

    @staticmethod
    def _prepare_payload(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return deepcopy(payload)
        raise JobValidationError("Job payload must be a JSON object")

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

    @staticmethod
    def _hash_value(value: Any) -> str:
        """Hash a value using SHA256 and return in the format 'sha256:hash'."""
        text = str(value) if value is not None else ""
        digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
        return f"sha256:{digest}"

    @staticmethod
    def _is_callback_secret_field(key: Any) -> bool:
        if not key or not isinstance(key, str):
            return False
        return key.lower() == "callback_secret"

    @staticmethod
    def _strip_image_filename(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        cleaned = value.split("?", 1)[0].split("#", 1)[0]
        parsed = urlparse(cleaned)
        candidate = parsed.path or cleaned
        basename = PurePosixPath(candidate).name
        return basename or cleaned

    def _sanitize_job_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Hash prompts and strip image paths from a job record."""

        def _sanitize_value(value: Any, *, key: str | None = None) -> Any:
            key_lower = key.lower() if isinstance(key, str) else ""
            if isinstance(value, dict):
                return {
                    child_key: _sanitize_value(child_value, key=child_key)
                    for child_key, child_value in value.items()
                    if not self._is_callback_secret_field(child_key)
                }
            if isinstance(value, list):
                return [_sanitize_value(item, key=key) for item in value]
            if key_lower and "prompt" in key_lower and "seed" not in key_lower:
                return self._hash_value(value)
            if key_lower and "image" in key_lower and ("url" in key_lower or "uri" in key_lower):
                return self._strip_image_filename(value)
            return value

        sanitized: Dict[str, Any] = {}
        for key, value in deepcopy(record).items():
            if self._is_callback_secret_field(key):
                continue
            sanitized[key] = _sanitize_value(value, key=key)
        return sanitized

    def _job_from_record(self, record: Dict[str, Any]) -> Job:
        job_id = self._parse_uuid(record.get("job_id")) or uuid7()
        job_type = self._parse_job_type(record.get("job_type"))

        request_payload = deepcopy(record.get("payload") or {})
        creation_ts = parse_timestamp(record.get("creation_timestamp"))
        if creation_ts is None:
            creation_ts = time.time()
        job_request = JobRequest(
            job_type=job_type,
            payload=request_payload,
            timestamp=creation_ts,
        )

        response_payload = record.get("response_payload")
        response_timestamp = parse_timestamp(
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

        prepared_at = parse_timestamp(record.get("prepared_at"))
        dispatched_at = parse_timestamp(
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
            is_audit_job=bool(record.get("is_audit_job")),
            audit_status=self._parse_audit_status(record.get("audit_status")),
            audit_id=audit_id,
        )
