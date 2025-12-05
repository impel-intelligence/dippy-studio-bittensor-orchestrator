from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
import hashlib
from typing import Any, Dict, Optional, List
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from orchestrator.common.job_store import Job


def _ensure_datetime(value: Optional[float | datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


class JobStatus(str, Enum):
    pending = "pending"
    success = "success"
    timeout = "timeout"
    failed = "failed"


class AuditStatus(str, Enum):
    not_audited = "not_audited"
    audit_pending = "audit_pending"
    audit_success = "audit_success"
    audit_failed = "audit_failed"


class VerificationStatus(str, Enum):
    nonverified = "nonverified"
    verified = "verified"


class _DateTimeModel(BaseModel):
    @staticmethod
    def _ensure_tz(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator(
        "creation_timestamp",
        "last_updated_at",
        "miner_received_at",
        "completed_at",
        "expires_at",
        mode="before",
        check_fields=False,
    )
    @classmethod
    def _datetime_to_utc(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            parsed = value
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class InferenceJob(_DateTimeModel):
    job_id: UUID
    job_type: str
    miner_hotkey: str
    payload: Dict[str, Any]

    result_image_url: Optional[str] = None
    result_image_sha256: Optional[str] = None

    creation_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated_at: Optional[datetime] = None
    miner_received_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_duration_ms: Optional[int] = Field(default=None, ge=0)
    expires_at: Optional[datetime] = None

    status: JobStatus = Field(default=JobStatus.pending)
    audit_status: AuditStatus = Field(default=AuditStatus.not_audited)
    verification_status: VerificationStatus = Field(default=VerificationStatus.nonverified)

    is_audit_job: bool = False
    audit_target_job_id: Optional[UUID] = None


class JobRecord(InferenceJob):

    model_config = ConfigDict(populate_by_name=True)

    miner_hotkey: str = Field(serialization_alias="hotkey")
    payload: Dict[str, Any] = Field(serialization_alias="request_payload")
    creation_timestamp: datetime = Field(serialization_alias="created_at")
    completed_at: Optional[datetime] = Field(default=None, serialization_alias="completed_at")
    last_updated_at: Optional[datetime] = Field(default=None, serialization_alias="last_updated_at")
    miner_received_at: Optional[datetime] = Field(default=None, serialization_alias="miner_received_at")

    prompt_seed: Optional[int] = None
    callback_secret: Optional[str] = None
    prepared_at: Optional[datetime] = None
    dispatched_at: Optional[datetime] = None
    failure_reason: Optional[str] = None
    response_payload: Optional[Dict[str, Any]] = None
    response_timestamp: Optional[datetime] = Field(default=None, serialization_alias="response_timestamp")
    audit_id: Optional[UUID] = None

    verification_status: VerificationStatus = VerificationStatus.nonverified

    @classmethod
    def from_store(cls, job: Job) -> "JobRecord":
        request_payload = deepcopy(job.job_request.payload)
        response_payload: Optional[Dict[str, Any]] = None
        response_timestamp: Optional[datetime] = None
        completed_at: Optional[datetime] = None
        last_updated_at: Optional[datetime] = None
        result_image_url: Optional[str] = None
        result_image_sha256: Optional[str] = None

        if job.job_response is not None:
            response_payload = deepcopy(job.job_response.payload)
            response_timestamp = _ensure_datetime(job.job_response.timestamp)
            completed_at = response_timestamp
            last_updated_at = response_timestamp
            if isinstance(response_payload, dict):
                candidate_url = response_payload.get("image_url") or response_payload.get("image_uri")
                if isinstance(candidate_url, str) and candidate_url.strip():
                    result_image_url = candidate_url
                candidate_hash = response_payload.get("image_sha256")
                if isinstance(candidate_hash, str) and candidate_hash.strip():
                    result_image_sha256 = candidate_hash
        else:
            last_updated_at = _ensure_datetime(job.dispatched_at or job.prepared_at or job.job_request.timestamp)

        prepared_at = _ensure_datetime(job.prepared_at)
        dispatched_at = _ensure_datetime(job.dispatched_at)

        return cls(
            job_id=job.job_id,
            job_type=getattr(job.job_request.job_type, "value", job.job_request.job_type),
            miner_hotkey=job.hotkey,
            payload=request_payload,
            result_image_url=result_image_url,
            result_image_sha256=result_image_sha256,
            creation_timestamp=_ensure_datetime(job.job_request.timestamp) or datetime.now(timezone.utc),
            last_updated_at=last_updated_at,
            miner_received_at=dispatched_at,
            completed_at=completed_at,
            execution_duration_ms=None,
            expires_at=None,
            status=_to_job_status(job.status),
            audit_status=_to_audit_status(job.audit_status),
            verification_status=VerificationStatus.nonverified,
            is_audit_job=bool(getattr(job, "is_audit_job", False)),
            audit_target_job_id=job.audit_id,
            prompt_seed=job.prompt_seed,
            callback_secret=job.callback_secret,
            prepared_at=prepared_at,
            dispatched_at=dispatched_at,
            failure_reason=job.failure_reason,
            response_payload=response_payload,
            response_timestamp=response_timestamp,
            audit_id=job.audit_id,
        )


class CompletedJobSummary(BaseModel):
    job_id: UUID
    job_type: str
    miner_hotkey: str
    status: JobStatus
    completed_at: datetime
    failure_reason: Optional[str] = None
    response_payload: Optional[Dict[str, Any]] = None
    result_image_url: Optional[str] = None
    result_image_sha256: Optional[str] = None

    @classmethod
    def from_job_record(cls, record: JobRecord) -> "CompletedJobSummary":
        completed = record.completed_at or record.last_updated_at or record.creation_timestamp
        sanitized_payload = _sanitize_response_payload(record.response_payload)
        return cls(
            job_id=record.job_id,
            job_type=record.job_type,
            miner_hotkey=record.miner_hotkey,
            status=record.status,
            completed_at=completed,
            failure_reason=record.failure_reason,
            response_payload=sanitized_payload,
            result_image_url=record.result_image_url,
            result_image_sha256=record.result_image_sha256,
        )


class CompletedJobsResponse(BaseModel):
    jobs: List[CompletedJobSummary]
    limit: int
    lookback_days: float


class RecentJobsResponse(BaseModel):
    jobs: List[Dict[str, Any]]
    limit: int
    total_jobs: int


def _to_job_status(status: Any) -> JobStatus:
    if isinstance(status, JobStatus):
        return status
    return JobStatus(getattr(status, "value", status))


def _to_audit_status(status: Any) -> AuditStatus:
    if isinstance(status, AuditStatus):
        return status
    return AuditStatus(getattr(status, "value", status))


def _sanitize_response_payload(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return payload
    sanitized: Dict[str, Any] = {}
    for key, value in payload.items():
        if _is_callback_secret_field(key):
            continue
        sanitized[key] = _sanitize_value(value, key=key)
    return sanitized


def _sanitize_value(value: Any, *, key: Optional[str] = None) -> Any:
    if isinstance(value, dict):
        sanitized: Dict[str, Any] = {}
        for child_key, child_value in value.items():
            if _is_callback_secret_field(child_key):
                continue
            sanitized[child_key] = _sanitize_value(child_value, key=child_key)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_value(item, key=key) for item in value]
    if _should_mask_field(key):
        return _hash_sensitive_value(value)
    return value


def _should_mask_field(key: Optional[str]) -> bool:
    if not key:
        return False
    return "prompt" in key.lower()


def _is_callback_secret_field(key: Optional[str]) -> bool:
    if not key:
        return False
    return key.lower() == "callback_secret"


def _hash_sensitive_value(value: Any) -> str:
    text = repr(value) if value is not None else ""
    digest = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"


__all__ = [
    "AuditStatus",
    "CompletedJobSummary",
    "CompletedJobsResponse",
    "InferenceJob",
    "JobRecord",
    "JobStatus",
    "RecentJobsResponse",
    "VerificationStatus",
]
