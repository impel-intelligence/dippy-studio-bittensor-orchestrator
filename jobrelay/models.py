"""Pydantic models and domain types."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


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
        "prepared_at",
        "dispatched_at",
        "response_timestamp",
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


class InferenceJobCreate(_DateTimeModel):
    job_type: str
    miner_hotkey: str
    source: str = Field(default="")
    payload: Dict[str, Any]

    result_image_url: Optional[str] = None

    creation_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated_at: Optional[datetime] = None
    miner_received_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_duration_ms: Optional[int] = Field(default=None, ge=0)
    expires_at: Optional[datetime] = None
    prepared_at: Optional[datetime] = None
    dispatched_at: Optional[datetime] = None
    response_timestamp: Optional[datetime] = None

    status: JobStatus = Field(default=JobStatus.pending)
    audit_status: AuditStatus = Field(default=AuditStatus.not_audited)
    verification_status: VerificationStatus = Field(default=VerificationStatus.nonverified)

    is_audit_job: bool = False
    audit_target_job_id: Optional[UUID] = None
    failure_reason: Optional[str] = None
    callback_secret: Optional[str] = None
    prompt_seed: Optional[int] = Field(default=None, ge=0)
    response_payload: Optional[Dict[str, Any]] = None


class InferenceJob(InferenceJobCreate):
    job_id: UUID


class InferenceJobUpdate(_DateTimeModel):
    job_type: Optional[str] = None
    miner_hotkey: Optional[str] = None
    source: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    result_image_url: Optional[str] = None

    creation_timestamp: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None
    miner_received_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_duration_ms: Optional[int] = Field(default=None, ge=0)
    expires_at: Optional[datetime] = None
    prepared_at: Optional[datetime] = None
    dispatched_at: Optional[datetime] = None
    response_timestamp: Optional[datetime] = None

    status: Optional[JobStatus] = None
    audit_status: Optional[AuditStatus] = None
    verification_status: Optional[VerificationStatus] = None

    is_audit_job: Optional[bool] = None
    audit_target_job_id: Optional[UUID] = None
    failure_reason: Optional[str] = None
    callback_secret: Optional[str] = None
    prompt_seed: Optional[int] = Field(default=None, ge=0)
    response_payload: Optional[Dict[str, Any]] = None

    def has_updates(self) -> bool:
        return any(value is not None for value in self.model_dump().values())
