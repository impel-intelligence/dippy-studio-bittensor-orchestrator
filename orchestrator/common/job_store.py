from __future__ import annotations

"""Job domain models and enums."""

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
from sn_uuid import uuid7


__all__ = [
    "Job",
    "JobRequest",
    "JobResponse",
    "JobStatus",
    "JobType",
    "AuditStatus",
]


class JobType(str, Enum):
    LORA = "lora"
    GENERATE = "generate"
    FLUX_DEV = "base-h100_pcie"
    FLUX_KONTEXT = "img-h100_pcie"


class JobStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    TIMEOUT = "timeout"
    FAILED = "failed"


class AuditStatus(str, Enum):
    """The audit status of a completed job."""

    NOT_AUDITED = "not_audited"
    AUDIT_PENDING = "audit_pending"
    AUDIT_SUCCESS = "audit_success"
    AUDIT_FAILED = "audit_failed"



@dataclass
class JobRequest:
    job_type: JobType
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)


@dataclass
class JobResponse:
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)



@dataclass
class Job:
    """Represents a job to be executed by a miner, and its result."""

    job_request: JobRequest
    hotkey: str
    job_id: uuid.UUID = field(default_factory=uuid7)
    status: JobStatus = JobStatus.PENDING
    job_response: Optional[JobResponse] = None

    callback_secret: Optional[str] = None
    prompt_seed: Optional[int] = None
    prepared_at: Optional[float] = None
    dispatched_at: Optional[float] = None
    failure_reason: Optional[str] = None

    is_audit_job: bool = False
    audit_status: AuditStatus = AuditStatus.NOT_AUDITED
    audit_id: Optional[uuid.UUID] = None
