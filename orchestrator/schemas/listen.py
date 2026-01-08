from __future__ import annotations

import uuid
from typing import Any

from pydantic import AnyHttpUrl, BaseModel

from orchestrator.common.job_sources import JobSource
from orchestrator.common.job_store import JobType
from orchestrator.domain.miner import Miner


class ListenRequest(BaseModel):
    job_type: JobType
    payload: dict[str, Any]
    job_id: uuid.UUID | None = None
    source: JobSource | str | None = None


class RemoteListenRequest(ListenRequest):
    webhook_url: AnyHttpUrl
    route_to_auditor: bool = False


class DebugListenRequest(ListenRequest):
    miner: Miner | None = None


__all__ = ["ListenRequest", "RemoteListenRequest", "DebugListenRequest"]
