from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel


class AuditRecordResponse(BaseModel):
    hotkey: str
    failed_job: Mapping[str, Any]
    reference_job: Mapping[str, Any]
