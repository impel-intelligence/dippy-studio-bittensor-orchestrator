from __future__ import annotations

from enum import Enum
from typing import Any


class JobSource(str, Enum):
    """Enumerates known job source labels used for auditing and tracking."""

    AUDIT_ORIGINAL = "audit_original"
    AUDIT_CHECK = "audit_check"


def normalize_job_source(value: JobSource | str | None) -> str:
    """Return a trimmed string representation of a job source enum or value."""
    if value is None:
        return ""
    if isinstance(value, JobSource):
        return value.value
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


__all__ = ["JobSource", "normalize_job_source"]
