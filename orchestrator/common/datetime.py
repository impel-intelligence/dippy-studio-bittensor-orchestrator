"""Utilities for working with timezone-aware datetimes and ISO strings."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def ensure_aware(value: datetime) -> datetime:
    """Return a UTC-aware datetime."""

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse ISO8601-like values or return timezone-aware datetimes."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_aware(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return ensure_aware(parsed)
    return None


def parse_timestamp(value: Any) -> Optional[float]:
    """Coerce a variety of timestamp inputs into a float seconds value."""

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    parsed = parse_datetime(value)
    if parsed is not None:
        return parsed.timestamp()
    return None


def timestamp_to_iso(timestamp: float | int | None) -> Optional[str]:
    """Render a numeric timestamp (seconds) as an ISO8601 string."""

    if timestamp is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    except (OSError, OverflowError, ValueError, TypeError):
        return None
    return dt.isoformat()


__all__ = [
    "ensure_aware",
    "parse_datetime",
    "parse_timestamp",
    "timestamp_to_iso",
]
