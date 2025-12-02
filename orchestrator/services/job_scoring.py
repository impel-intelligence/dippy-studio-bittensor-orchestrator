from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from orchestrator.common.datetime import parse_datetime, parse_timestamp

DEFAULT_MAX_LATENCY_MS = 60_000.0
H100_KONTEXT_MAX_LATENCY_MS = 15_000.0
_JOB_TYPE_WEIGHTS: dict[str, float] = {
    "img-h100_pcie": 0.8,
    "base-h100_pcie": 0.2,
}
_H100_KONTEXT_JOB_TYPES = {
    "img-h100_pcie",
    "flux_kontext",
    "h100_pcie",
}


def job_latency_ms(job: Mapping[str, Any]) -> Optional[float]:
    """Return the first latency metric found for a job, or None when missing."""
    return _extract_latency_ms(job)


def job_to_score(job: Mapping[str, Any], *, max_latency_ms: float = DEFAULT_MAX_LATENCY_MS) -> float:
    """Score an inference job based on its latency.

    The score falls in the range [0, 1], where faster jobs earn a higher score.
    """
    latency_ms = _extract_latency_ms(job)
    if latency_ms is None:
        return 0.0

    latency_ms = max(latency_ms, 0.0)
    if max_latency_ms <= 0:
        return 0.0 if latency_ms > 0 else 1.0

    ratio = min(latency_ms / max_latency_ms, 1.0)
    return 1.0 - ratio


def job_to_weighted_score(
    job: Mapping[str, Any],
    *,
    max_latency_ms: float = DEFAULT_MAX_LATENCY_MS,
) -> float:
    """Apply job-type weighting to the latency-based score."""
    weight = _job_type_weight(job.get("job_type"))
    if weight <= 0.0:
        return 0.0
    base_score = job_to_score(job, max_latency_ms=max_latency_ms)
    weighted = weight * base_score
    if weighted <= 0.0:
        return 0.0
    if weighted >= 1.0:
        return 1.0
    return weighted


def job_type_has_weight(job: Mapping[str, Any]) -> bool:
    """Return True when the job's type carries a non-zero weight."""
    return _job_type_weight(job.get("job_type")) > 0.0


def is_h100_kontext_job_type(job_type: Any) -> bool:
    normalized = _normalize_job_type(job_type)
    if not normalized:
        return False
    return normalized in _H100_KONTEXT_JOB_TYPES


def callback_latency_ms(job: Mapping[str, Any]) -> Optional[float]:
    """Return miner-ack-to-callback latency when available."""
    latency_keys = ("latency_ms",)
    metrics = job.get("metrics") if isinstance(job, Mapping) else None
    response_payload = job.get("response_payload") if isinstance(job, Mapping) else None

    for key in latency_keys:
        latency = _coerce_to_float(job.get(key))
        if latency is not None:
            return latency
        if isinstance(response_payload, Mapping):
            latency = _coerce_to_float(response_payload.get(key))
            if latency is not None:
                return latency
        if isinstance(metrics, Mapping):
            latency = _coerce_to_float(metrics.get(key))
            if latency is not None:
                return latency

    if isinstance(response_payload, Mapping):
        callback_received_at = response_payload.get("callback_received_at")
    else:
        callback_received_at = None

    dispatched_at = job.get("dispatched_at") if isinstance(job, Mapping) else None
    miner_received_at = job.get("miner_received_at") if isinstance(job, Mapping) else None
    response_timestamp = job.get("response_timestamp") if isinstance(job, Mapping) else None
    completed_at = job.get("completed_at") if isinstance(job, Mapping) else None

    start_dt = _coerce_datetime(dispatched_at or miner_received_at)
    end_dt = _coerce_datetime(response_timestamp or completed_at or callback_received_at)
    if start_dt is None or end_dt is None:
        return None

    delta_ms = (end_dt - start_dt).total_seconds() * 1000.0
    return delta_ms if delta_ms >= 0 else None


def _extract_latency_ms(job: Mapping[str, Any]) -> Optional[float]:
    direct_keys = (
        "execution_duration_ms",
        "total_runtime_ms",
        "latency_ms",
    )

    for key in direct_keys:
        value = job.get(key)
        latency = _coerce_to_float(value)
        if latency is not None:
            return latency

    response_payload = job.get("response_payload")
    if isinstance(response_payload, Mapping):
        for key in direct_keys:
            latency = _coerce_to_float(response_payload.get(key))
            if latency is not None:
                return latency

    metrics = job.get("metrics")
    if isinstance(metrics, Mapping):
        for key in direct_keys:
            latency = _coerce_to_float(metrics.get(key))
            if latency is not None:
                return latency

    return None


def _coerce_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    return None


def _normalize_job_type(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip().lower()
    try:
        return str(value).strip().lower()
    except Exception:
        return ""


def _coerce_datetime(value: Any) -> Optional[datetime]:
    parsed_dt = parse_datetime(value)
    if parsed_dt is not None:
        return parsed_dt
    parsed_ts = parse_timestamp(value)
    if parsed_ts is None:
        return None
    try:
        return datetime.fromtimestamp(parsed_ts, tz=timezone.utc)
    except (OverflowError, OSError, ValueError, TypeError):
        return None


def _job_type_weight(raw_value: Any) -> float:
    normalized = _normalize_job_type(raw_value)
    return float(_JOB_TYPE_WEIGHTS.get(normalized, 0.0))


__all__ = [
    "job_to_score",
    "job_to_weighted_score",
    "job_type_has_weight",
    "job_latency_ms",
    "callback_latency_ms",
    "is_h100_kontext_job_type",
    "H100_KONTEXT_MAX_LATENCY_MS",
    "DEFAULT_MAX_LATENCY_MS",
]
