from __future__ import annotations

from typing import Any, Mapping, Optional

DEFAULT_MAX_LATENCY_MS = 60_000.0


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


__all__ = ["job_to_score", "DEFAULT_MAX_LATENCY_MS"]
