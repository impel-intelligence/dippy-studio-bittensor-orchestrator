"""OpenTelemetry metrics for image generation jobs.

This module provides OTEL metrics for tracking job lifecycle events,
latencies, and outcomes in the orchestrator service.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter, Histogram, Meter

logger = logging.getLogger(__name__)

# Global meter and instruments (initialized lazily)
_meter: "Meter | None" = None
_jobs_created: "Counter | None" = None
_jobs_completed: "Counter | None" = None
_jobs_failed: "Counter | None" = None
_job_latency: "Histogram | None" = None
_job_dispatch_latency: "Histogram | None" = None
_callback_processing_time: "Histogram | None" = None
_image_upload_time: "Histogram | None" = None


def _get_meter() -> "Meter | None":
    """Get or create the OTEL meter for orchestrator metrics."""
    global _meter
    if _meter is not None:
        return _meter

    try:
        from opentelemetry import metrics
        _meter = metrics.get_meter("orchestrator.jobs", version="1.0.0")
        return _meter
    except Exception as e:
        logger.debug("Failed to get OTEL meter: %s", e)
        return None


def _ensure_instruments() -> bool:
    """Lazily initialize all metric instruments. Returns True if successful."""
    global _jobs_created, _jobs_completed, _jobs_failed
    global _job_latency, _job_dispatch_latency
    global _callback_processing_time, _image_upload_time

    if _jobs_created is not None:
        return True

    meter = _get_meter()
    if meter is None:
        return False

    try:
        _jobs_created = meter.create_counter(
            name="orchestrator.jobs.created",
            description="Total number of jobs created",
            unit="1",
        )

        _jobs_completed = meter.create_counter(
            name="orchestrator.jobs.completed",
            description="Total number of jobs completed successfully",
            unit="1",
        )

        _jobs_failed = meter.create_counter(
            name="orchestrator.jobs.failed",
            description="Total number of jobs that failed",
            unit="1",
        )

        _job_latency = meter.create_histogram(
            name="orchestrator.jobs.latency",
            description="End-to-end job latency from creation to completion",
            unit="ms",
        )

        _job_dispatch_latency = meter.create_histogram(
            name="orchestrator.jobs.dispatch_latency",
            description="Time from job creation to dispatch to miner",
            unit="ms",
        )

        _callback_processing_time = meter.create_histogram(
            name="orchestrator.callbacks.processing_time",
            description="Time to process a callback (validation, upload, etc.)",
            unit="ms",
        )

        _image_upload_time = meter.create_histogram(
            name="orchestrator.images.upload_time",
            description="Time to upload result images to storage",
            unit="ms",
        )

        logger.info("OTEL job metrics initialized successfully")
        return True
    except Exception as e:
        logger.warning("Failed to initialize OTEL job metrics: %s", e)
        return False


def record_job_created(job_type: str, miner_hotkey: str) -> None:
    """Record a job creation event."""
    if not _ensure_instruments() or _jobs_created is None:
        return

    try:
        _jobs_created.add(1, {
            "job_type": job_type,
            "miner_hotkey": miner_hotkey,
        })
    except Exception as e:
        logger.debug("Failed to record job_created metric: %s", e)


def record_job_completed(
    job_type: str,
    miner_hotkey: str,
    latency_ms: float | None = None,
) -> None:
    """Record a successful job completion."""
    if not _ensure_instruments():
        return

    attributes = {
        "job_type": job_type,
        "miner_hotkey": miner_hotkey,
    }

    try:
        if _jobs_completed is not None:
            _jobs_completed.add(1, attributes)

        if latency_ms is not None and _job_latency is not None:
            _job_latency.record(latency_ms, attributes)
    except Exception as e:
        logger.debug("Failed to record job_completed metric: %s", e)


def record_job_failed(
    job_type: str,
    miner_hotkey: str,
    failure_reason: str | None = None,
) -> None:
    """Record a job failure."""
    if not _ensure_instruments() or _jobs_failed is None:
        return

    try:
        _jobs_failed.add(1, {
            "job_type": job_type,
            "miner_hotkey": miner_hotkey,
            "failure_reason": failure_reason or "unknown",
        })
    except Exception as e:
        logger.debug("Failed to record job_failed metric: %s", e)


def record_dispatch_latency(job_type: str, latency_ms: float) -> None:
    """Record the time from job creation to dispatch."""
    if not _ensure_instruments() or _job_dispatch_latency is None:
        return

    try:
        _job_dispatch_latency.record(latency_ms, {"job_type": job_type})
    except Exception as e:
        logger.debug("Failed to record dispatch_latency metric: %s", e)


def record_callback_processing_time(job_type: str, duration_ms: float, status: str) -> None:
    """Record the time to process a callback."""
    if not _ensure_instruments() or _callback_processing_time is None:
        return

    try:
        _callback_processing_time.record(duration_ms, {
            "job_type": job_type,
            "status": status,
        })
    except Exception as e:
        logger.debug("Failed to record callback_processing_time metric: %s", e)


def record_image_upload_time(job_type: str, duration_ms: float) -> None:
    """Record the time to upload an image."""
    if not _ensure_instruments() or _image_upload_time is None:
        return

    try:
        _image_upload_time.record(duration_ms, {"job_type": job_type})
    except Exception as e:
        logger.debug("Failed to record image_upload_time metric: %s", e)


class CallbackTimer:
    """Context manager for timing callback processing."""

    def __init__(self, job_type: str):
        self.job_type = job_type
        self.start_time: float | None = None
        self.status = "success"

    def __enter__(self) -> "CallbackTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is None:
            return

        duration_ms = (time.perf_counter() - self.start_time) * 1000
        if exc_type is not None:
            self.status = "error"

        record_callback_processing_time(self.job_type, duration_ms, self.status)

    def set_status(self, status: str) -> None:
        """Set the status for this callback (success, failure, error)."""
        self.status = status


class ImageUploadTimer:
    """Context manager for timing image uploads."""

    def __init__(self, job_type: str):
        self.job_type = job_type
        self.start_time: float | None = None

    def __enter__(self) -> "ImageUploadTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.start_time is None:
            return

        duration_ms = (time.perf_counter() - self.start_time) * 1000
        record_image_upload_time(self.job_type, duration_ms)
