
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, UploadFile, status

from orchestrator.services.callback_uploader import BaseUploader
from orchestrator.services.job_service import JobService
from orchestrator.common.job_store import JobStatus


CALLBACK_SECRET_HEADER = "X-Callback-Secret"

logger = logging.getLogger(__name__)


class CallbackService:
    def __init__(self, uploader: Optional[BaseUploader] = None) -> None:
        self._uploader = uploader or BaseUploader()

    async def process_callback(
        self,
        *,
        job_service: JobService,
        job_id: str,
        status: str,
        completed_at: str,
        error: Optional[str],
        provided_secret: Optional[str],
        image: UploadFile | None,
        received_at: Optional[datetime] = None,
    ) -> Tuple[str, str]:

        received_at = received_at or datetime.now(timezone.utc)

        try:
            job_uuid = uuid.UUID(job_id)
        except ValueError as exc:  # Invalid UUID format
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid job_id provided",
            ) from exc

        job = await job_service.get_job(job_uuid)

        expected_secret = getattr(job, "callback_secret", None)
        if not expected_secret:
            await job_service.mark_job_failure(job_uuid, "callback_secret_missing")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Job callback secret is not configured",
            )

        if provided_secret != expected_secret:
            await job_service.mark_job_failure(job_uuid, "callback_secret_mismatch")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Callback secret mismatch",
            )

        image_bytes: Optional[bytes] = None
        image_filename: Optional[str] = None
        image_content_type: Optional[str] = None
        if image is not None:
            image_filename = image.filename
            image_content_type = image.content_type
            image_bytes = await image.read()
            if not image_bytes:
                await job_service.mark_job_failure(job_uuid, "empty_image_payload")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Image payload is empty",
                )

        has_image = bool(image_bytes)
        logger.info(
            "callback.process_start job_id=%s status=%s completed_at=%s has_image=%s filename=%s bytes=%s",
            job_id,
            status,
            completed_at,
            has_image,
            image_filename,
            len(image_bytes) if image_bytes is not None else 0,
        )

        image_uri: Optional[str] = None
        image_size: Optional[int] = None
        if has_image and image_bytes is not None:
            try:
                image_uri = self._uploader.upload_bytes(
                    job_id=job_id,
                    content=image_bytes,
                    filename=image_filename,
                    content_type=image_content_type,
                )
            except Exception as exc:  # noqa: BLE001 - uploader failure guard
                logger.exception(
                    "callback.image_upload_failed job_id=%s filename=%s",
                    job_id,
                    image_filename,
                )
                await job_service.mark_job_failure(job_uuid, "callback_upload_failed")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to upload callback image",
                ) from exc

            if image_uri:
                image_size = len(image_bytes)
                logger.info(
                    "callback.image_uploaded job_id=%s uri=%s bytes=%s",
                    job_id,
                    image_uri,
                    image_size,
                )
            else:
                logger.info(
                    "callback.image_upload_skipped job_id=%s filename=%s",
                    job_id,
                    image_filename,
                )

        total_latency_ms: Optional[int] = None
        dispatch_latency_ms: Optional[int] = None

        created_ts = getattr(job.job_request, "timestamp", None)
        if isinstance(created_ts, (int, float)):
            created_dt = datetime.fromtimestamp(created_ts, tz=timezone.utc)
            total_latency_ms = int((received_at - created_dt).total_seconds() * 1000)

        dispatched_ts = getattr(job, "dispatched_at", None)
        if isinstance(dispatched_ts, (int, float)):
            dispatched_dt = datetime.fromtimestamp(dispatched_ts, tz=timezone.utc)
            dispatch_latency_ms = int((received_at - dispatched_dt).total_seconds() * 1000)

        result_payload: Dict[str, Any] = {
            "status": status,
            "error": error,
            "completed_at": completed_at,
            "callback_received_at": received_at.isoformat(),
        }

        if total_latency_ms is not None:
            result_payload["total_runtime_ms"] = total_latency_ms
        if dispatch_latency_ms is not None:
            result_payload["latency_ms"] = dispatch_latency_ms

        if image_uri:
            result_payload["image_uri"] = image_uri
            result_payload["image_size"] = image_size
            result_payload["image_content_type"] = image_content_type

        result_payload["callback_metadata"] = {
            "has_image": bool(image_uri),
            "secret_verified": True,
            "secret_provided": bool(provided_secret),
        }

        updated_job = await job_service.update_job(job_uuid, result_payload)

        try:
            await job_service.audit(updated_job)
        except Exception:  # noqa: BLE001
            logger.exception("audit.post_callback_evaluation_failed job_id=%s", job_uuid)

        normalized_status = status.strip().lower()
        if normalized_status in {JobStatus.FAILED.value, "failed", "error"} or (error and error.strip()):
            await job_service.mark_job_failure(job_uuid, f"callback_status:{status}")
            response_status = "error"
        else:
            response_status = "success"

        message = f"Callback processed for job {updated_job.job_id}"
        return response_status, message

    async def list_callbacks(
        self,
        *,
        job_service: JobService,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return recent callbacks persisted via the job service."""

        job_records = await job_service.dump_jobs(limit=limit)
        callbacks: List[Dict[str, Any]] = []
        for record in job_records:
            payload = record.response_payload or {}
            callback_received_at = payload.get("callback_received_at")
            if not callback_received_at:
                continue

            entry: Dict[str, Any] = {
                "job_id": str(record.job_id),
                "status": payload.get("status"),
                "error": payload.get("error"),
                "completed_at": payload.get("completed_at"),
                "callback_received_at": callback_received_at,
                "callback_metadata": payload.get("callback_metadata", {}),
                "image_uri": payload.get("image_uri"),
                "image_size": payload.get("image_size"),
                "image_content_type": payload.get("image_content_type"),
            }
            callbacks.append(entry)

        return callbacks


__all__ = ["CallbackService", "CALLBACK_SECRET_HEADER"]
