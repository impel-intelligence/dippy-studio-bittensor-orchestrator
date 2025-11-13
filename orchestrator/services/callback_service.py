
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import HTTPException, UploadFile, status

from orchestrator.common.job_store import JobStatus
from orchestrator.services.callback_uploader import BaseUploader
from orchestrator.services.job_service import JobService


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
        job_uuid = self._parse_job_uuid(job_id)
        job = await job_service.get_job(job_uuid)

        await self._verify_secret(job, provided_secret, job_service, job_uuid)

        image_bytes, image_filename, image_content_type = await self._prepare_image(
            job_service, job_uuid, image
        )
        self._log_process_start(
            job_id=job_id,
            status=status,
            completed_at=completed_at,
            image_bytes=image_bytes,
            image_filename=image_filename,
        )

        image_info = await self._upload_image(
            job_service=job_service,
            job_id=job_id,
            job_uuid=job_uuid,
            image_bytes=image_bytes,
            image_filename=image_filename,
            image_content_type=image_content_type,
        )
        if image_info.get("image_uri"):
            image_hash = await self._hash_remote_image(
                job_id=job_id,
                image_uri=str(image_info["image_uri"]),
            )
            if image_hash:
                image_info["image_sha256"] = image_hash

        latencies = self._calculate_latencies(job, received_at)
        result_payload = self._compose_result_payload(
            status=status,
            error=error,
            completed_at=completed_at,
            received_at=received_at,
            latencies=latencies,
            image_info=image_info,
            provided_secret=provided_secret,
        )

        updated_job = await job_service.update_job(job_uuid, result_payload)
        await self._audit(job_service, updated_job, job_uuid)

        response_status = await self._finalize_status(
            job_service=job_service,
            job_uuid=job_uuid,
            status=status,
            error=error,
        )
        message = f"Callback processed for job {updated_job.job_id}"
        return response_status, message

    async def _prepare_image(
        self,
        job_service: JobService,
        job_uuid: uuid.UUID,
        image: UploadFile | None,
    ) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        if image is None:
            return None, None, None

        image_bytes = await image.read()
        if not image_bytes:
            await job_service.mark_job_failure(job_uuid, "empty_image_payload")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Image payload is empty",
            )

        return image_bytes, image.filename, image.content_type

    def _log_process_start(
        self,
        *,
        job_id: str,
        status: str,
        completed_at: str,
        image_bytes: Optional[bytes],
        image_filename: Optional[str],
    ) -> None:
        logger.info(
            "callback.process_start job_id=%s status=%s completed_at=%s has_image=%s filename=%s bytes=%s",
            job_id,
            status,
            completed_at,
            bool(image_bytes),
            image_filename,
            len(image_bytes) if image_bytes is not None else 0,
        )

    async def _upload_image(
        self,
        *,
        job_service: JobService,
        job_id: str,
        job_uuid: uuid.UUID,
        image_bytes: Optional[bytes],
        image_filename: Optional[str],
        image_content_type: Optional[str],
    ) -> Dict[str, Any]:
        if image_bytes is None:
            return {}

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
            return {
                "image_uri": image_uri,
                "image_size": image_size,
                "image_content_type": image_content_type,
            }

        logger.info(
            "callback.image_upload_skipped job_id=%s filename=%s",
            job_id,
            image_filename,
        )
        return {}

    async def _hash_remote_image(self, *, job_id: str, image_uri: str) -> Optional[str]:
        fetch_url = self._resolve_fetch_url(image_uri)
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(fetch_url)
                response.raise_for_status()
        except Exception as exc:  # noqa: BLE001 - network guard
            logger.warning(
                "callback.image_hash_fetch_failed job_id=%s uri=%s resolved=%s",
                job_id,
                image_uri,
                fetch_url,
                exc_info=exc,
            )
            return None

        try:
            digest = hashlib.sha256(response.content).hexdigest()
        except Exception as exc:  # noqa: BLE001 - hashing guard
            logger.warning(
                "callback.image_hash_compute_failed job_id=%s uri=%s",
                job_id,
                image_uri,
                exc_info=exc,
            )
            return None

        logger.info(
            "callback.image_hash_computed job_id=%s uri=%s",
            job_id,
            image_uri,
        )
        return digest

    @staticmethod
    def _resolve_fetch_url(image_uri: str) -> str:
        if image_uri.startswith("gs://"):
            return f"https://storage.googleapis.com/{image_uri[5:]}"
        return image_uri

    def _calculate_latencies(
        self,
        job: Any,
        received_at: datetime,
    ) -> Dict[str, Optional[int]]:
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

        return {
            "total_runtime_ms": total_latency_ms,
            "latency_ms": dispatch_latency_ms,
        }

    def _compose_result_payload(
        self,
        *,
        status: str,
        error: Optional[str],
        completed_at: str,
        received_at: datetime,
        latencies: Dict[str, Optional[int]],
        image_info: Dict[str, Any],
        provided_secret: Optional[str],
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": status,
            "error": error,
            "completed_at": completed_at,
            "callback_received_at": received_at.isoformat(),
        }

        if latencies["total_runtime_ms"] is not None:
            payload["total_runtime_ms"] = latencies["total_runtime_ms"]
        if latencies["latency_ms"] is not None:
            payload["latency_ms"] = latencies["latency_ms"]

        if image_info.get("image_uri"):
            payload.update(image_info)

        payload["callback_metadata"] = {
            "has_image": bool(image_info.get("image_uri")),
            "secret_verified": True,
            "secret_provided": bool(provided_secret),
        }
        return payload

    async def _audit(
        self,
        job_service: JobService,
        updated_job: Any,
        job_uuid: uuid.UUID,
    ) -> None:
        try:
            await job_service.audit(updated_job)
        except Exception:  # noqa: BLE001
            logger.exception("audit.post_callback_evaluation_failed job_id=%s", job_uuid)

    async def _finalize_status(
        self,
        *,
        job_service: JobService,
        job_uuid: uuid.UUID,
        status: str,
        error: Optional[str],
    ) -> str:
        normalized_status = status.strip().lower()
        if normalized_status in {JobStatus.FAILED.value, "failed", "error"} or (error and error.strip()):
            await job_service.mark_job_failure(job_uuid, f"callback_status:{status}")
            return "error"
        return "success"

    def _parse_job_uuid(self, job_id: str) -> uuid.UUID:
        try:
            return uuid.UUID(job_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid job_id provided",
            ) from exc

    async def _verify_secret(
        self,
        job: Any,
        provided_secret: Optional[str],
        job_service: JobService,
        job_uuid: uuid.UUID,
    ) -> None:
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
