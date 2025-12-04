
from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import UploadFile

from orchestrator.common.job_store import JobStatus
from orchestrator.clients.storage import BaseUploader
from orchestrator.services.job_service import JobService
from orchestrator.services.sync_waiter import SyncCallbackResult, SyncCallbackWaiter
from orchestrator.services.job_scoring import (
    H100_KONTEXT_MAX_LATENCY_MS,
    is_h100_kontext_job_type,
)
from orchestrator.services.webhook_dispatcher import WebhookDispatcher
from orchestrator.services.exceptions import (
    CallbackImageError,
    CallbackImageUploadError,
    CallbackSecretMismatch,
    CallbackSecretMissing,
    CallbackValidationError,
)


CALLBACK_SECRET_HEADER = "X-Callback-Secret"

logger = logging.getLogger(__name__)

_FLUX_KONTEXT_MIN_RUNTIME_MS = 8_000

class CallbackService:
    def __init__(
        self,
        uploader: Optional[BaseUploader] = None,
        *,
        sync_waiter: Optional["SyncCallbackWaiter"] = None,
        webhook_dispatcher: Optional[WebhookDispatcher] = None,
    ) -> None:
        self._uploader = uploader or BaseUploader()
        self._sync_waiter = sync_waiter
        self._webhook_dispatcher = webhook_dispatcher

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
        try:
            job = await job_service.get_job(job_uuid)
        except Exception:
            logger.exception("callback.job_fetch_failed job_id=%s", job_id)
            raise

        is_audit_job = self._is_audit_job(job)
        job_type = self._extract_job_type(job)

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
            job_type=job_type,
        )

        image_info = await self._upload_image(
            job_service=job_service,
            job_id=job_id,
            job_uuid=job_uuid,
            image_bytes=image_bytes,
            image_filename=image_filename,
            image_content_type=image_content_type,
            job_type=job_type,
        )
        if image_info.get("image_uri"):
            image_hash = await self._hash_remote_image(
                job_id=job_id,
                image_uri=str(image_info["image_uri"]),
            )
            if image_hash:
                image_info["image_sha256"] = image_hash

        latencies = self._calculate_latencies(job, received_at)
        latency_failure_reason: Optional[str] = None
        if is_audit_job:
            logger.info("callback.audit_job_validations_skipped job_id=%s", job_uuid)
        else:
            latency_failure_reason = self._detect_flux_kontext_latency_violation(
                job_type=job_type,
                dispatch_latency_ms=latencies.get("latency_ms"),
                job_uuid=job_uuid,
            )
            await self._enforce_flux_kontext_runtime(
                job_type=job_type,
                total_runtime_ms=latencies.get("total_runtime_ms"),
                job_service=job_service,
                job_uuid=job_uuid,
            )
        webhook_url = self._extract_webhook_url(job)
        webhook_forwarded = self._forward_to_webhook(
            webhook_url=webhook_url,
            job_id=job_id,
            status=status,
            completed_at=completed_at,
            error=error,
            latencies=latencies,
            image_bytes=image_bytes,
            image_filename=image_filename,
            image_content_type=image_content_type,
        )
        result_payload = self._compose_result_payload(
            status=status,
            error=error,
            completed_at=completed_at,
            received_at=received_at,
            latencies=latencies,
            image_info=image_info,
            provided_secret=provided_secret,
            webhook_forwarded=webhook_forwarded,
        )

        updated_job = await job_service.update_job(job_uuid, result_payload)
        latency_snapshot = {k: v for k, v in latencies.items() if v is not None}
        logger.info(
            "callback.job_updated job_id=%s status=%s has_image=%s latencies=%s",
            job_id,
            status,
            bool(image_info.get("image_uri")),
            latency_snapshot or None,
        )

        response_status = await self._finalize_status(
            job_service=job_service,
            job_uuid=job_uuid,
            status=status,
            error=error,
            forced_failure_reason=latency_failure_reason,
        )

        await self._notify_sync_waiter(
            job_service=job_service,
            job_uuid=job_uuid,
            image_bytes=image_bytes,
            image_content_type=image_content_type,
        )
        message = f"Callback processed for job {updated_job.job_id}"
        return response_status, message

    async def _notify_sync_waiter(
        self,
        *,
        job_service: JobService,
        job_uuid: uuid.UUID,
        image_bytes: Optional[bytes],
        image_content_type: Optional[str],
    ) -> None:
        if self._sync_waiter is None:
            return
        try:
            final_job = await job_service.get_job(job_uuid)
        except Exception:  # pragma: no cover - defensive
            logger.exception("callback.sync_waiter.job_fetch_failed job_id=%s", job_uuid)
            return

        payload = getattr(getattr(final_job, "job_response", None), "payload", None)
        failure_reason = getattr(final_job, "failure_reason", None)

        result = SyncCallbackResult(
            job_id=job_uuid,
            status=final_job.status,
            payload=payload,
            image_bytes=image_bytes,
            content_type=image_content_type,
            failure_reason=failure_reason,
            error=None,
        )
        try:
            await self._sync_waiter.resolve(result)
        except Exception:  # pragma: no cover - defensive
            logger.exception("callback.sync_waiter.resolve_failed job_id=%s", job_uuid)

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
            logger.warning(
                "callback.image_payload_empty job_id=%s filename=%s",
                job_uuid,
                image.filename if image else None,
            )
            await job_service.mark_job_failure(job_uuid, "empty_image_payload")
            raise CallbackImageError("Image payload is empty")

        return image_bytes, image.filename, image.content_type

    def _log_process_start(
        self,
        *,
        job_id: str,
        status: str,
        completed_at: str,
        image_bytes: Optional[bytes],
        image_filename: Optional[str],
        job_type: Optional[str],
    ) -> None:
        logger.info(
            "callback.process_start job_id=%s status=%s completed_at=%s has_image=%s filename=%s bytes=%s job_type=%s",
            job_id,
            status,
            completed_at,
            bool(image_bytes),
            image_filename,
            len(image_bytes) if image_bytes is not None else 0,
            job_type,
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
        job_type: Optional[str],
    ) -> Dict[str, Any]:
        if image_bytes is None:
            return {}

        uploader_name = type(self._uploader).__name__
        logger.info(
            "callback.image_upload_attempt job_id=%s filename=%s content_type=%s bytes=%s uploader=%s job_type=%s",
            job_id,
            image_filename,
            image_content_type,
            len(image_bytes),
            uploader_name,
            job_type,
        )
        try:
            image_uri = self._uploader.upload_bytes(
                job_id=job_id,
                content=image_bytes,
                filename=image_filename,
                content_type=image_content_type,
                job_type=job_type,
            )
        except Exception as exc:  # noqa: BLE001 - uploader failure guard
            logger.exception(
                "callback.image_upload_failed job_id=%s filename=%s uploader=%s error=%s job_type=%s",
                job_id,
                image_filename,
                uploader_name,
                exc,
                job_type,
            )
            await job_service.mark_job_failure(job_uuid, "callback_upload_failed")
            raise CallbackImageUploadError("Failed to upload callback image") from exc

        if image_uri:
            image_size = len(image_bytes)
            logger.info(
                "callback.image_uploaded job_id=%s uri=%s bytes=%s job_type=%s",
                job_id,
                image_uri,
                image_size,
                job_type,
            )
            return {
                "image_uri": image_uri,
                "image_size": image_size,
                "image_content_type": image_content_type,
            }

        logger.info(
            "callback.image_upload_skipped job_id=%s filename=%s job_type=%s",
            job_id,
            image_filename,
            job_type,
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

    @staticmethod
    def _extract_job_type(job: Any) -> Optional[str]:
        job_request = getattr(job, "job_request", None)
        if job_request is None:
            return None
        raw_job_type = getattr(job_request, "job_type", None)
        if raw_job_type is None:
            return None
        value = getattr(raw_job_type, "value", raw_job_type)
        text = str(value).strip()
        return text or None

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

    def _detect_flux_kontext_latency_violation(
        self,
        *,
        job_type: Optional[str],
        dispatch_latency_ms: Optional[int],
        job_uuid: uuid.UUID,
    ) -> Optional[str]:
        if not is_h100_kontext_job_type(job_type):
            return None
        if dispatch_latency_ms is None:
            return None
        if dispatch_latency_ms <= H100_KONTEXT_MAX_LATENCY_MS:
            return None

        logger.warning(
            "callback.flux_kontext_latency_violation job_id=%s job_type=%s elapsed_ms=%s max_ms=%s",
            job_uuid,
            job_type,
            dispatch_latency_ms,
            H100_KONTEXT_MAX_LATENCY_MS,
        )
        return "flux_kontext_max_latency_violation"

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
        webhook_forwarded: bool = False,
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

        callback_metadata = {
            "has_image": bool(image_info.get("image_uri")),
            "secret_verified": True,
            "secret_provided": bool(provided_secret),
        }
        callback_metadata["webhook_forwarded"] = webhook_forwarded
        payload["callback_metadata"] = callback_metadata
        return payload

    async def _finalize_status(
        self,
        *,
        job_service: JobService,
        job_uuid: uuid.UUID,
        status: str,
        error: Optional[str],
        forced_failure_reason: Optional[str] = None,
    ) -> str:
        normalized_status = status.strip().lower()
        failure_reason = forced_failure_reason
        if failure_reason is None and (
            normalized_status in {JobStatus.FAILED.value, "failed", "error"} or (error and error.strip())
        ):
            failure_reason = f"callback_status:{status}"

        if failure_reason is not None:
            logger.warning(
                "callback.finalize_status_failure job_id=%s status=%s error=%s failure_reason=%s",
                job_uuid,
                status,
                error,
                failure_reason,
            )
            await job_service.mark_job_failure(job_uuid, failure_reason)
            return "error"
        return "success"

    @staticmethod
    def _is_audit_job(job: Any) -> bool:
        return bool(getattr(job, "is_audit_job", False))

    def _parse_job_uuid(self, job_id: str) -> uuid.UUID:
        try:
            return uuid.UUID(job_id)
        except ValueError as exc:
            raise CallbackValidationError("Invalid job_id provided") from exc

    async def _verify_secret(
        self,
        job: Any,
        provided_secret: Optional[str],
        job_service: JobService,
        job_uuid: uuid.UUID,
    ) -> None:
        expected_secret = getattr(job, "callback_secret", None)
        if not expected_secret:
            logger.error("callback.secret_missing job_id=%s", job_uuid)
            await job_service.mark_job_failure(job_uuid, "callback_secret_missing")
            raise CallbackSecretMissing("Job callback secret is not configured")

        if provided_secret != expected_secret:
            logger.warning(
                "callback.secret_mismatch job_id=%s secret_provided=%s",
                job_uuid,
                bool(provided_secret),
            )
            await job_service.mark_job_failure(job_uuid, "callback_secret_mismatch")
            raise CallbackSecretMismatch("Callback secret mismatch")

    async def _enforce_flux_kontext_runtime(
        self,
        *,
        job_type: Optional[str],
        total_runtime_ms: Optional[int],
        job_service: JobService,
        job_uuid: uuid.UUID,
    ) -> None:
        if not is_h100_kontext_job_type(job_type):
            return
        if total_runtime_ms is None:
            return
        if total_runtime_ms >= _FLUX_KONTEXT_MIN_RUNTIME_MS:
            return

        logger.warning(
            "callback.flux_kontext_runtime_violation job_id=%s job_type=%s elapsed_ms=%s min_required_ms=%s",
            job_uuid,
            job_type,
            total_runtime_ms,
            _FLUX_KONTEXT_MIN_RUNTIME_MS,
        )
        await job_service.mark_job_failure(job_uuid, "flux_kontext_min_runtime_violation")
        raise CallbackValidationError("Flux-Kontext jobs must run for at least 10 seconds before completing")

    @staticmethod
    def _extract_webhook_url(job: Any) -> Optional[str]:
        payload = getattr(getattr(job, "job_request", None), "payload", None)
        if not isinstance(payload, dict):
            return None
        url = payload.get("webhook_url")
        if not isinstance(url, str):
            return None
        normalized = url.strip()
        return normalized or None

    def _forward_to_webhook(
        self,
        *,
        webhook_url: Optional[str],
        job_id: str,
        status: str,
        completed_at: str,
        error: Optional[str],
        latencies: Dict[str, Optional[int]],
        image_bytes: Optional[bytes],
        image_filename: Optional[str],
        image_content_type: Optional[str],
    ) -> bool:
        if not webhook_url:
            return False

        if self._webhook_dispatcher is None:
            logger.warning(
                "callback.webhook_dispatcher_missing job_id=%s webhook_url=%s",
                job_id,
                webhook_url,
            )
            return False

        try:
            dispatched = self._webhook_dispatcher.dispatch(
                webhook_url=webhook_url,
                job_id=job_id,
                status=status,
                completed_at=completed_at,
                error=error,
                latencies=latencies,
                image_bytes=image_bytes,
                image_filename=image_filename,
                image_content_type=image_content_type,
            )
        except Exception:  # noqa: BLE001 - defensive guard
            logger.exception(
                "callback.webhook_dispatch_error job_id=%s webhook_url=%s",
                job_id,
                webhook_url,
            )
            return False

        if dispatched:
            logger.info(
                "callback.webhook_forward_queued job_id=%s webhook_url=%s has_image=%s",
                job_id,
                webhook_url,
                bool(image_bytes),
            )
        else:
            logger.warning(
                "callback.webhook_forward_not_dispatched job_id=%s webhook_url=%s",
                job_id,
                webhook_url,
            )
        return dispatched

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
