from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, Optional
from urllib import error as urllib_error

from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.job_store import JobType
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.domain.miner import Miner
from orchestrator.services.job_service import JobService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.exceptions import MinerSelectionError
TEMP_OVERRIDE_STEPS = 10


_TASK_TYPE_PAYLOAD_OVERRIDES: dict[JobType, dict[str, str]] = {
    JobType.FLUX_DEV: {
        "task_type": JobType.FLUX_DEV.value,
        "flux_mode": "dev",
    },
    JobType.FLUX_KONTEXT: {
        "task_type": JobType.FLUX_KONTEXT.value,
        "flux_mode": "kontext",
    },
}

_KONTEXT_JOB_TYPES: frozenset[JobType] = frozenset({JobType.FLUX_KONTEXT})


class ListenService:
    """Coordinate miner selection, job creation, and dispatch."""

    def __init__(
        self,
        job_service: JobService,
        metagraph: MinerMetagraphService,
        logger: StructuredLogger,
        *,
        callback_url: Optional[str] = None,
        keypair: Any | None = None,
        epistula_client: EpistulaClient | None = None,
    ) -> None:
        self._job_service = job_service
        self._metagraph = metagraph
        self._logger = logger
        self._epistula_client = epistula_client or EpistulaClient(keypair)
        self._default_callback_url = callback_url.strip() if callback_url else None

    async def process(
        self,
        *,
        job_type: JobType,
        payload: Any,
        desired_job_id: Optional[uuid.UUID],
        override_miner: Optional[Miner] = None,
    ) -> uuid.UUID:
        miner = self._select_miner(job_type, override=override_miner)
        normalized_payload = self._apply_listen_payload_overrides(job_type, payload)
        job = await self._create_job(
            job_type=job_type,
            payload=normalized_payload,
            miner=miner,
            desired_job_id=desired_job_id,
        )

        try:
            dispatch_payload = self._build_dispatch_payload(job)
            inference_url = self._resolve_inference_url(miner, job_type)
        except Exception as exc:  # noqa: BLE001 - validation guard
            await self._fail_job(
                job.job_id,
                f"prepare_failed:{type(exc).__name__}",
                event="listen.prepare_failed",
                error=str(exc),
            )
            return job.job_id

        await self._job_service.mark_job_prepared(job.job_id)
        await self._dispatch(job, miner, inference_url, dispatch_payload)
        return job.job_id

    def _select_miner(self, job_type: JobType, override: Optional[Miner] = None) -> Miner:
        if override is not None:
            return override

        miner = self._metagraph.fetch_candidate(task_type=job_type.value)
        if miner is None:
            self._logger.error(
                "listen.no_candidate",
                job_type=str(job_type),
            )
            raise MinerSelectionError("Candidate miner not found")
        return miner

    async def _create_job(
        self,
        *,
        job_type: JobType,
        payload: Any,
        miner: Miner,
        desired_job_id: Optional[uuid.UUID],
    ):
        job = await self._job_service.create_job(
            job_type=job_type,
            payload=payload,
            hotkey=miner.hotkey,
            job_id=desired_job_id,
        )
        self._logger.info(
            "job.created",
            job_id=str(job.job_id),
            job_type=str(job_type),
            miner_uid=getattr(miner, "uid", None),
            miner_hotkey=getattr(miner, "hotkey", None),
            miner_addr=getattr(miner, "network_address", None),
            miner_valid=getattr(miner, "valid", None),
            miner_alpha_stake=getattr(miner, "alpha_stake", None),
        )
        return job

    async def _dispatch(
        self,
        job: Any,
        miner: Miner,
        inference_url: str,
        payload: dict[str, Any],
    ) -> bool:
        timeout = self._resolve_dispatch_timeout(getattr(job, "job_request", None))
        try:
            status_code, response_text = await self._epistula_client.post_signed_request(
                url=inference_url,
                payload=payload,
                miner_hotkey=miner.hotkey,
                timeout=timeout,
            )
        except urllib_error.URLError as exc:
            await self._fail_job(
                job.job_id,
                f"dispatch_error:{exc.reason or type(exc).__name__}",
                event="listen.dispatch_error",
                url=inference_url,
                error=str(exc),
            )
            self._record_request_failure(miner, reason="dispatch_error")
            return False
        except Exception as exc:  # noqa: BLE001
            await self._fail_job(
                job.job_id,
                f"dispatch_error:{type(exc).__name__}",
                event="listen.dispatch_error",
                url=inference_url,
                error=str(exc),
            )
            self._record_request_failure(miner, reason="dispatch_error")
            return False

        if status_code >= 400:
            await self._fail_job(
                job.job_id,
                f"dispatch_http_{status_code}",
                event="listen.dispatch_failed",
                url=inference_url,
                status_code=status_code,
                response_preview=response_text[:200],
            )
            self._record_request_failure(miner, reason=f"dispatch_http_{status_code}")
            return False

        await self._job_service.mark_job_dispatched(job.job_id)
        self._logger.info(
            "listen.dispatch_success",
            job_id=str(job.job_id),
            url=inference_url,
            status_code=status_code,
            response_preview=response_text[:200],
        )
        return True

    async def _fail_job(
        self,
        job_id: uuid.UUID,
        reason: str,
        *,
        event: str,
        **log_fields: Any,
    ) -> None:
        await self._job_service.mark_job_failure(job_id, reason)
        self._logger.error(
            event,
            job_id=str(job_id),
            reason=reason,
            **log_fields,
        )

    def _record_request_failure(self, miner: Miner, *, reason: str) -> None:
        hotkey = getattr(miner, "hotkey", None)
        if not hotkey:
            return
        try:
            self._metagraph.record_request_failure(hotkey)
        except Exception as exc:  # pragma: no cover - defensive guard
            self._logger.debug(
                "listen.record_request_failure_failed",
                hotkey=hotkey,
                reason=reason,
                error=str(exc),
            )

    def _resolve_inference_url(self, miner: Miner, job_type: JobType, *, sync: bool = False) -> str:
        address = (miner.network_address or "").strip()
        # img-h100* jobs must hit the edit endpoint (including any string aliases)
        uses_edit = self._is_kontext_job_type(job_type)

        if sync:
            endpoint = "/sync/edit" if uses_edit else "/sync/inference"
        else:
            endpoint = "/edit" if uses_edit else "/inference"

        base = address.rstrip("/")
        if base.endswith(endpoint):
            return base or endpoint

        return f"{base}{endpoint}"

    @staticmethod
    def _resolve_dispatch_timeout(job_request: Any) -> int:
        """Use longer HTTP timeouts for slower edit jobs."""
        job_type_value = ""
        if job_request is not None:
            job_type_value = getattr(job_request, "job_type", "") or ""
        job_type_str = job_type_value.value if isinstance(job_type_value, JobType) else str(job_type_value)
        if "img-h100" in job_type_str.lower():
            return 60
        return 20

    def _build_dispatch_payload(self, job: Any) -> dict[str, Any]:
        if not hasattr(job, "job_request"):
            raise ValueError("Invalid job provided for dispatch")

        base_payload: Any = getattr(job.job_request, "payload", None)
        if not isinstance(base_payload, dict):
            raise ValueError("Job payload must be a JSON object")

        payload_copy = deepcopy(base_payload)
        payload_copy.setdefault("job_id", str(job.job_id))

        secret = getattr(job, "callback_secret", None)
        if not secret:
            raise ValueError("Job callback secret is missing")
        payload_copy["callback_secret"] = secret

        seed = getattr(job, "prompt_seed", None)
        if seed is not None:
            payload_copy["seed"] = seed

        callback_url = payload_copy.get("callback_url")
        if not isinstance(callback_url, str) or not callback_url.strip():
            if self._default_callback_url:
                callback_url = self._default_callback_url
                payload_copy["callback_url"] = callback_url
            else:
                raise ValueError("Job payload missing callback_url")

        payload_copy["callback_url"] = callback_url
        self._apply_task_type_overrides(job, payload_copy)
        return payload_copy

    def _apply_listen_payload_overrides(self, job_type: JobType, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload

        if not self._is_kontext_job_type(job_type):
            return payload

        normalized_steps = self._normalize_inference_steps(payload.get("num_inference_steps"))
        if normalized_steps is None or normalized_steps <= TEMP_OVERRIDE_STEPS:
            return payload

        capped_payload = deepcopy(payload)
        capped_payload["num_inference_steps"] = TEMP_OVERRIDE_STEPS
        return capped_payload

    @staticmethod
    def _normalize_inference_steps(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            steps = int(value)
        except (TypeError, ValueError):
            return None
        return steps

    @staticmethod
    def _is_kontext_job_type(job_type: JobType | str | None) -> bool:
        if job_type in _KONTEXT_JOB_TYPES:
            return True

        job_type_value = ""
        if isinstance(job_type, JobType):
            job_type_value = job_type.value
        elif job_type is not None:
            job_type_value = str(job_type)

        normalized = job_type_value.lower()
        return "img-h100" in normalized or "kontext" in normalized

    def _apply_task_type_overrides(self, job: Any, payload: dict[str, Any]) -> None:
        job_type = getattr(getattr(job, "job_request", None), "job_type", None)
        try:
            job_type = JobType(job_type) if job_type is not None else None
        except ValueError:
            job_type = None

        if job_type is None:
            return

        overrides = _TASK_TYPE_PAYLOAD_OVERRIDES.get(job_type)
        if overrides is None:
            return

        for key, value in overrides.items():
            payload.setdefault(key, value)
