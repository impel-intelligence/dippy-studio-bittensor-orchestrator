from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, Optional
from urllib import error as urllib_error

from fastapi import HTTPException, status

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.job_store import JobType
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.services.job_service import JobService


class ListenEngine:
    """Coordinate miner selection, job creation, and dispatch."""

    def __init__(
        self,
        job_service: JobService,
        metagraph: LiveMinerMetagraphClient,
        *,
        epistula_client: EpistulaClient,
        default_callback_url: str | None = None,
    ) -> None:
        self._job_service = job_service
        self._metagraph = metagraph
        self._epistula_client = epistula_client
        self._default_callback_url = default_callback_url.strip() if default_callback_url else None

    async def process(
        self,
        *,
        job_type: JobType,
        payload: Any,
        desired_job_id: Optional[uuid.UUID],
        slog: StructuredLogger,
    ) -> uuid.UUID:
        miner = self._select_miner(job_type, slog)
        job = await self._create_job(
            job_type=job_type,
            payload=payload,
            miner=miner,
            desired_job_id=desired_job_id,
            slog=slog,
        )

        try:
            dispatch_payload = self._build_dispatch_payload(job)
            inference_url = self._resolve_inference_url(miner)
        except Exception as exc:  # noqa: BLE001 - validation guard
            await self._fail_job(
                job.job_id,
                f"prepare_failed:{type(exc).__name__}",
                slog,
                event="listen.prepare_failed",
                error=str(exc),
            )
            return job.job_id

        await self._job_service.mark_job_prepared(job.job_id)
        await self._dispatch(job, miner, inference_url, dispatch_payload, slog)
        return job.job_id

    def _select_miner(self, job_type: JobType, slog: StructuredLogger) -> Miner:
        miner = self._metagraph.fetch_candidate()
        if miner is None:
            slog.error(
                "listen.no_candidate",
                job_type=str(job_type),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No candidate miner available",
            )
        return miner

    async def _create_job(
        self,
        *,
        job_type: JobType,
        payload: Any,
        miner: Miner,
        desired_job_id: Optional[uuid.UUID],
        slog: StructuredLogger,
    ):
        job = await self._job_service.create_job(
            job_type=job_type,
            payload=payload,
            hotkey=miner.hotkey,
            job_id=desired_job_id,
        )
        slog.info(
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
        slog: StructuredLogger,
    ) -> None:
        try:
            status_code, response_text = await self._epistula_client.post_signed_request(
                url=inference_url,
                payload=payload,
                miner_hotkey=miner.hotkey,
            )
        except urllib_error.URLError as exc:
            await self._fail_job(
                job.job_id,
                f"dispatch_error:{exc.reason or type(exc).__name__}",
                slog,
                event="listen.dispatch_error",
                url=inference_url,
                error=str(exc),
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._fail_job(
                job.job_id,
                f"dispatch_error:{type(exc).__name__}",
                slog,
                event="listen.dispatch_error",
                url=inference_url,
                error=str(exc),
            )
            return

        if status_code >= 400:
            await self._fail_job(
                job.job_id,
                f"dispatch_http_{status_code}",
                slog,
                event="listen.dispatch_failed",
                url=inference_url,
                status_code=status_code,
                response_preview=response_text[:200],
            )
            return

        await self._job_service.mark_job_dispatched(job.job_id)
        slog.info(
            "listen.dispatch_success",
            job_id=str(job.job_id),
            url=inference_url,
            status_code=status_code,
            response_preview=response_text[:200],
        )

    async def _fail_job(
        self,
        job_id: uuid.UUID,
        reason: str,
        slog: StructuredLogger,
        *,
        event: str,
        **log_fields: Any,
    ) -> None:
        await self._job_service.mark_job_failure(job_id, reason)
        slog.error(
            event,
            job_id=str(job_id),
            reason=reason,
            **log_fields,
        )

    def _resolve_inference_url(self, miner: Miner) -> str:
        address = (miner.network_address or "").strip()
        if address.endswith("/inference"):
            return address
        base = address.rstrip("/")
        return f"{base}/inference"

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
        return payload_copy


class ListenService:
    """Backward-compatible facade over ListenEngine."""

    def __init__(
        self,
        job_service: JobService,
        metagraph: LiveMinerMetagraphClient,
        callback_url: Optional[str] = None,
        keypair: Any | None = None,
    ) -> None:
        epistula_client = EpistulaClient(keypair)
        self._engine = ListenEngine(
            job_service=job_service,
            metagraph=metagraph,
            epistula_client=epistula_client,
            default_callback_url=callback_url,
        )

    async def process(
        self,
        *,
        job_type: JobType,
        payload: Any,
        desired_job_id: Optional[uuid.UUID],
        slog: StructuredLogger,
    ) -> uuid.UUID:
        return await self._engine.process(
            job_type=job_type,
            payload=payload,
            desired_job_id=desired_job_id,
            slog=slog,
        )
