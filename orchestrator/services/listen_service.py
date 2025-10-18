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



class ListenService:

    def __init__(
        self,
        job_service: JobService,
        metagraph: LiveMinerMetagraphClient,
        callback_url: Optional[str] = None,
        keypair: Any | None = None,
    ) -> None:
        self.job_service = job_service
        self.metagraph = metagraph
        self.epistula_client = EpistulaClient(keypair)
        self.default_callback_url = callback_url.strip() if isinstance(callback_url, str) else None

    async def process(
        self,
        *,
        job_type: JobType,
        payload: Any,
        desired_job_id: Optional[uuid.UUID],
        slog: StructuredLogger,
    ) -> uuid.UUID:
        miner = self.metagraph.fetch_candidate()
        if miner is None:
            slog.warning(
                "listen.no_candidate",
                job_type=str(job_type),
                reason="no_valid_miners",
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No valid candidate miner available",
            )

        job = await self.job_service.create_job(
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

        dispatch_payload: dict[str, Any]
        inference_url: str
        try:
            dispatch_payload = self._build_dispatch_payload(job)
            inference_url = self._resolve_inference_url(miner)
        except Exception as exc:  # noqa: BLE001 - capture validation errors
            reason = f"prepare_failed:{type(exc).__name__}"
            await self.job_service.mark_job_failure(job.job_id, reason)
            slog.error(
                "listen.prepare_failed",
                job_id=str(job.job_id),
                error=str(exc),
            )
            return job.job_id

        await self.job_service.mark_job_prepared(job.job_id)

        try:
            status_code, response_text = await self.epistula_client.post_signed_request(
                url=inference_url,
                payload=dispatch_payload,
                miner_hotkey=miner.hotkey,
            )
        except urllib_error.URLError as exc:
            reason = f"dispatch_error:{exc.reason or type(exc).__name__}"
            await self.job_service.mark_job_failure(job.job_id, reason)
            slog.error(
                "listen.dispatch_error",
                job_id=str(job.job_id),
                url=inference_url,
                error=str(exc),
            )
            return job.job_id
        except Exception as exc:  # noqa: BLE001
            reason = f"dispatch_error:{type(exc).__name__}"
            await self.job_service.mark_job_failure(job.job_id, reason)
            slog.error(
                "listen.dispatch_error",
                job_id=str(job.job_id),
                url=inference_url,
                error=str(exc),
            )
            return job.job_id

        if status_code >= 400:
            reason = f"dispatch_http_{status_code}"
            await self.job_service.mark_job_failure(job.job_id, reason)
            slog.error(
                "listen.dispatch_failed",
                job_id=str(job.job_id),
                url=inference_url,
                status_code=status_code,
                response_preview=response_text[:200],
            )
            return job.job_id

        await self.job_service.mark_job_dispatched(job.job_id)
        slog.info(
            "listen.dispatch_success",
            job_id=str(job.job_id),
            url=inference_url,
            status_code=status_code,
            response_preview=response_text[:200],
        )

        return job.job_id

    def _resolve_inference_url(self, miner: Miner) -> str:
        address = miner.network_address
        address = address.strip()
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
            if self.default_callback_url:
                callback_url = self.default_callback_url
                payload_copy["callback_url"] = callback_url
            else:
                raise ValueError("Job payload missing callback_url")

        payload_copy["callback_url"] = callback_url
        return payload_copy
