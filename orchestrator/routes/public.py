from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from pydantic import AnyHttpUrl, BaseModel

from orchestrator.domain.miner import Miner
from orchestrator.common.job_store import JobStatus, JobType
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.config import OrchestratorConfig
from orchestrator.dependencies import (
    get_audit_failure_repository,
    get_callback_service,
    get_config,
    get_health_service,
    get_job_service,
    get_listen_service,
    get_miner_metagraph_service,
    get_score_service,
    get_sync_callback_waiter,
    get_structured_logger,
)
from orchestrator.routes.internal import MetagraphDumpResponse
from orchestrator.routes.error_mapping import (
    raise_callback_service_error,
    raise_job_service_error,
    raise_listen_service_error,
)
from orchestrator.schemas.job import RecentJobsResponse
from orchestrator.schemas.scores import ScorePayload, ScoreValue, ScoresResponse
from orchestrator.services.callback_service import CallbackService, CALLBACK_SECRET_HEADER
from orchestrator.services.health_service import HealthService
from orchestrator.services.job_service import JobService
from orchestrator.services.listen_service import ListenService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.score_service import ScoreService, build_scores_from_state
from orchestrator.services.sync_waiter import SyncCallbackWaiter
from orchestrator.services.exceptions import (
    CallbackServiceError,
    JobServiceError,
    ListenServiceError,
)
from orchestrator.repositories import AuditFailureRecord, AuditFailureRepository
from orchestrator.common import stubbing


LISTEN_AUTH_HEADER = "X-Service-Auth-Secret"
LISTEN_AUTH_SECRET = "orchestrator-listen-secret"
BURN_HOTKEY = "5EtM9iXMAYRsmt6aoQAoWNDX6yaBnjhmnEQhWKv8HpwkVtML"

logger = logging.getLogger("orchestrator.routes.public")


class ListenRequest(BaseModel):
    job_type: JobType
    payload: Any
    job_id: Optional[uuid.UUID] = None


class RemoteListenRequest(ListenRequest):
    webhook_url: AnyHttpUrl
    route_to_auditor: bool = False


class DebugListenRequest(ListenRequest):
    miner: Miner | None = None


DEBUG_CALLBACK_BASE = "https://orchestrator.dippy-bittensor-subnet.com"


class ListenResponse(BaseModel):
    job_id: uuid.UUID


class CandidateSelection(BaseModel):
    selected_at: datetime
    miner: Miner


class LastCandidatesResponse(BaseModel):
    candidates: list[CandidateSelection]


class CallbackResponse(BaseModel):
    status: str
    message: str


class AuditFailureEntry(BaseModel):
    id: uuid.UUID
    created_at: datetime
    audit_job_id: uuid.UUID
    target_job_id: Optional[uuid.UUID] = None
    miner_hotkey: Optional[str] = None
    netuid: Optional[int] = None
    network: Optional[str] = None
    audit_payload: Optional[dict[str, Any]] = None
    audit_response_payload: Optional[dict[str, Any]] = None
    target_payload: Optional[dict[str, Any]] = None
    target_response_payload: Optional[dict[str, Any]] = None
    audit_image_hash: Optional[str] = None
    target_image_hash: Optional[str] = None


class AuditFailuresResponse(BaseModel):
    audit_failures: list[AuditFailureEntry]


def create_public_router() -> APIRouter:
    router = APIRouter()

    @router.post(
        "/listen",
        response_model=ListenResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def listen(
        listen_request: ListenRequest,
        request: Request,
        listen_service: ListenService = Depends(get_listen_service),
        slog: StructuredLogger = Depends(get_structured_logger),
    ) -> ListenResponse:
        provided_secret = request.headers.get(LISTEN_AUTH_HEADER)
        if provided_secret != LISTEN_AUTH_SECRET:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid service auth secret",
            )

        try:
            job_id = await listen_service.process(
                job_type=listen_request.job_type,
                payload=listen_request.payload,
                desired_job_id=listen_request.job_id,
            )
        except ListenServiceError as exc:
            raise_listen_service_error(exc)
        except JobServiceError as exc:
            raise_job_service_error(exc)

        return ListenResponse(job_id=job_id)

    @router.post(
        "/listen/remote",
        response_model=ListenResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def listen_remote(
        listen_request: RemoteListenRequest,
        request: Request,
        listen_service: ListenService = Depends(get_listen_service),
        slog: StructuredLogger = Depends(get_structured_logger),
    ) -> ListenResponse:
        provided_secret = request.headers.get(LISTEN_AUTH_HEADER)
        if provided_secret != LISTEN_AUTH_SECRET:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid service auth secret",
            )

        payload = _attach_webhook_url(listen_request.payload, str(listen_request.webhook_url))
        override_miner = stubbing.AUDIT_MINER if listen_request.route_to_auditor else None

        try:
            job_id = await listen_service.process(
                job_type=listen_request.job_type,
                payload=payload,
                desired_job_id=listen_request.job_id,
                override_miner=override_miner,
            )
        except ListenServiceError as exc:
            raise_listen_service_error(exc)
        except JobServiceError as exc:
            raise_job_service_error(exc)

        return ListenResponse(job_id=job_id)

    @router.post(
        "/debug/listen",
        response_model=ListenResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def debug_listen(
        listen_request: DebugListenRequest,
        request: Request,
        listen_service: ListenService = Depends(get_listen_service),
        slog: StructuredLogger = Depends(get_structured_logger),
    ) -> ListenResponse:
        provided_secret = request.headers.get(LISTEN_AUTH_HEADER)
        if provided_secret != LISTEN_AUTH_SECRET:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid service auth secret",
            )

        miner = listen_request.miner or stubbing.AUDIT_MINER
        payload = _override_callback_url(listen_request.payload)
        slog.info(
            "debug.listen.start",
            job_type=str(listen_request.job_type),
            miner_hotkey=getattr(miner, "hotkey", None),
            miner_addr=getattr(miner, "network_address", None),
        )

        try:
            job_id = await listen_service.process(
                job_type=listen_request.job_type,
                payload=payload,
                desired_job_id=listen_request.job_id,
                override_miner=miner,
            )
        except ListenServiceError as exc:
            raise_listen_service_error(exc)
        except JobServiceError as exc:
            raise_job_service_error(exc)

        return ListenResponse(job_id=job_id)

    @router.get(
        "/audit_failures",
        response_model=AuditFailuresResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_audit_failures(
        repo: AuditFailureRepository = Depends(get_audit_failure_repository),
    ) -> AuditFailuresResponse:
        records = repo.list_recent(limit=100)
        return AuditFailuresResponse(
            audit_failures=[_record_to_entry(record) for record in records],
        )

    @router.get(
        "/last_candidates",
        response_model=LastCandidatesResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_last_candidates(
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> LastCandidatesResponse:
        selections = [
            CandidateSelection(selected_at=selected_at, miner=miner)
            for selected_at, miner in client.get_last_candidates()
        ]
        return LastCandidatesResponse(candidates=selections)

    @router.get(
        "/miner_status",
        response_model=MetagraphDumpResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_miner_status(
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> MetagraphDumpResponse:
        last_update_dt = client.last_update()
        last_updated = last_update_dt.isoformat() if last_update_dt else None
        last_block = client.last_block()
        return MetagraphDumpResponse(
            data=client.dump_filtered_state(),
            block=last_block,
            last_updated=last_updated,
            meta=None,
        )

    @router.get("/scores", response_model=ScoresResponse, status_code=status.HTTP_200_OK)
    async def get_scores(
        request: Request,
        score_service: ScoreService = Depends(get_score_service),
        metagraph: MinerMetagraphService = Depends(get_miner_metagraph_service),
        job_service: JobService = Depends(get_job_service),
    ) -> ScoresResponse:
        try:
            _ = await request.json()
        except Exception:
            pass

        last_update = score_service.last_update()
        if last_update is not None:
            stored_scores = score_service.all()
            metagraph_state = metagraph.dump_full_state()
            scores_payload: dict[str, ScorePayload] = {}

            for hotkey, record in stored_scores.items():
                miner = metagraph_state.get(hotkey)
                failed_audits = getattr(miner, "failed_audits", 0) if miner else 0
                status_value = "SLASHED" if failed_audits else "COMPLETED"
                scores_payload[hotkey] = ScorePayload(
                    status=status_value,
                    score=ScoreValue(total_score=float(record.scores)),
                )

            # Calculate empty_scores by summing all scores
            total_score_sum = sum(float(record.scores) for record in stored_scores.values())
            empty_scores = total_score_sum < 1

            if empty_scores:
                payload = ScorePayload(
                status="COMPLETED",
                score=ScoreValue(total_score=1.0),
                )
                scores_payload[BURN_HOTKEY] = payload
            stats = {
                "source": "score_service",
                "requested": len(scores_payload),
                "available": len(scores_payload),
                "last_updated": last_update.isoformat(),
            }

            return ScoresResponse(scores=scores_payload, stats=stats)

        state = metagraph.dump_full_state()
        return await build_scores_from_state(
            state,
            job_relay_client=job_service.job_relay,
        )

    @router.get(
        "/jobs/{job_id}",
        status_code=status.HTTP_200_OK,
    )
    async def get_job(
        job_id: uuid.UUID,
        job_service: JobService = Depends(get_job_service),
    ) -> dict[str, Any]:
        try:
            record = await job_service.fetch_masked_job_record(job_id=job_id)
        except JobServiceError as exc:
            raise_job_service_error(exc)
        return record

    @router.get(
        "/recent_jobs",
        response_model=RecentJobsResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_recent_jobs(
        limit: int = Query(1000, ge=1, le=1000, description="Number of recent jobs to return (max 1000)"),
        job_service: JobService = Depends(get_job_service),
    ) -> RecentJobsResponse:
        try:
            records = await job_service.list_recent_completed_job_records(
                max_results=limit,
                lookback_days=None,
            )
        except JobServiceError as exc:
            raise_job_service_error(exc)
        return RecentJobsResponse(jobs=records, limit=limit)

    @router.get("/health", status_code=status.HTTP_200_OK)
    async def health(
        health_service: HealthService = Depends(get_health_service),
    ) -> dict[str, str]:
        return await health_service.get_health_status()

    @router.post(
        "/results/callback",
        response_model=CallbackResponse,
        status_code=status.HTTP_200_OK,
    )
    async def receive_results_callback(
        request: Request,
        job_id: str = Form(...),
        status: str = Form(...),
        completed_at: str = Form(...),
        error: Optional[str] = Form(None),
        image: UploadFile | None = File(None),
        job_service: JobService = Depends(get_job_service),
        callback_service: CallbackService = Depends(get_callback_service),
        sync_waiter: SyncCallbackWaiter = Depends(get_sync_callback_waiter),
    ) -> CallbackResponse:
        received_at = datetime.now(timezone.utc)
        provided_secret = request.headers.get(CALLBACK_SECRET_HEADER)
        client_host = request.client.host if request.client else None

        logger.info(
            "results_callback.request_received job_id=%s status=%s completed_at=%s has_image=%s filename=%s content_type=%s secret_provided=%s client=%s",
            job_id,
            status,
            completed_at,
            image is not None,
            image.filename if image else None,
            image.content_type if image else None,
            bool(provided_secret),
            client_host,
        )

        try:
            response_status, message = await callback_service.process_callback(
                job_service=job_service,
                job_id=job_id,
                status=status,
                completed_at=completed_at,
                error=error,
                provided_secret=provided_secret,
                image=image,
                received_at=received_at,
            )
        except CallbackServiceError as exc:
            await sync_waiter.fail(job_id, error=str(exc), status=JobStatus.FAILED)
            logger.warning(
                "results_callback.callback_error job_id=%s status=%s detail=%s",
                job_id,
                status,
                str(exc),
            )
            raise_callback_service_error(exc)
        except JobServiceError as exc:
            await sync_waiter.fail(job_id, error=str(exc), status=JobStatus.FAILED)
            logger.warning(
                "results_callback.job_service_error job_id=%s status=%s detail=%s",
                job_id,
                status,
                str(exc),
            )
            raise_job_service_error(exc)
        except HTTPException as http_exc:
            await sync_waiter.fail(job_id, error=str(http_exc.detail), status=JobStatus.FAILED)
            cause = getattr(http_exc, "__cause__", None)
            cause_type = type(cause).__name__ if cause else None
            cause_message = str(cause) if cause else None
            logger.warning(
                "results_callback.http_error job_id=%s status=%s code=%s detail=%s cause_type=%s cause_message=%s",
                job_id,
                status,
                http_exc.status_code,
                http_exc.detail,
                cause_type,
                cause_message,
            )
            raise
        except Exception:
            await sync_waiter.fail(job_id, error="callback_unexpected_error", status=JobStatus.FAILED)
            logger.exception(
                "results_callback.unexpected_error job_id=%s status=%s",
                job_id,
                status,
            )
            raise

        logger.info(
            "results_callback.processed job_id=%s request_status=%s response_status=%s",
            job_id,
            status,
            response_status,
        )

        return CallbackResponse(status=response_status, message=message)

    return router


def _record_to_entry(record: AuditFailureRecord) -> AuditFailureEntry:
    return AuditFailureEntry(
        id=record.id,
        created_at=record.created_at,
        audit_job_id=record.audit_job_id,
        target_job_id=record.target_job_id,
        miner_hotkey=record.miner_hotkey,
        netuid=record.netuid,
        network=record.network,
        audit_payload=record.audit_payload,
        audit_response_payload=record.audit_response_payload,
        target_payload=record.target_payload,
        target_response_payload=record.target_response_payload,
        audit_image_hash=record.audit_image_hash,
        target_image_hash=record.target_image_hash,
    )


def _override_callback_url(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    updated = dict(payload)
    updated["callback_url"] = f"{DEBUG_CALLBACK_BASE}/results/callback"
    return updated


def _attach_webhook_url(payload: Any, webhook_url: str) -> Any:
    if not isinstance(payload, dict):
        return payload
    updated = dict(payload)
    updated["webhook_url"] = webhook_url
    return updated
