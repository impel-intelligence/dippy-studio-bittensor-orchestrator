from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from copy import deepcopy

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel

from orchestrator.domain.miner import Miner
from orchestrator.common.job_store import JobStatus, JobType
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.dependencies import (
    get_callback_service,
    get_config,
    get_health_service,
    get_job_service,
    get_listen_service,
    get_miner_metagraph_service,
    get_score_service,
    get_structured_logger,
)
from orchestrator.routes.internal import MetagraphDumpResponse
from orchestrator.routes.error_mapping import (
    raise_callback_service_error,
    raise_job_service_error,
    raise_listen_service_error,
)
from orchestrator.schemas.job import CompletedJobSummary, CompletedJobsResponse
from orchestrator.schemas.scores import ScorePayload, ScoreValue, ScoresResponse
from orchestrator.services.callback_service import CallbackService, CALLBACK_SECRET_HEADER
from orchestrator.services.health_service import HealthService
from orchestrator.services.job_service import JobService, JobWaitCancelledError, JobWaitTimeoutError
from orchestrator.services.listen_service import ListenService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.score_service import ScoreService, build_scores_from_state
from orchestrator.services.exceptions import (
    CallbackServiceError,
    JobServiceError,
    ListenServiceError,
)


LISTEN_AUTH_HEADER = "X-Service-Auth-Secret"
LISTEN_AUTH_SECRET = "orchestrator-listen-secret"
BURN_HOTKEY = "5EtM9iXMAYRsmt6aoQAoWNDX6yaBnjhmnEQhWKv8HpwkVtML"

logger = logging.getLogger("orchestrator.routes.public")


class ListenRequest(BaseModel):
    job_type: JobType
    payload: Any
    job_id: Optional[uuid.UUID] = None


class ListenResponse(BaseModel):
    job_id: uuid.UUID


class ListenSyncResponse(BaseModel):
    job_id: uuid.UUID
    status: JobStatus
    result: Any | None = None
    failure_reason: Optional[str] = None


class CandidateSelection(BaseModel):
    selected_at: datetime
    miner: Miner


class LastCandidatesResponse(BaseModel):
    candidates: list[CandidateSelection]


class CallbackResponse(BaseModel):
    status: str
    message: str


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
                slog=slog,
            )
        except ListenServiceError as exc:
            raise_listen_service_error(exc)
        except JobServiceError as exc:
            raise_job_service_error(exc)

        return ListenResponse(job_id=job_id)

    @router.post(
        "/listen/sync",
        response_model=ListenSyncResponse,
        status_code=status.HTTP_200_OK,
    )
    async def listen_sync(
        listen_request: ListenRequest,
        request: Request,
        listen_service: ListenService = Depends(get_listen_service),
        job_service: JobService = Depends(get_job_service),
        config = Depends(get_config),
        slog: StructuredLogger = Depends(get_structured_logger),
    ) -> ListenSyncResponse:
        provided_secret = request.headers.get(LISTEN_AUTH_HEADER)
        if provided_secret != LISTEN_AUTH_SECRET:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid service auth secret",
            )

        slog.info(
            "listen.sync.start",
            job_type=str(listen_request.job_type),
            request_has_job_id=bool(listen_request.job_id),
        )

        try:
            job_id = await listen_service.process(
                job_type=listen_request.job_type,
                payload=listen_request.payload,
                desired_job_id=listen_request.job_id,
                slog=slog,
            )
        except ListenServiceError as exc:
            raise_listen_service_error(exc)
        except JobServiceError as exc:
            raise_job_service_error(exc)

        listen_sync_cfg = getattr(getattr(config, "listen", None), "sync", None)
        timeout_seconds = 10.0
        poll_interval = 1.0
        if listen_sync_cfg is not None:
            timeout_seconds = max(0.0, float(getattr(listen_sync_cfg, "timeout_seconds", 10.0) or 0.0))
            poll_interval = float(getattr(listen_sync_cfg, "poll_interval_seconds", 1.0) or 1.0)
        if poll_interval <= 0.0:
            poll_interval = 0.1

        async def _is_disconnected() -> bool:
            return bool(await request.is_disconnected())

        start_wait = time.monotonic()
        try:
            job = await job_service.wait_for_terminal_state(
                job_id,
                timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval,
                is_disconnected=_is_disconnected,
            )
        except JobWaitCancelledError as exc:
            slog.info(
                "listen.sync.cancelled",
                job_id=str(job_id),
                elapsed_ms=int((time.monotonic() - start_wait) * 1000),
            )
            raise HTTPException(
                status_code=499,
                detail=str(exc),
            ) from exc
        except JobWaitTimeoutError as exc:
            slog.warning(
                "listen.sync.timeout",
                job_id=str(job_id),
                timeout_seconds=timeout_seconds,
                elapsed_ms=int((time.monotonic() - start_wait) * 1000),
            )
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail=str(exc),
            ) from exc
        except JobServiceError as exc:
            raise_job_service_error(exc)

        duration_ms = int((time.monotonic() - start_wait) * 1000)
        result_payload: Any | None = None
        if job.job_response is not None:
            result_payload = deepcopy(job.job_response.payload)

        slog.info(
            "listen.sync.completed",
            job_id=str(job.job_id),
            status=str(getattr(job.status, "value", job.status)),
            elapsed_ms=duration_ms,
            failure_reason=job.failure_reason,
        )

        return ListenSyncResponse(
            job_id=job.job_id,
            status=job.status if isinstance(job.status, JobStatus) else JobStatus(str(job.status)),
            result=result_payload,
            failure_reason=job.failure_reason,
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
        "/completed_jobs",
        response_model=CompletedJobsResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_recent_completed_jobs(
        limit: int = Query(
            100,
            ge=1,
            le=100,
            description="Maximum number of completed jobs to return (default 100)",
        ),
        job_service: JobService = Depends(get_job_service),
    ) -> CompletedJobsResponse:
        lookback_days = 7
        try:
            records = await job_service.list_recent_completed_jobs(
                max_results=limit,
                lookback_days=lookback_days,
            )
        except JobServiceError as exc:
            raise_job_service_error(exc)
        summaries = [CompletedJobSummary.from_job_record(record) for record in records]
        return CompletedJobsResponse(jobs=summaries, limit=limit, lookback_days=lookback_days)

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
            logger.warning(
                "results_callback.callback_error job_id=%s status=%s detail=%s",
                job_id,
                status,
                str(exc),
            )
            raise_callback_service_error(exc)
        except JobServiceError as exc:
            logger.warning(
                "results_callback.job_service_error job_id=%s status=%s detail=%s",
                job_id,
                status,
                str(exc),
            )
            raise_job_service_error(exc)
        except HTTPException as http_exc:
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
