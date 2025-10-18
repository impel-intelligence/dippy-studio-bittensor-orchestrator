from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status

from orchestrator.common.job_store import Job, JobStatus, JobType
from orchestrator.dependencies import (
    get_health_service,
    get_job_service,
    get_miner_metagraph_client,
    get_listen_service,
    get_score_service,
)
from orchestrator.services.health_service import HealthService
from orchestrator.services.job_service import JobService
from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.dependencies import get_structured_logger, get_callback_service
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.services.callback_service import CallbackService, CALLBACK_SECRET_HEADER
from epistula.epistula import load_keypair_from_env
from orchestrator.services.listen_service import ListenService
from orchestrator.schemas.job import JobRecord
from orchestrator.schemas.scores import ScorePayload, ScoreValue, ScoresResponse
from orchestrator.services.score_service import ScoreRecord, ScoreService, build_scores_from_state
from pydantic import BaseModel


LISTEN_AUTH_HEADER = "X-Service-Auth-Secret"
LISTEN_AUTH_SECRET = "orchestrator-listen-secret"

logger = logging.getLogger("orchestrator.routes")

class ListenRequest(BaseModel):
    job_type: JobType
    payload: Any
    job_id: uuid.UUID | None = None


class ResultRequest(BaseModel):
    payload: Any


class CreateJobRequest(BaseModel):
    job_type: JobType
    payload: Any
    hotkey: str


class UpdateJobRequest(BaseModel):
    payload: Any


class ListenResponse(BaseModel):
    job_id: uuid.UUID


class MetagraphDumpResponse(BaseModel):
    data: dict[str, Miner]
    block: int | None = None
    meta: dict[str, Any] | None = None
    last_updated: Optional[str] = None


class CallbackResponse(BaseModel):
    status: str
    message: str


class CallbackListResponse(BaseModel):
    callbacks: list[dict[str, Any]]


class StatsResponse(BaseModel):
    epistula_loaded: bool
    epistula_ss58: Optional[str]
    metagraph_last_updated: Optional[str]
    job_total: int
    job_completed: int


class LiveJobDumpResponse(BaseModel):
    jobs: list[JobRecord]


class ScoreStateResponse(BaseModel):
    last_updated: Optional[str]
    db_path: str
    records: dict[str, ScoreRecord]


class RawDumpEntry(BaseModel):
    all_jobs_count: int
    completed_jobs_count: int
    miner_object: Optional[Miner]
    converted_score: ScoreRecord


def create_internal_router() -> APIRouter:
    router = APIRouter(prefix="/_internal")

    @router.post(
        "/job", response_model=Job, status_code=status.HTTP_201_CREATED
    )
    async def create_job(
        request: CreateJobRequest,
        job_service: JobService = Depends(get_job_service),
    ) -> Job:
        """Create a new job using the job relay service."""
        return await job_service.create_job(
            job_type=request.job_type,
            payload=request.payload,
            hotkey=request.hotkey,
        )

    @router.put(
        "/job/{job_id}", response_model=Job, status_code=status.HTTP_200_OK
    )
    async def update_job(
        job_id: uuid.UUID,
        request: UpdateJobRequest,
        job_service: JobService = Depends(get_job_service),
    ) -> Job:
        return await job_service.update_job(job_id=job_id, payload=request.payload)

    @router.get(
        "/stats",
        response_model=StatsResponse,
        status_code=status.HTTP_200_OK,
    )
    async def stats(
        job_service: JobService = Depends(get_job_service),
        metagraph: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    ) -> StatsResponse:
        keypair = load_keypair_from_env()
        epistula_ss58 = getattr(keypair, "ss58_address", None) if keypair else None

        metagraph_last_update = None
        if hasattr(metagraph, "last_update"):
            last_update_dt = metagraph.last_update()
            if last_update_dt is not None:
                metagraph_last_update = last_update_dt.isoformat()

        job_totals = await job_service.get_job_totals()
        job_total = job_totals["total"]
        job_completed = job_totals["completed"]

        return StatsResponse(
            epistula_loaded=keypair is not None,
            epistula_ss58=epistula_ss58,
            metagraph_last_updated=metagraph_last_update,
            job_total=job_total,
            job_completed=job_completed,
        )

    @router.get(
        "/jobs/live",
        response_model=LiveJobDumpResponse,
        status_code=status.HTTP_200_OK,
    )
    async def dump_live_jobs(
        limit: int | None = Query(
            None,
            ge=1,
            description="Optional maximum number of jobs to return",
        ),
        status_filter: list[JobStatus] | None = Query(
            None,
            alias="status",
            description="Optional list of job statuses to include",
        ),
        active_only: bool = Query(
            True,
            description="When true, only include jobs that are still pending",
        ),
        job_service: JobService = Depends(get_job_service),
    ) -> LiveJobDumpResponse:
        """Dump live jobs from the job relay with optional filtering."""

        statuses: set[JobStatus] | None = set(status_filter) if status_filter else None
        if active_only:
            pending_only = {JobStatus.PENDING}
            statuses = pending_only if statuses is None else statuses & pending_only

        if statuses is not None and not statuses:
            jobs: list[JobRecord] = []
        else:
            jobs = await job_service.dump_jobs(statuses=statuses, limit=limit)

        return LiveJobDumpResponse(jobs=jobs)

    @router.get(
        "/metagraph/state",
        response_model=MetagraphDumpResponse,
        status_code=status.HTTP_200_OK,
    )
    async def dump_metagraph_state(
        client: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    ) -> MetagraphDumpResponse:
        last_update_dt = client.last_update()
        last_updated = last_update_dt.isoformat() if last_update_dt else None
        last_block = client.last_block()
        return MetagraphDumpResponse(
            data=client.dump_filtered_state(),
            block=last_block,
            last_updated=last_updated,
            meta={
                "client_instance_id": client.instance_id,
                "db_path": client.db_path,
                "source": "filtered",
                "last_updated": last_updated,
            },
        )

    @router.get(
        "/metagraph/full-state",
        response_model=MetagraphDumpResponse,
        status_code=status.HTTP_200_OK,
    )
    async def dump_full_metagraph_state(
        client: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    ) -> MetagraphDumpResponse:
        last_update_dt = client.last_update()
        last_updated = last_update_dt.isoformat() if last_update_dt else None
        last_block = client.last_block()
        return MetagraphDumpResponse(
            data=client.dump_full_state(),
            block=last_block,
            last_updated=last_updated,
            meta={
                "client_instance_id": client.instance_id,
                "db_path": client.db_path,
                "source": "full",
                "last_updated": last_updated,
            },
        )

    async def _collect_and_persist_scores(
        *,
        job_service: JobService,
        score_service: ScoreService,
        metagraph: LiveMinerMetagraphClient,
    ) -> Dict[str, RawDumpEntry]:
        try:
            job_records = await job_service.job_relay.list_jobs()
        except Exception as exc:  # noqa: BLE001
            logger.exception("raw_dump.list_jobs_failed")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Failed to fetch jobs from relay",
            ) from exc

        jobs_by_hotkey: Dict[str, list[dict[str, Any]]] = {}
        for job in job_records:
            hotkey = job.get("miner_hotkey")
            if not hotkey:
                continue
            jobs_by_hotkey.setdefault(str(hotkey), []).append(job)

        miner_state = metagraph.dump_full_state()
        stored_scores = score_service.all()

        hotkeys = set(jobs_by_hotkey.keys()) | set(miner_state.keys()) | set(stored_scores.keys())

        fresh_scores: Dict[str, ScoreRecord] = {}
        snapshot: Dict[str, RawDumpEntry] = {}

        for hotkey in hotkeys:
            jobs = jobs_by_hotkey.get(hotkey, [])
            completed_jobs_count = sum(
                1
                for job in jobs
                if ScoreService._is_completed_job(job)  # noqa: SLF001
            )

            existing_record = stored_scores.get(hotkey)
            extras: Dict[str, Any]
            is_slashed = False
            if existing_record is not None:
                payload = existing_record.model_dump()
                is_slashed = bool(payload.get("is_slashed", False))
                extras = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"scores", "is_slashed"}
                }
            else:
                extras = {}

            record_payload: Dict[str, Any] = {
                "scores": float(completed_jobs_count),
                "is_slashed": is_slashed,
                **extras,
            }
            score_record = ScoreRecord(**record_payload)
            fresh_scores[hotkey] = score_record

            snapshot[hotkey] = RawDumpEntry(
                all_jobs_count=len(jobs),
                completed_jobs_count=completed_jobs_count,
                miner_object=miner_state.get(hotkey),
                converted_score=score_record,
            )

        if fresh_scores:
            try:
                persisted = score_service.update_scores(fresh_scores)
                if not persisted:
                    logger.warning("raw_dump.update_scores_failed hotkeys=%s", len(fresh_scores))
            except Exception:  # noqa: BLE001
                logger.exception("raw_dump.update_scores_exception hotkeys=%s", len(fresh_scores))

        return snapshot

    @router.post(
        "/scores/trigger",
        response_model=Dict[str, RawDumpEntry],
        status_code=status.HTTP_200_OK,
    )
    async def trigger_score_pipeline(
        score_service: ScoreService = Depends(get_score_service),
        job_service: JobService = Depends(get_job_service),
        metagraph: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    ) -> Dict[str, RawDumpEntry]:

        return await _collect_and_persist_scores(
            job_service=job_service,
            score_service=score_service,
            metagraph=metagraph,
        )

    @router.get(
        "/scores/state",
        response_model=ScoreStateResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_score_state(
        score_service: ScoreService = Depends(get_score_service),
    ) -> ScoreStateResponse:
        """Expose the persisted score state for internal diagnostics."""

        records = dict(score_service.items())
        last_update = score_service.last_update()
        return ScoreStateResponse(
            last_updated=last_update.isoformat() if last_update else None,
            db_path=score_service.db_path,
            records=records,
        )

    @router.get(
        "/raw_dump",
        response_model=Dict[str, RawDumpEntry],
        status_code=status.HTTP_200_OK,
    )
    async def raw_dump(
        score_service: ScoreService = Depends(get_score_service),
        job_service: JobService = Depends(get_job_service),
        metagraph: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    ) -> Dict[str, RawDumpEntry]:
        """Return aggregated miner/job state without background batching."""

        return await _collect_and_persist_scores(
            job_service=job_service,
            score_service=score_service,
            metagraph=metagraph,
        )

    return router


def create_public_router() -> APIRouter:
    router = APIRouter()

    @router.post(
        "/listen", response_model=ListenResponse, status_code=status.HTTP_202_ACCEPTED
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

        job_id = await listen_service.process(
            job_type=listen_request.job_type,
            payload=listen_request.payload,
            desired_job_id=listen_request.job_id,
            slog=slog,
        )

        return ListenResponse(job_id=job_id)

    @router.get("/scores", response_model=ScoresResponse, status_code=status.HTTP_200_OK)
    async def get_scores(
        request: Request,
        score_service: ScoreService = Depends(get_score_service),
        metagraph: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
        job_service: JobService = Depends(get_job_service),
    ) -> ScoresResponse:

        try:
            _ = await request.json()
        except Exception:
            pass

        last_update = score_service.last_update()
        if last_update is not None:
            stored_scores = score_service.all()
            scores_payload: dict[str, ScorePayload] = {}
            for hotkey, record in stored_scores.items():
                status_value = "SLASHED" if record.is_slashed else "COMPLETED"
                scores_payload[hotkey] = ScorePayload(
                    status=status_value,
                    score=ScoreValue(total_score=float(record.scores)),
                )

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

        return CallbackResponse(status=response_status, message=message)

    return router 
