from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from epistula.epistula import load_keypair_from_env
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from orchestrator.domain.miner import Miner
from orchestrator.common.job_store import Job, JobStatus, JobType
from orchestrator.dependencies import (
    get_job_service,
    get_miner_metagraph_service,
    get_score_service,
)
from orchestrator.routes.error_mapping import raise_job_service_error
from orchestrator.schemas.job import JobRecord
from orchestrator.services.job_service import JobService
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.services.score_service import ScoreRecord, ScoreService
from orchestrator.services.exceptions import JobServiceError


logger = logging.getLogger("orchestrator.routes.internal")


class CreateJobRequest(BaseModel):
    job_type: JobType
    payload: Any
    hotkey: str


class UpdateJobRequest(BaseModel):
    payload: Any


class StatsResponse(BaseModel):
    epistula_loaded: bool
    epistula_ss58: Optional[str]
    metagraph_last_updated: Optional[str]
    job_total: int
    job_completed: int


class MetagraphDumpResponse(BaseModel):
    data: dict[str, Miner]
    block: int | None = None
    meta: dict[str, Any] | None = None
    last_updated: Optional[str] = None


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


class MinerUpsertRequest(BaseModel):
    hotkey: Optional[str] = None
    uid: int = 0
    network_address: str = "http://localhost"
    valid: bool = False
    alpha_stake: int = 0
    capacity: Dict[str, bool] = Field(default_factory=dict)
    failed_audits: int = Field(default=0, ge=0)

    def to_miner(self, *, hotkey_override: str | None = None) -> Miner:
        payload = self.model_dump() if hasattr(self, "model_dump") else self.dict()
        effective_hotkey = hotkey_override or payload.get("hotkey")
        if not effective_hotkey:
            raise ValueError("Miner hotkey is required")
        payload["hotkey"] = effective_hotkey
        return Miner(**payload)


class MinerListResponse(BaseModel):
    miners: Dict[str, Miner]


class MinerDeleteResponse(BaseModel):
    status: str
    hotkey: str


class MinerCapacityResponse(BaseModel):
    capacities: Dict[str, Dict[str, bool]]
    block: int | None = None
    last_updated: Optional[str] = None


def create_internal_router() -> APIRouter:
    router = APIRouter(prefix="/_internal")

    @router.post("/job", response_model=Job, status_code=status.HTTP_201_CREATED)
    async def create_job(
        request: CreateJobRequest,
        job_service: JobService = Depends(get_job_service),
    ) -> Job:
        try:
            return await job_service.create_job(
                job_type=request.job_type,
                payload=request.payload,
                hotkey=request.hotkey,
            )
        except JobServiceError as exc:
            raise_job_service_error(exc)

    @router.put("/job/{job_id}", response_model=Job, status_code=status.HTTP_200_OK)
    async def update_job(
        job_id: uuid.UUID,
        request: UpdateJobRequest,
        job_service: JobService = Depends(get_job_service),
    ) -> Job:
        try:
            return await job_service.update_job(job_id=job_id, payload=request.payload)
        except JobServiceError as exc:
            raise_job_service_error(exc)

    @router.get("/stats", response_model=StatsResponse, status_code=status.HTTP_200_OK)
    async def stats(
        job_service: JobService = Depends(get_job_service),
        metagraph: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> StatsResponse:
        keypair = load_keypair_from_env()
        epistula_ss58 = getattr(keypair, "ss58_address", None) if keypair else None

        metagraph_last_update = None
        if hasattr(metagraph, "last_update"):
            last_update_dt = metagraph.last_update()
            if last_update_dt is not None:
                metagraph_last_update = last_update_dt.isoformat()

        try:
            job_totals = await job_service.get_job_totals()
        except JobServiceError as exc:
            raise_job_service_error(exc)
        return StatsResponse(
            epistula_loaded=keypair is not None,
            epistula_ss58=epistula_ss58,
            metagraph_last_updated=metagraph_last_update,
            job_total=job_totals["total"],
            job_completed=job_totals["completed"],
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
        statuses: set[JobStatus] | None = set(status_filter) if status_filter else None
        if active_only:
            pending_only = {JobStatus.PENDING}
            statuses = pending_only if statuses is None else statuses & pending_only

        if statuses is not None and not statuses:
            jobs: list[JobRecord] = []
        else:
            try:
                jobs = await job_service.dump_jobs(statuses=statuses, limit=limit)
            except JobServiceError as exc:
                raise_job_service_error(exc)

        return LiveJobDumpResponse(jobs=jobs)

    @router.get(
        "/metagraph/state",
        response_model=MetagraphDumpResponse,
        status_code=status.HTTP_200_OK,
    )
    async def dump_metagraph_state(
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
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
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
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

    @router.get("/miners", response_model=MinerListResponse, status_code=status.HTTP_200_OK)
    async def list_miners(
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> MinerListResponse:
        return MinerListResponse(miners=client.dump_full_state())

    @router.get(
        "/miners/capacity",
        response_model=MinerCapacityResponse,
        status_code=status.HTTP_200_OK,
    )
    async def list_miner_capacities(
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> MinerCapacityResponse:
        state = client.dump_full_state()
        capacities: Dict[str, Dict[str, bool]] = {
            hotkey: dict(getattr(miner, "capacity", {}) or {}) for hotkey, miner in state.items()
        }
        last_update_dt = client.last_update()
        return MinerCapacityResponse(
            capacities=capacities,
            block=client.last_block(),
            last_updated=last_update_dt.isoformat() if last_update_dt else None,
        )

    @router.get(
        "/miners/{hotkey}",
        response_model=Miner,
        status_code=status.HTTP_200_OK,
    )
    async def get_miner_record(
        hotkey: str,
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> Miner:
        miner = client.get_miner(hotkey)
        if miner is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Miner not found")
        return miner

    @router.post(
        "/miners",
        response_model=Miner,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_miner_record(
        request: MinerUpsertRequest,
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> Miner:
        try:
            miner = client.upsert_miner(request.to_miner())
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return miner

    @router.put(
        "/miners/{hotkey}",
        response_model=Miner,
        status_code=status.HTTP_200_OK,
    )
    async def update_miner_record(
        hotkey: str,
        request: MinerUpsertRequest,
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> Miner:
        try:
            miner = client.upsert_miner(request.to_miner(hotkey_override=hotkey))
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        return miner

    @router.delete(
        "/miners/{hotkey}",
        response_model=MinerDeleteResponse,
        status_code=status.HTTP_200_OK,
    )
    async def delete_miner_record(
        hotkey: str,
        client: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> MinerDeleteResponse:
        if not client.delete_miner(hotkey):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Miner not found")
        return MinerDeleteResponse(status="deleted", hotkey=hotkey)

    async def _collect_and_persist_scores(
        *,
        job_service: JobService,
        score_service: ScoreService,
        metagraph: MinerMetagraphService,
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
            hotkey = str(job.get("miner_hotkey", "")).strip()
            if not hotkey:
                continue
            jobs_by_hotkey.setdefault(hotkey, []).append(job)

        miner_state = metagraph.dump_full_state()
        stored_scores = score_service.all()

        def _normalize(items: Iterable[str]) -> list[str]:
            seen: set[str] = set()
            normalized: list[str] = []
            for value in items:
                item = str(value).strip()
                if not item or item in seen:
                    continue
                seen.add(item)
                normalized.append(item)
            return normalized

        union_hotkeys = set(jobs_by_hotkey.keys()) | set(miner_state.keys()) | set(stored_scores.keys())
        normalized_hotkeys = _normalize(union_hotkeys)
        for hotkey in normalized_hotkeys:
            jobs_by_hotkey.setdefault(hotkey, [])

        existing_records = {hk: stored_scores[hk] for hk in normalized_hotkeys if hk in stored_scores}

        fresh_scores = score_service.jobs_to_score(
            jobs_by_hotkey,
            reference_time=datetime.now(timezone.utc),
            existing_records=existing_records,
            hotkeys=normalized_hotkeys,
        )

        snapshot: Dict[str, RawDumpEntry] = {}
        for hotkey in normalized_hotkeys:
            jobs = jobs_by_hotkey.get(hotkey, [])
            completed_jobs_count = sum(
                1 for job in jobs if ScoreService._is_completed_job(job)  # noqa: SLF001
            )
            score_record = fresh_scores.get(hotkey) or ScoreRecord(
                scores=0.0,
                failure_count=0,
            )
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

    @router.get(
        "/scores/state",
        response_model=ScoreStateResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_score_state(
        score_service: ScoreService = Depends(get_score_service),
    ) -> ScoreStateResponse:
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
        metagraph: MinerMetagraphService = Depends(get_miner_metagraph_service),
    ) -> Dict[str, RawDumpEntry]:
        return await _collect_and_persist_scores(
            job_service=job_service,
            score_service=score_service,
            metagraph=metagraph,
        )

    return router
