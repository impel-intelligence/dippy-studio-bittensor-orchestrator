"""FastAPI application exposing the job relay endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status

from .background import BackgroundJobProcessor
from .config import Settings, get_settings
from .duckdb_manager import JobRelayDuckDBManager
from .models import InferenceJob, InferenceJobCreate, InferenceJobUpdate
from .repository import InferenceJobRepository

LOGGER = logging.getLogger("jobrelay.app")
AUTH_HEADER_NAME = "X-Service-Auth-Secret"


def create_app() -> FastAPI:
    settings = get_settings()
    manager = JobRelayDuckDBManager(settings)
    repository = InferenceJobRepository(manager)
    processor = BackgroundJobProcessor(repository, settings, manager)

    app = FastAPI(title="Job Relay Service", version="0.1.0")

    app.state.settings = settings
    app.state.manager = manager
    app.state.repository = repository
    app.state.processor = processor

    LOGGER.info(
        "Job relay auth configured with header '%s' and token '%s'",
        AUTH_HEADER_NAME,
        settings.auth_token,
    )

    @app.on_event("startup")
    async def startup_event() -> None:  # pragma: no cover - exercised in runtime
        LOGGER.info("Starting job relay service")
        await processor.start()

    @app.on_event("shutdown")
    async def shutdown_event() -> None:  # pragma: no cover - exercised in runtime
        LOGGER.info("Stopping job relay service")
        await processor.stop()

    def get_processor_dependency() -> BackgroundJobProcessor:
        return processor

    def get_repository_dependency() -> InferenceJobRepository:
        return repository

    @app.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    auth_dependency = Depends(_build_auth_dependency(settings))

    @app.post("/jobs/{job_id}", dependencies=[auth_dependency])
    async def create_inference_job(
        job_id: UUID,
        payload: InferenceJobCreate,
        processor_dep: BackgroundJobProcessor = Depends(get_processor_dependency),
    ) -> Dict[str, str]:
        prepared = processor_dep.prepare_insert_payload(job_id, payload.model_dump())
        await processor_dep.enqueue_insert(prepared)
        return {"job_id": str(job_id), "status": "queued"}

    @app.patch("/jobs/{job_id}", dependencies=[auth_dependency])
    async def update_inference_job(
        job_id: UUID,
        updates: InferenceJobUpdate,
        processor_dep: BackgroundJobProcessor = Depends(get_processor_dependency),
    ) -> Dict[str, str]:
        update_dict = updates.model_dump(exclude_none=True, exclude_unset=True)
        if not update_dict:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="no updates provided")
        prepared = processor_dep.prepare_update_payload(update_dict)
        if not prepared:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="no updates provided")
        await processor_dep.enqueue_update(job_id, prepared)
        return {"job_id": str(job_id), "status": "queued"}

    @app.get("/jobs/{job_id}", dependencies=[auth_dependency], response_model=InferenceJob)
    async def fetch_inference_job(
        job_id: UUID,
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> InferenceJob:
        record = repository_dep.fetch_job(job_id)
        if record is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
        return InferenceJob(**_normalize_record(record))

    @app.get("/jobs", dependencies=[auth_dependency])
    async def fetch_all_jobs(
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> dict:
        records = [_record_to_dict(item) for item in repository_dep.fetch_all()]
        return {"jobs": records}

    @app.post("/nuclear", dependencies=[auth_dependency])
    async def nuclear_wipe(
        processor_dep: BackgroundJobProcessor = Depends(get_processor_dependency),
    ) -> Dict[str, object]:
        result = await processor_dep.nuclear_wipe()
        return {"status": "obliterated", **result}

    @app.get("/hotkeys/{hotkey}/jobs", dependencies=[auth_dependency])
    async def fetch_jobs_for_hotkey(
        hotkey: str,
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> dict:
        records = [_record_to_dict(item) for item in repository_dep.fetch_for_hotkey(hotkey)]
        return {"jobs": records}

    return app


def _build_auth_dependency(settings: Settings):
    async def _auth_dependency(request: Request) -> None:
        token = settings.auth_token
        if token is None:
            return
        provided = request.headers.get(AUTH_HEADER_NAME)
        if provided != token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")

    return _auth_dependency


def _normalize_record(record: dict) -> dict:
    normalized = dict(record)
    normalized["job_id"] = str(normalized["job_id"])
    if "audit_target_job_id" in normalized and normalized["audit_target_job_id"] is not None:
        normalized["audit_target_job_id"] = str(normalized["audit_target_job_id"])
    return normalized


def _record_to_dict(record: dict) -> dict:
    model = InferenceJob(**_normalize_record(record))
    return model.model_dump(mode="json")


app = create_app()
