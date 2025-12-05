"""FastAPI application exposing the job relay endpoints."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status

from .background import BackgroundJobProcessor
from .config import Settings, get_settings
from .duckdb_manager import JobRelayDuckDBManager
from .models import InferenceJob, InferenceJobCreate, InferenceJobUpdate
from .repository import InferenceJobRepository

_OTEL_CONFIGURED = False
LOGGER = logging.getLogger("jobrelay.app")
AUTH_HEADER_NAME = "X-Service-Auth-Secret"


def _configure_opentelemetry(app: FastAPI) -> None:
    """Initialize OpenTelemetry tracing/metrics with OTLP exporters."""
    global _OTEL_CONFIGURED
    if _OTEL_CONFIGURED:
        return
    if os.getenv("OTEL_SDK_DISABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        LOGGER.info("OpenTelemetry disabled via OTEL_SDK_DISABLED")
        return
    try:
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:  # pragma: no cover
        LOGGER.warning("OpenTelemetry not configured (missing packages): %s", exc)
        return

    base_otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318").rstrip("/")
    traces_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or f"{base_otlp_endpoint}/v1/traces"
    metrics_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or f"{base_otlp_endpoint}/v1/metrics"

    resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "jobrelay")})

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_endpoint)))
    trace.set_tracer_provider(tracer_provider)

    metric_exporter = OTLPMetricExporter(endpoint=metrics_endpoint)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[PeriodicExportingMetricReader(metric_exporter)],
    )
    metrics.set_meter_provider(meter_provider)

    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )
    RequestsInstrumentor().instrument()

    _OTEL_CONFIGURED = True
    LOGGER.info("OpenTelemetry configured for jobrelay (endpoint_base=%s)", base_otlp_endpoint)


def create_app() -> FastAPI:
    settings = get_settings()
    manager = JobRelayDuckDBManager(settings)
    repository = InferenceJobRepository(manager)
    processor = BackgroundJobProcessor(repository, settings, manager)

    app = FastAPI(title="Job Relay Service", version="0.1.0")
    _configure_opentelemetry(app)

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

    @app.get("/jobs", dependencies=[auth_dependency])
    async def fetch_all_jobs(
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> dict:
        records = []
        for item in repository_dep.fetch_all():
            serialized = _record_to_dict(item)
            if serialized is not None:
                records.append(serialized)
        return {"jobs": records}

    @app.get("/jobs/recent", dependencies=[auth_dependency])
    async def fetch_recent_jobs(
        limit: int = Query(1000, ge=1, le=10000),
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> dict:
        records = []
        for item in repository_dep.fetch_all(limit=limit):
            serialized = _record_to_dict(item)
            if serialized is not None:
                records.append(serialized)
        return {"jobs": records}

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

    @app.post("/nuclear", dependencies=[auth_dependency])
    async def nuclear_wipe(
        processor_dep: BackgroundJobProcessor = Depends(get_processor_dependency),
    ) -> Dict[str, object]:
        result = await processor_dep.nuclear_wipe()
        return {"status": "obliterated", **result}

    @app.post("/purge-local", dependencies=[auth_dependency])
    async def purge_local(
        skip_snapshots: bool = Query(default=True, description="If true, skip snapshot IO after purge"),
        processor_dep: BackgroundJobProcessor = Depends(get_processor_dependency),
    ) -> Dict[str, object]:
        result = await processor_dep.purge_local(skip_snapshots=skip_snapshots)
        return {"status": "purged", **result}

    @app.get("/hotkeys/{hotkey}/jobs", dependencies=[auth_dependency])
    async def fetch_jobs_for_hotkey(
        hotkey: str,
        since: datetime | None = None,
        repository_dep: InferenceJobRepository = Depends(get_repository_dependency),
    ) -> dict:
        cutoff: datetime | None = None
        if since is not None:
            if since.tzinfo is None:
                cutoff = since.replace(tzinfo=timezone.utc)
            else:
                cutoff = since.astimezone(timezone.utc)
        records = [
            _record_to_dict(item)
            for item in repository_dep.fetch_for_hotkey(hotkey, since=cutoff)
        ]
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

    def _maybe_parse_json(value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:  # noqa: BLE001
                return value
        return value

    normalized["payload"] = _maybe_parse_json(normalized.get("payload"))
    normalized["response_payload"] = _maybe_parse_json(normalized.get("response_payload"))
    return normalized


def _record_to_dict(record: dict) -> dict | None:
    try:
        model = InferenceJob(**_normalize_record(record))
        return model.model_dump(mode="json")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("record.serialization_failed job_id=%s error=%s", record.get("job_id"), exc)
        return None


app = create_app()
