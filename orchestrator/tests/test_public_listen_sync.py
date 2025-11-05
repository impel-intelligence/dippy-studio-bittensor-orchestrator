from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.common.job_store import Job, JobRequest, JobResponse, JobStatus, JobType
from orchestrator.config import ListenConfig, ListenSyncConfig, OrchestratorConfig
from orchestrator.dependencies import (
    get_config,
    get_job_service,
    get_listen_service,
    get_structured_logger,
)
from orchestrator.routes.public import LISTEN_AUTH_HEADER, LISTEN_AUTH_SECRET, create_public_router
from orchestrator.services.job_service import JobWaitTimeoutError


class StubStructuredLogger:
    def info(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def warning(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def error(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass


class StubListenService:
    def __init__(self, job_id: uuid.UUID) -> None:
        self.job_id = job_id
        self.calls: list[tuple[JobType, dict[str, object], uuid.UUID | None]] = []

    async def process(
        self,
        *,
        job_type: JobType,
        payload: dict[str, object],
        desired_job_id: uuid.UUID | None,
        slog: StubStructuredLogger,
    ) -> uuid.UUID:
        self.calls.append((job_type, payload, desired_job_id))
        return self.job_id


class StubJobService:
    def __init__(self, job: Job, *, error: Exception | None = None) -> None:
        self.job = job
        self.error = error
        self.calls: list[tuple[uuid.UUID, float, float]] = []

    async def wait_for_terminal_state(
        self,
        job_id: uuid.UUID,
        *,
        timeout_seconds: float,
        poll_interval_seconds: float,
        is_disconnected,
    ) -> Job:
        self.calls.append((job_id, timeout_seconds, poll_interval_seconds))
        if self.error is not None:
            raise self.error
        return self.job


def _build_config(timeout_seconds: float = 10.0, poll_interval_seconds: float = 1.0) -> OrchestratorConfig:
    config = OrchestratorConfig()
    config.listen = ListenConfig(sync=ListenSyncConfig(timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds))
    return config


def _build_job(job_id: uuid.UUID, status: JobStatus, payload: dict[str, object] | None = None) -> Job:
    request = JobRequest(job_type=JobType.GENERATE, payload={"prompt": "test"})
    job = Job(job_request=request, hotkey="hk-sync", job_id=job_id, status=status)
    if payload is not None:
        job.job_response = JobResponse(payload=payload)
    return job


def _create_client(listen_service: StubListenService, job_service: StubJobService, config: OrchestratorConfig) -> TestClient:
    app = FastAPI()
    app.include_router(create_public_router())

    app.dependency_overrides[get_listen_service] = lambda: listen_service
    app.dependency_overrides[get_job_service] = lambda: job_service
    app.dependency_overrides[get_config] = lambda: config
    app.dependency_overrides[get_structured_logger] = lambda: StubStructuredLogger()

    client = TestClient(app)
    return client


def test_listen_sync_success_returns_job_payload() -> None:
    job_id = uuid.uuid4()
    payload = {"result": "ok"}
    listen_service = StubListenService(job_id)
    job_service = StubJobService(_build_job(job_id, JobStatus.SUCCESS, payload=payload))
    config = _build_config(timeout_seconds=5.0, poll_interval_seconds=0.01)

    with _create_client(listen_service, job_service, config) as client:
        response = client.post(
            "/listen/sync",
            json={"job_type": "generate", "payload": {"input": "value"}},
            headers={LISTEN_AUTH_HEADER: LISTEN_AUTH_SECRET},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == str(job_id)
    assert body["status"] == JobStatus.SUCCESS.value
    assert body["result"] == payload
    assert job_service.calls[0][1] == pytest.approx(5.0)
    assert job_service.calls[0][2] == pytest.approx(0.01)


def test_listen_sync_returns_504_on_timeout() -> None:
    job_id = uuid.uuid4()
    listen_service = StubListenService(job_id)
    job_service = StubJobService(_build_job(job_id, JobStatus.PENDING), error=JobWaitTimeoutError("timeout"))
    config = _build_config(timeout_seconds=0.5, poll_interval_seconds=0.05)

    with _create_client(listen_service, job_service, config) as client:
        response = client.post(
            "/listen/sync",
            json={"job_type": "generate", "payload": {"input": "value"}},
            headers={LISTEN_AUTH_HEADER: LISTEN_AUTH_SECRET},
        )

    assert response.status_code == 504
    assert "timeout" in response.json()["detail"]


def test_listen_sync_rejects_invalid_secret() -> None:
    job_id = uuid.uuid4()
    listen_service = StubListenService(job_id)
    job_service = StubJobService(_build_job(job_id, JobStatus.SUCCESS, payload={"ok": True}))
    config = _build_config()

    with _create_client(listen_service, job_service, config) as client:
        response = client.post(
            "/listen/sync",
            json={"job_type": "generate", "payload": {"input": "value"}},
            headers={LISTEN_AUTH_HEADER: "wrong"},
        )

    assert response.status_code == 403
