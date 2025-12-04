from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.common.job_store import JobStatus, JobType
from orchestrator.config import ListenConfig, ListenSyncConfig, OrchestratorConfig
from orchestrator.dependencies import (
    get_config,
    get_listen_service,
    get_structured_logger,
)
from orchestrator.routes.public import LISTEN_AUTH_HEADER, LISTEN_AUTH_SECRET, create_public_router
from orchestrator.services.listen_service import SyncDispatchResult
from sn_uuid import uuid7


class StubStructuredLogger:
    def info(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def warning(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def error(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass


class StubListenService:
    def __init__(self, sync_result: SyncDispatchResult) -> None:
        self.sync_result = sync_result
        self.sync_calls: list[tuple[JobType, dict[str, object], float, float, object | None]] = []

    async def process_sync(
        self,
        *,
        job_type: JobType,
        payload: dict[str, object],
        timeout_seconds: float,
        poll_interval_seconds: float,
        desired_job_id=None,
        is_disconnected=None,
    ) -> SyncDispatchResult:
        self.sync_calls.append((job_type, payload, timeout_seconds, poll_interval_seconds, desired_job_id))
        return self.sync_result


def _build_config(timeout_seconds: float = 10.0, poll_interval_seconds: float = 1.0) -> OrchestratorConfig:
    config = OrchestratorConfig()
    config.listen = ListenConfig(sync=ListenSyncConfig(timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds))
    return config


def _create_client(listen_service: StubListenService, config: OrchestratorConfig) -> TestClient:
    app = FastAPI()
    app.include_router(create_public_router())

    app.dependency_overrides[get_listen_service] = lambda: listen_service
    app.dependency_overrides[get_config] = lambda: config
    app.dependency_overrides[get_structured_logger] = lambda: StubStructuredLogger()

    client = TestClient(app)
    return client


def test_listen_sync_success_returns_job_payload() -> None:
    job_id = uuid7()
    payload = {"result": "ok"}
    sync_result = SyncDispatchResult(
        success=True,
        job_id=str(job_id),
        job_status=JobStatus.SUCCESS,
        result_payload=payload,
    )
    listen_service = StubListenService(sync_result)
    config = _build_config(timeout_seconds=5.0, poll_interval_seconds=0.01)

    with _create_client(listen_service, config) as client:
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
    assert listen_service.sync_calls[0][2] == pytest.approx(5.0)
    assert listen_service.sync_calls[0][3] == pytest.approx(0.01)


def test_listen_sync_returns_504_on_timeout() -> None:
    job_id = uuid7()
    sync_result = SyncDispatchResult(
        success=False,
        job_id=str(job_id),
        job_status=JobStatus.TIMEOUT,
        error="timeout",
        status_code=504,
    )
    listen_service = StubListenService(sync_result)
    config = _build_config(timeout_seconds=0.5, poll_interval_seconds=0.05)

    with _create_client(listen_service, config) as client:
        response = client.post(
            "/listen/sync",
            json={"job_type": "generate", "payload": {"input": "value"}},
            headers={LISTEN_AUTH_HEADER: LISTEN_AUTH_SECRET},
        )

    assert response.status_code == 504
    assert "timeout" in response.json()["detail"]


def test_listen_sync_rejects_invalid_secret() -> None:
    job_id = uuid7()
    sync_result = SyncDispatchResult(
        success=True,
        job_id=str(job_id),
        job_status=JobStatus.SUCCESS,
        result_payload={"ok": True},
    )
    listen_service = StubListenService(sync_result)
    config = _build_config()

    with _create_client(listen_service, config) as client:
        response = client.post(
            "/listen/sync",
            json={"job_type": "generate", "payload": {"input": "value"}},
            headers={LISTEN_AUTH_HEADER: "wrong"},
        )

    assert response.status_code == 403
