from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.dependencies import get_audit_repository
from orchestrator.repositories import AuditRecord
from orchestrator.routes.public import create_public_router


class _StubAuditRepository:
    def __init__(self, record: AuditRecord) -> None:
        self._record = record

    def fetch(self, hotkey: str) -> AuditRecord | None:
        return self._record if hotkey == self._record.hotkey else None


def _build_client(record: AuditRecord) -> TestClient:
    app = FastAPI()
    app.include_router(create_public_router())
    app.dependency_overrides[get_audit_repository] = lambda: _StubAuditRepository(
        record
    )
    return TestClient(app)


def test_get_audit_record_success() -> None:
    record = AuditRecord(
        hotkey="hk1",
        failed_job={"job": "failed"},
        reference_job={"job": "reference"},
    )
    client = _build_client(record)

    response = client.get("/audit/hk1")

    assert response.status_code == 200
    assert response.json() == {
        "hotkey": "hk1",
        "failed_job": {"job": "failed"},
        "reference_job": {"job": "reference"},
    }


def test_get_audit_record_not_found() -> None:
    record = AuditRecord(
        hotkey="hk1",
        failed_job={"job": "failed"},
        reference_job={"job": "reference"},
    )
    client = _build_client(record)

    response = client.get("/audit/hk-missing")

    assert response.status_code == 404
