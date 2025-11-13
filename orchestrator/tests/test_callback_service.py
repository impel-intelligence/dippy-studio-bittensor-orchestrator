from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from orchestrator.services.callback_service import CallbackService
from orchestrator.services.callback_uploader import BaseUploader


class _FakeUploadFile:
    def __init__(self, data: bytes, filename: str = "image.png", content_type: str = "image/png") -> None:
        self._data = data
        self.filename = filename
        self.content_type = content_type

    async def read(self) -> bytes:
        return self._data


class _StubUploader(BaseUploader):
    def __init__(self, uri: str) -> None:
        super().__init__()
        self._uri = uri

    def upload_bytes(
        self,
        *,
        job_id: str,
        content: bytes,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> str:
        return self._uri


class _FakeJob:
    def __init__(self) -> None:
        self.job_id = uuid.uuid4()
        self.job_request = SimpleNamespace(timestamp=datetime.now(timezone.utc).timestamp())
        self.dispatched_at = None
        self.callback_secret = "secret"


class _FakeJobService:
    def __init__(self) -> None:
        self.job = _FakeJob()
        self.updated_payload: dict | None = None
        self.failures: list[str] = []
        self.audit_called = False

    async def get_job(self, job_uuid: uuid.UUID) -> _FakeJob:
        assert job_uuid == self.job.job_id
        return self.job

    async def mark_job_failure(self, job_uuid: uuid.UUID, reason: str) -> None:
        self.failures.append(reason)

    async def update_job(self, job_uuid: uuid.UUID, payload: dict) -> _FakeJob:
        self.updated_payload = payload
        return self.job

    async def audit(self, job: _FakeJob) -> None:
        self.audit_called = True


@pytest.mark.asyncio
async def test_process_callback_appends_image_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    uploader = _StubUploader("gs://bucket/image.png")
    service = CallbackService(uploader=uploader)
    expected_hash = "deadbeef"
    job_service = _FakeJobService()
    job_id_str = str(job_service.job.job_id)

    async def _fake_hash(self, *, job_id: str, image_uri: str) -> str | None:  # type: ignore[override]
        assert job_id == job_id_str
        assert image_uri == "gs://bucket/image.png"
        return expected_hash

    monkeypatch.setattr(CallbackService, "_hash_remote_image", _fake_hash)

    image = _FakeUploadFile(b"content-bytes")

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=datetime.now(timezone.utc).isoformat(),
        error=None,
        provided_secret="secret",
        image=image,
        received_at=datetime.now(timezone.utc),
    )

    assert status == "success"
    assert job_service.updated_payload is not None
    assert job_service.updated_payload.get("image_sha256") == expected_hash


class _MockResponse:
    def __init__(self, *, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=None, response=None)


def _mock_async_client(response: _MockResponse, recorder: list[str]):
    class _Client:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def get(self, url: str) -> _MockResponse:
            recorder.append(url)
            return response

    return _Client


@pytest.mark.asyncio
async def test_hash_remote_image_fetches_and_hashes(monkeypatch: pytest.MonkeyPatch) -> None:
    service = CallbackService()
    recorder: list[str] = []
    payload = b"hello-world"
    response = _MockResponse(content=payload)

    monkeypatch.setattr(
        "orchestrator.services.callback_service.httpx.AsyncClient",
        _mock_async_client(response, recorder),
    )

    digest = await service._hash_remote_image(job_id="job-2", image_uri="gs://bucket/path/file.png")

    assert recorder == ["https://storage.googleapis.com/bucket/path/file.png"]
    assert digest == hashlib.sha256(payload).hexdigest()
