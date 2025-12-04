from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from sn_uuid import uuid7

from orchestrator.services.callback_service import CallbackService
from orchestrator.clients.storage import BaseUploader
from orchestrator.services.exceptions import CallbackValidationError


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
        self.calls: list[dict[str, object]] = []

    def upload_bytes(
        self,
        *,
        job_id: str,
        content: bytes,
        filename: str | None = None,
        content_type: str | None = None,
        job_type: str | None = None,
    ) -> str:
        self.calls.append(
            {
                "job_id": job_id,
                "filename": filename,
                "content_type": content_type,
                "job_type": job_type,
            }
        )
        return self._uri


class _StubWebhookDispatcher:
    def __init__(self, *, succeed: bool = True) -> None:
        self.calls: list[dict[str, object]] = []
        self.succeed = succeed

    def dispatch(
        self,
        *,
        webhook_url: str,
        job_id: str,
        status: str,
        completed_at: str,
        error: str | None,
        latencies: dict,
        image_bytes: bytes | None,
        image_filename: str | None,
        image_content_type: str | None,
    ) -> bool:
        self.calls.append(
            {
                "webhook_url": webhook_url,
                "job_id": job_id,
                "status": status,
                "completed_at": completed_at,
                "error": error,
                "latencies": latencies,
                "has_image": image_bytes is not None,
                "image_filename": image_filename,
                "image_content_type": image_content_type,
            }
        )
        return self.succeed


class _FakeJob:
    def __init__(self, payload: dict | None = None) -> None:
        self.job_id = uuid7()
        self.job_request = SimpleNamespace(
            timestamp=datetime.now(timezone.utc).timestamp(),
            job_type="generate",
            payload=payload or {},
        )
        self.dispatched_at = None
        self.callback_secret = "secret"
        self.is_audit_job = False


class _FakeJobService:
    def __init__(self, job: _FakeJob | None = None) -> None:
        self.job = job or _FakeJob()
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
    assert uploader.calls and uploader.calls[-1]["job_type"] == "generate"


@pytest.mark.asyncio
async def test_process_callback_forwards_to_webhook_when_present() -> None:
    dispatcher = _StubWebhookDispatcher()
    service = CallbackService(webhook_dispatcher=dispatcher)
    job = _FakeJob(payload={"webhook_url": "https://example.com/webhook"})
    job_service = _FakeJobService(job=job)
    job_id_str = str(job_service.job.job_id)

    image = _FakeUploadFile(b"content-bytes", filename="image.png", content_type="image/png")

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
    assert dispatcher.calls and dispatcher.calls[-1]["webhook_url"] == "https://example.com/webhook"
    assert job_service.updated_payload is not None
    assert job_service.updated_payload["callback_metadata"]["webhook_forwarded"] is True


@pytest.mark.asyncio
async def test_process_callback_marks_webhook_forward_false_when_missing_dispatcher() -> None:
    service = CallbackService(webhook_dispatcher=None)
    job = _FakeJob(payload={"webhook_url": "https://example.com/webhook"})
    job_service = _FakeJobService(job=job)
    job_id_str = str(job_service.job.job_id)

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=datetime.now(timezone.utc).isoformat(),
        error=None,
        provided_secret="secret",
        image=None,
        received_at=datetime.now(timezone.utc),
    )

    assert status == "success"
    assert job_service.updated_payload is not None
    assert job_service.updated_payload["callback_metadata"]["webhook_forwarded"] is False


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


@pytest.mark.asyncio
async def test_process_callback_enforces_flux_kontext_runtime_minimum() -> None:
    service = CallbackService()
    job_service = _FakeJobService()
    job_service.job.job_request.job_type = "img-h100_pcie"
    received_at = datetime.now(timezone.utc)
    job_service.job.job_request.timestamp = (received_at - timedelta(seconds=2)).timestamp()
    job_id_str = str(job_service.job.job_id)

    with pytest.raises(CallbackValidationError):
        await service.process_callback(
            job_service=job_service,
            job_id=job_id_str,
            status="success",
            completed_at=received_at.isoformat(),
            error=None,
            provided_secret="secret",
            image=None,
            received_at=received_at,
        )

    assert job_service.failures[-1] == "flux_kontext_min_runtime_violation"
    assert job_service.updated_payload is None


@pytest.mark.asyncio
async def test_process_callback_allows_flux_kontext_runtime_after_threshold() -> None:
    service = CallbackService()
    job_service = _FakeJobService()
    job_service.job.job_request.job_type = "img-h100_pcie"
    received_at = datetime.now(timezone.utc)
    job_service.job.job_request.timestamp = (received_at - timedelta(seconds=20)).timestamp()
    job_id_str = str(job_service.job.job_id)

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=received_at.isoformat(),
        error=None,
        provided_secret="secret",
        image=None,
        received_at=received_at,
    )

    assert status == "success"
    assert job_service.updated_payload is not None


@pytest.mark.asyncio
async def test_process_callback_marks_flux_kontext_latency_timeout_failed() -> None:
    service = CallbackService()
    job_service = _FakeJobService()
    job_service.job.job_request.job_type = "img-h100_pcie"
    received_at = datetime.now(timezone.utc)
    job_service.job.job_request.timestamp = (received_at - timedelta(seconds=25)).timestamp()
    job_service.job.dispatched_at = (received_at - timedelta(seconds=16)).timestamp()
    job_id_str = str(job_service.job.job_id)

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=received_at.isoformat(),
        error=None,
        provided_secret="secret",
        image=None,
        received_at=received_at,
    )

    assert status == "error"
    assert job_service.failures[-1] == "flux_kontext_max_latency_violation"
    assert job_service.updated_payload is not None


@pytest.mark.asyncio
async def test_process_callback_allows_flux_kontext_latency_within_threshold() -> None:
    service = CallbackService()
    job_service = _FakeJobService()
    job_service.job.job_request.job_type = "img-h100_pcie"
    received_at = datetime.now(timezone.utc)
    job_service.job.job_request.timestamp = (received_at - timedelta(seconds=20)).timestamp()
    job_service.job.dispatched_at = (received_at - timedelta(seconds=10)).timestamp()
    job_id_str = str(job_service.job.job_id)

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=received_at.isoformat(),
        error=None,
        provided_secret="secret",
        image=None,
        received_at=received_at,
    )

    assert status == "success"
    assert not job_service.failures


@pytest.mark.asyncio
async def test_process_callback_skips_validations_for_audit_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    service = CallbackService()
    job_service = _FakeJobService()
    job_service.job.is_audit_job = True
    job_service.job.job_request.job_type = "img-h100_pcie"
    received_at = datetime.now(timezone.utc)
    job_service.job.job_request.timestamp = (received_at - timedelta(seconds=2)).timestamp()
    job_service.job.dispatched_at = (received_at - timedelta(seconds=20)).timestamp()
    job_id_str = str(job_service.job.job_id)

    def _fail_detect(self, *, job_type, dispatch_latency_ms, job_uuid):  # type: ignore[override]
        raise AssertionError("latency validation should be skipped for audit jobs")

    async def _fail_enforce(self, *, job_type, total_runtime_ms, job_service, job_uuid):  # type: ignore[override]
        raise AssertionError("runtime validation should be skipped for audit jobs")

    monkeypatch.setattr(CallbackService, "_detect_flux_kontext_latency_violation", _fail_detect)
    monkeypatch.setattr(CallbackService, "_enforce_flux_kontext_runtime", _fail_enforce)

    status, _ = await service.process_callback(
        job_service=job_service,
        job_id=job_id_str,
        status="success",
        completed_at=received_at.isoformat(),
        error=None,
        provided_secret="secret",
        image=None,
        received_at=received_at,
    )

    assert status == "success"
    assert not job_service.failures
    assert job_service.updated_payload is not None
