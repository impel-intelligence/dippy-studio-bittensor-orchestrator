from __future__ import annotations

from types import SimpleNamespace
from urllib import error as urllib_error

import pytest

from orchestrator.common.job_store import JobRequest, JobType
from orchestrator.services.listen_service import ListenService
from sn_uuid import uuid7


class _StubJobService:
    def __init__(self) -> None:
        self.created: list[tuple[JobType, dict[str, object]]] = []
        self.failures: list[tuple[str, str]] = []
        self.prepared: list[str] = []
        self.dispatched: list[str] = []

    async def create_job(self, *, job_type: JobType, payload: dict[str, object], hotkey: str, job_id=None):
        job = SimpleNamespace(
            job_id=uuid7(),
            callback_secret="secret",
            prompt_seed=123,
            job_request=JobRequest(job_type=job_type, payload=payload),
        )
        self.created.append((job_type, payload))
        return job

    async def mark_job_failure(self, job_id, reason):  # noqa: ANN001
        self.failures.append((str(job_id), reason))

    async def mark_job_prepared(self, job_id):  # noqa: ANN001
        self.prepared.append(str(job_id))

    async def mark_job_dispatched(self, job_id):  # noqa: ANN001
        self.dispatched.append(str(job_id))


class _StubMetagraph:
    def __init__(self, miner=None) -> None:
        self.miner = miner
        self.failures: list[str] = []

    def fetch_candidate(self, task_type: str | None = None):  # noqa: ARG002
        return self.miner

    def record_request_failure(self, hotkey: str, *, increment: int = 1) -> None:  # noqa: ARG002
        self.failures.append(hotkey)


class _StubEpistula:
    def __init__(self, *, raise_error: bool = False) -> None:
        self.raise_error = raise_error

    async def post_signed_request(self, **kwargs):  # noqa: ANN001
        if self.raise_error:
            raise urllib_error.URLError("timeout")
        return 200, "ok"


class _StubLogger:
    def debug(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def info(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def warning(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass

    def error(self, *args, **kwargs) -> None:  # pragma: no cover - noop logger
        pass


def _make_service(default_callback_url: str | None = None) -> ListenService:
    return ListenService(
        job_service=_StubJobService(),
        metagraph=_StubMetagraph(),
        logger=_StubLogger(),
        epistula_client=_StubEpistula(),
        callback_url=default_callback_url,
    )


def _job(job_type: JobType, payload: dict[str, object] | None = None) -> SimpleNamespace:
    request_payload = {"callback_url": "https://callback.example"}
    if payload:
        request_payload.update(payload)
    return SimpleNamespace(
        job_id=uuid7(),
        callback_secret="secret",
        prompt_seed=123,
        job_request=JobRequest(job_type=job_type, payload=request_payload),
    )


def _miner(address: str) -> SimpleNamespace:
    return SimpleNamespace(network_address=address)


def test_apply_listen_payload_overrides_caps_kontext_steps() -> None:
    listen_service = _make_service()
    payload = {"callback_url": "https://callback.example", "num_inference_steps": 30}

    updated = listen_service._apply_listen_payload_overrides(JobType.FLUX_KONTEXT, payload)

    assert updated["num_inference_steps"] == 10
    assert payload["num_inference_steps"] == 30


def test_apply_listen_payload_overrides_respects_existing_cap() -> None:
    listen_service = _make_service()
    payload = {"callback_url": "https://callback.example", "num_inference_steps": 8}

    updated = listen_service._apply_listen_payload_overrides(JobType.FLUX_KONTEXT, payload)

    assert updated["num_inference_steps"] == 8


def test_apply_listen_payload_overrides_ignores_non_kontext_jobs() -> None:
    listen_service = _make_service()
    payload = {"callback_url": "https://callback.example", "num_inference_steps": 42}

    updated = listen_service._apply_listen_payload_overrides(JobType.FLUX_DEV, payload)

    assert updated["num_inference_steps"] == 42


def test_build_dispatch_payload_includes_flux_dev_overrides() -> None:
    listen_service = _make_service()
    job = _job(JobType.FLUX_DEV)

    payload = listen_service._build_dispatch_payload(job)

    assert payload["task_type"] == JobType.FLUX_DEV.value
    assert payload["flux_mode"] == "dev"


def test_build_dispatch_payload_includes_flux_kontext_overrides() -> None:
    listen_service = _make_service()
    job = _job(JobType.FLUX_KONTEXT)

    payload = listen_service._build_dispatch_payload(job)

    assert payload["task_type"] == JobType.FLUX_KONTEXT.value
    assert payload["flux_mode"] == "kontext"


def test_existing_fields_are_not_overwritten() -> None:
    listen_service = _make_service()
    job = _job(
        JobType.FLUX_DEV,
        payload={
            "flux_mode": "custom",
            "task_type": "custom-type",
        },
    )

    payload = listen_service._build_dispatch_payload(job)

    assert payload["flux_mode"] == "custom"
    assert payload["task_type"] == "custom-type"


def test_resolve_inference_url_defaults_to_inference_endpoint() -> None:
    listen_service = _make_service()
    miner = _miner("https://miner.example/api")

    url = listen_service._resolve_inference_url(miner, JobType.FLUX_DEV)

    assert url == "https://miner.example/api/inference"


def test_resolve_inference_url_uses_edit_endpoint_for_kontext_jobs() -> None:
    listen_service = _make_service()
    miner = _miner("https://miner.example/api")

    url = listen_service._resolve_inference_url(miner, JobType.FLUX_KONTEXT)

    assert url == "https://miner.example/api/edit"


def test_resolve_inference_url_preserves_existing_edit_endpoint() -> None:
    listen_service = _make_service()
    miner = _miner("https://miner.example/api/edit/")

    url = listen_service._resolve_inference_url(miner, JobType.FLUX_KONTEXT)

    assert url == "https://miner.example/api/edit"


@pytest.mark.asyncio
async def test_process_caps_inference_steps_before_job_creation() -> None:
    miner = SimpleNamespace(hotkey="hk", network_address="https://miner.example")
    job_service = _StubJobService()
    listen_service = ListenService(
        job_service=job_service,
        metagraph=_StubMetagraph(miner=miner),
        logger=_StubLogger(),
        epistula_client=_StubEpistula(),
        callback_url="https://callback.example",
    )

    payload = {"callback_url": "https://callback.example", "num_inference_steps": 25}

    await listen_service.process(
        job_type=JobType.FLUX_KONTEXT,
        payload=payload,
        desired_job_id=None,
    )

    assert job_service.created
    _, created_payload = job_service.created[0]
    assert created_payload["num_inference_steps"] == 10
    assert payload["num_inference_steps"] == 25


@pytest.mark.asyncio
async def test_dispatch_failure_penalizes_miner() -> None:
    miner = SimpleNamespace(hotkey="hk-fail", network_address="https://miner.example")
    job_service = _StubJobService()
    metagraph = _StubMetagraph(miner=miner)
    listen_service = ListenService(
        job_service=job_service,
        metagraph=metagraph,
        logger=_StubLogger(),
        epistula_client=_StubEpistula(raise_error=True),
        callback_url="https://callback.example",
    )

    job_id = await listen_service.process(
        job_type=JobType.FLUX_DEV,
        payload={"callback_url": "https://callback.example"},
        desired_job_id=None,
    )

    assert job_service.failures and job_service.failures[0][0] == str(job_id)
    assert metagraph.failures == ["hk-fail"]
