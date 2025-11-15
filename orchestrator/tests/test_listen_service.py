from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from orchestrator.common.job_store import JobRequest, JobType
from orchestrator.services.listen_service import ListenEngine


class _StubJobService:
    pass


class _StubMetagraph:
    pass


class _StubEpistula:
    pass


def _make_engine(default_callback_url: str | None = None) -> ListenEngine:
    return ListenEngine(
        job_service=_StubJobService(),
        metagraph=_StubMetagraph(),
        epistula_client=_StubEpistula(),
        default_callback_url=default_callback_url,
    )


def _job(job_type: JobType, payload: dict[str, object] | None = None) -> SimpleNamespace:
    request_payload = {"callback_url": "https://callback.example"}
    if payload:
        request_payload.update(payload)
    return SimpleNamespace(
        job_id=uuid.uuid4(),
        callback_secret="secret",
        prompt_seed=123,
        job_request=JobRequest(job_type=job_type, payload=request_payload),
    )


def _miner(address: str) -> SimpleNamespace:
    return SimpleNamespace(network_address=address)


def test_build_dispatch_payload_includes_flux_dev_overrides() -> None:
    engine = _make_engine()
    job = _job(JobType.FLUX_DEV)

    payload = engine._build_dispatch_payload(job)

    assert payload["task_type"] == JobType.FLUX_DEV.value
    assert payload["flux_mode"] == "dev"


def test_build_dispatch_payload_includes_flux_kontext_overrides() -> None:
    engine = _make_engine()
    job = _job(JobType.FLUX_KONTEXT)

    payload = engine._build_dispatch_payload(job)

    assert payload["task_type"] == JobType.FLUX_KONTEXT.value
    assert payload["flux_mode"] == "kontext"


def test_existing_fields_are_not_overwritten() -> None:
    engine = _make_engine()
    job = _job(
        JobType.FLUX_DEV,
        payload={
            "flux_mode": "custom",
            "task_type": "custom-type",
        },
    )

    payload = engine._build_dispatch_payload(job)

    assert payload["flux_mode"] == "custom"
    assert payload["task_type"] == "custom-type"


def test_resolve_inference_url_defaults_to_inference_endpoint() -> None:
    engine = _make_engine()
    miner = _miner("https://miner.example/api")

    url = engine._resolve_inference_url(miner, JobType.FLUX_DEV)

    assert url == "https://miner.example/api/inference"


def test_resolve_inference_url_uses_edit_endpoint_for_kontext_jobs() -> None:
    engine = _make_engine()
    miner = _miner("https://miner.example/api")

    url = engine._resolve_inference_url(miner, JobType.FLUX_KONTEXT)

    assert url == "https://miner.example/api/edit"


def test_resolve_inference_url_preserves_existing_edit_endpoint() -> None:
    engine = _make_engine()
    miner = _miner("https://miner.example/api/edit/")

    url = engine._resolve_inference_url(miner, JobType.FLUX_KONTEXT)

    assert url == "https://miner.example/api/edit"
