from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.common.job_sources import JobSource
from orchestrator.common.job_store import (
    AuditStatus as StoreAuditStatus,
    JobStatus as StoreJobStatus,
)
from orchestrator.services.job_service import JobService
from orchestrator.schemas.job import (
    AuditStatus as SchemaAuditStatus,
    CompletedJobSummary,
    JobRecord,
    JobStatus as SchemaJobStatus,
    VerificationStatus,
)
from sn_uuid import uuid7


class _StubJobRelay:
    def __init__(self, records: list[dict]):
        self._records = records

    async def list_jobs(self, **_: object) -> list[dict]:
        return self._records

    async def list_recent_jobs(self, *, limit: int, **_: object) -> list[dict]:
        return self._records[:limit]


class _FetchStub:
    def __init__(self, record: dict):
        self._record = record

    async def fetch_job(self, *_: object, **__: object) -> dict:
        return self._record


def _make_record(
    *,
    status: StoreJobStatus,
    completed_at: datetime,
    hotkey: str,
) -> dict:
    return {
        "job_id": str(uuid7()),
        "job_type": "generate",
        "miner_hotkey": hotkey,
        "payload": {"foo": "bar"},
        "creation_timestamp": (completed_at - timedelta(minutes=5))
        .isoformat()
        .replace("+00:00", "Z"),
        "status": status.value,
        "audit_status": StoreAuditStatus.NOT_AUDITED.value,
        "verification_status": "nonverified",
        "is_audit_job": False,
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
        "response_payload": {"image_uri": "https://example.test/image.png"},
    }


@pytest.mark.asyncio()
async def test_list_recent_completed_jobs_filters_and_orders() -> None:
    now = datetime.now(timezone.utc)
    records = [
        _make_record(
            status=StoreJobStatus.SUCCESS,
            completed_at=now - timedelta(days=1),
            hotkey="hk1",
        ),
        _make_record(
            status=StoreJobStatus.FAILED,
            completed_at=now - timedelta(hours=1),
            hotkey="hk2",
        ),
        _make_record(
            status=StoreJobStatus.TIMEOUT,
            completed_at=now - timedelta(days=8),
            hotkey="hk3",
        ),
    ]
    service = JobService(job_relay=_StubJobRelay(records))  # type: ignore[arg-type]

    jobs = await service.list_recent_completed_jobs(max_results=2, lookback_days=7)

    assert len(jobs) == 2
    assert jobs[0].miner_hotkey == "hk2"
    assert jobs[1].miner_hotkey == "hk1"


@pytest.mark.asyncio()
async def test_list_recent_completed_jobs_skips_non_completed() -> None:
    now = datetime.now(timezone.utc)
    completed = _make_record(
        status=StoreJobStatus.SUCCESS,
        completed_at=now - timedelta(days=2),
        hotkey="hk-good",
    )
    pending = dict(completed)
    pending["status"] = StoreJobStatus.PENDING.value

    service = JobService(job_relay=_StubJobRelay([pending, completed]))  # type: ignore[arg-type]

    jobs = await service.list_recent_completed_jobs(max_results=5, lookback_days=7)

    assert len(jobs) == 1
    assert jobs[0].miner_hotkey == "hk-good"


@pytest.mark.asyncio()
async def test_list_recent_completed_jobs_skips_audit_original_source() -> None:
    now = datetime.now(timezone.utc)
    audit_record = _make_record(
        status=StoreJobStatus.SUCCESS,
        completed_at=now - timedelta(hours=1),
        hotkey="hk-audit",
    )
    audit_record["source"] = JobSource.AUDIT_ORIGINAL.value
    normal_record = _make_record(
        status=StoreJobStatus.SUCCESS,
        completed_at=now - timedelta(hours=2),
        hotkey="hk-normal",
    )

    service = JobService(job_relay=_StubJobRelay([audit_record, normal_record]))  # type: ignore[arg-type]

    jobs = await service.list_recent_completed_jobs(max_results=5, lookback_days=7)

    assert [job.miner_hotkey for job in jobs] == ["hk-normal"]


def test_completed_job_summary_masks_prompts() -> None:
    now = datetime.now(timezone.utc)
    record = JobRecord(
        job_id=uuid7(),
        job_type="generate",
        miner_hotkey="hk-secret",
        payload={"prompt": "visible"},
        result_image_url=None,
        result_image_sha256=None,
        creation_timestamp=now,
        last_updated_at=now,
        miner_received_at=now,
        completed_at=now,
        execution_duration_ms=None,
        expires_at=None,
        status=SchemaJobStatus.success,
        audit_status=SchemaAuditStatus.not_audited,
        verification_status=VerificationStatus.nonverified,
        is_audit_job=False,
        audit_target_job_id=None,
        prompt_seed=None,
        callback_secret=None,
        prepared_at=now,
        dispatched_at=now,
        failure_reason=None,
        response_payload={
            "prompt": "super secret",
            "other": "value",
            "nested": {"Prompt": "Another secret"},
            "list_prompts": ["alpha", "beta"],
            "callback_secret": "should not leak",
            "nested_secret": {"callback_secret": "nested-secret"},
            "list_with_secret": [
                {"callback_secret": "list-secret", "prompt": "list prompt"},
                "plain",
            ],
        },
        response_timestamp=now,
        audit_id=None,
    )

    summary = CompletedJobSummary.from_job_record(record)

    assert summary.response_payload is not None
    assert summary.response_payload["prompt"].startswith("sha256:")
    assert summary.response_payload["nested"]["Prompt"].startswith("sha256:")
    assert summary.response_payload["list_prompts"][0].startswith("sha256:")
    assert summary.response_payload["other"] == "value"
    assert "callback_secret" not in summary.response_payload
    assert "callback_secret" not in summary.response_payload["nested_secret"]
    first_list_entry = summary.response_payload["list_with_secret"][0]
    assert "callback_secret" not in first_list_entry
    assert first_list_entry["prompt"].startswith("sha256:")


@pytest.mark.asyncio()
async def test_fetch_masked_job_record_hides_source_metadata() -> None:
    now = datetime.now(timezone.utc)
    job_id = uuid7()
    record = {
        "job_id": str(job_id),
        "job_type": "generate",
        "miner_hotkey": "hk-source",
        "payload": {
            "prompt": "keep secret",
            "source": "audit_original",
            "callback_secret": "payload-secret",
        },
        "response_payload": {
            "image_url": "https://example.test/path.png",
            "source": "audit_original",
            "callback_secret": "response-secret",
        },
        "source": "audit_original",
        "creation_timestamp": now.isoformat().replace("+00:00", "Z"),
        "status": StoreJobStatus.PENDING.value,
        "audit_status": StoreAuditStatus.NOT_AUDITED.value,
        "verification_status": "nonverified",
        "is_audit_job": False,
        "callback_secret": "top-level-secret",
    }
    service = JobService(job_relay=_FetchStub(record))  # type: ignore[arg-type]

    sanitized = await service.fetch_masked_job_record(job_id=job_id)

    assert "source" not in sanitized
    assert "source" not in sanitized["payload"]
    assert "callback_secret" not in sanitized["payload"]
    assert sanitized["payload"]["prompt"].startswith("sha256:")
    assert "source" not in sanitized.get("response_payload", {})
    assert "callback_secret" not in sanitized.get("response_payload", {})
