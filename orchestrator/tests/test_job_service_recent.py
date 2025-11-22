from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.common.job_store import AuditStatus as StoreAuditStatus, JobStatus as StoreJobStatus
from orchestrator.services.job_service import JobService
from orchestrator.schemas.job import (
    AuditStatus as SchemaAuditStatus,
    CompletedJobSummary,
    JobRecord,
    JobStatus as SchemaJobStatus,
    VerificationStatus,
)


class _StubJobRelay:
    def __init__(self, records: list[dict]):
        self._records = records

    async def list_jobs(self) -> list[dict]:
        return self._records

    async def list_recent_jobs(self, *, limit: int) -> list[dict]:
        return self._records[:limit]


def _make_record(
    *,
    status: StoreJobStatus,
    completed_at: datetime,
    hotkey: str,
) -> dict:
    return {
        "job_id": str(uuid.uuid4()),
        "job_type": "generate",
        "miner_hotkey": hotkey,
        "payload": {"foo": "bar"},
        "creation_timestamp": (completed_at - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
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
        _make_record(status=StoreJobStatus.SUCCESS, completed_at=now - timedelta(days=1), hotkey="hk1"),
        _make_record(status=StoreJobStatus.FAILED, completed_at=now - timedelta(hours=1), hotkey="hk2"),
        _make_record(status=StoreJobStatus.TIMEOUT, completed_at=now - timedelta(days=8), hotkey="hk3"),
    ]
    service = JobService(job_relay=_StubJobRelay(records))  # type: ignore[arg-type]

    jobs = await service.list_recent_completed_jobs(max_results=2, lookback_days=7)

    assert len(jobs) == 2
    assert jobs[0].miner_hotkey == "hk2"
    assert jobs[1].miner_hotkey == "hk1"


@pytest.mark.asyncio()
async def test_list_recent_completed_jobs_skips_non_completed() -> None:
    now = datetime.now(timezone.utc)
    completed = _make_record(status=StoreJobStatus.SUCCESS, completed_at=now - timedelta(days=2), hotkey="hk-good")
    pending = dict(completed)
    pending["status"] = StoreJobStatus.PENDING.value

    service = JobService(job_relay=_StubJobRelay([pending, completed]))  # type: ignore[arg-type]

    jobs = await service.list_recent_completed_jobs(max_results=5, lookback_days=7)

    assert len(jobs) == 1
    assert jobs[0].miner_hotkey == "hk-good"


def test_completed_job_summary_masks_prompts() -> None:
    now = datetime.now(timezone.utc)
    record = JobRecord(
        job_id=uuid.uuid4(),
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
