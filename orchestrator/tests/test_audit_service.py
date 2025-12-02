from __future__ import annotations

from datetime import datetime, timezone

from sn_uuid import uuid7

import pytest

from orchestrator.domain.miner import Miner
from orchestrator.schemas.job import AuditStatus, JobRecord, JobStatus
from orchestrator.services.audit_service import AuditService


def _job_record(
    *,
    miner_hotkey: str,
    audit_status: AuditStatus,
    is_audit_job: bool = True,
) -> JobRecord:
    return JobRecord.model_validate(
        {
            "job_id": uuid7(),
            "job_type": "generate",
            "miner_hotkey": miner_hotkey,
            "payload": {},
            "creation_timestamp": datetime.now(timezone.utc),
            "status": JobStatus.success.value,
            "audit_status": audit_status.value,
            "verification_status": "nonverified",
            "is_audit_job": is_audit_job,
        }
    )


class _FakeJobService:
    def __init__(self, jobs: list[JobRecord]) -> None:
        self._jobs = jobs
        self.calls: list[int | None] = []

    async def dump_jobs(self, limit: int | None = None) -> list[JobRecord]:
        self.calls.append(limit)
        if limit is None:
            return list(self._jobs)
        return list(self._jobs[:limit])


class _FakeMetagraphClient:
    def __init__(self, miners: dict[str, Miner]) -> None:
        self._miners = miners
        self.updates: list[Miner] = []

    def get_miner(self, hotkey: str) -> Miner | None:
        return self._miners.get(hotkey)

    def upsert_miner(self, miner: Miner) -> Miner:
        self._miners[miner.hotkey] = miner
        self.updates.append(miner)
        return miner


@pytest.mark.asyncio
async def test_audit_service_runs_in_dry_mode() -> None:
    jobs = [
        _job_record(miner_hotkey="hk-a", audit_status=AuditStatus.audit_success),
        _job_record(miner_hotkey="hk-b", audit_status=AuditStatus.audit_failed, is_audit_job=False),
    ]
    job_service = _FakeJobService(jobs)
    miner_client = _FakeMetagraphClient(
        {
            "hk-a": Miner(
                uid=1,
                network_address="https://example-a",
                valid=False,
                alpha_stake=1,
                hotkey="hk-a",
            )
        }
    )

    service = AuditService(
        job_service=job_service,  # type: ignore[arg-type]
        miner_metagraph_service=miner_client,  # type: ignore[arg-type]
        audit_sample_size=1.0,
        batch_limit=10,
    )

    summary = await service.run_once(apply_changes=False)

    assert summary.jobs_examined == len(jobs)
    assert summary.audit_candidates == 1
    assert summary.miners_marked_valid == 0
    assert summary.miners_marked_invalid == 0
    assert summary.applied_changes is False
    assert miner_client.updates == []


@pytest.mark.asyncio
async def test_audit_service_updates_validity_when_applied() -> None:
    jobs = [
        _job_record(miner_hotkey="hk-b", audit_status=AuditStatus.audit_failed),
    ]
    job_service = _FakeJobService(jobs)
    miner_client = _FakeMetagraphClient(
        {
            "hk-b": Miner(
                uid=2,
                network_address="https://example-b",
                valid=True,
                alpha_stake=1,
                hotkey="hk-b",
            )
        }
    )

    service = AuditService(
        job_service=job_service,  # type: ignore[arg-type]
        miner_metagraph_service=miner_client,  # type: ignore[arg-type]
        audit_sample_size=1.0,
        batch_limit=5,
    )

    summary = await service.run_once(apply_changes=True)

    assert summary.audit_candidates == 1
    assert summary.miners_marked_valid == 0
    assert summary.miners_marked_invalid == 1
    assert miner_client._miners["hk-b"].valid is False
    assert len(miner_client.updates) == 1
