from __future__ import annotations

import uuid

import pytest
from sn_uuid import uuid7

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.common.job_store import Job, JobRequest, JobResponse, JobStatus, JobType
from orchestrator.services.job_service import (
    JobService,
    JobWaitCancelledError,
    JobWaitTimeoutError,
)


class StubJobRelay(BaseJobRelayClient):
    """No-op relay for testing wait logic."""


class FakeJobService(JobService):
    def __init__(self, statuses: list[JobStatus]) -> None:
        super().__init__(job_relay=StubJobRelay())
        self._statuses = statuses[:]
        self._cursor = 0
        self._job = Job(
            job_request=JobRequest(job_type=JobType.GENERATE, payload={"prompt": "test"}),
            hotkey="test-hotkey",
        )

    async def get_job(self, job_id: uuid.UUID) -> Job:
        if self._job.job_id != job_id:
            self._job.job_id = job_id

        index = min(self._cursor, len(self._statuses) - 1)
        self._cursor += 1
        status = self._statuses[index]
        self._job.status = status
        if status == JobStatus.SUCCESS and self._job.job_response is None:
            self._job.job_response = JobResponse(payload={"status": "complete"})
        return self._job


@pytest.mark.asyncio
async def test_wait_for_terminal_state_success() -> None:
    job_id = uuid7()
    service = FakeJobService(
        [JobStatus.PENDING, JobStatus.PENDING, JobStatus.SUCCESS],
    )

    job = await service.wait_for_terminal_state(
        job_id,
        timeout_seconds=1.0,
        poll_interval_seconds=0.01,
    )

    assert job.status == JobStatus.SUCCESS
    assert job.job_response is not None


@pytest.mark.asyncio
async def test_wait_for_terminal_state_times_out() -> None:
    job_id = uuid7()
    service = FakeJobService([JobStatus.PENDING])

    with pytest.raises(JobWaitTimeoutError):
        await service.wait_for_terminal_state(
            job_id,
            timeout_seconds=0.05,
            poll_interval_seconds=0.01,
        )


@pytest.mark.asyncio
async def test_wait_for_terminal_state_cancelled() -> None:
    job_id = uuid7()
    service = FakeJobService(
        [JobStatus.PENDING, JobStatus.PENDING, JobStatus.PENDING],
    )

    call_count = {"value": 0}

    async def disconnected() -> bool:
        call_count["value"] += 1
        return call_count["value"] > 1

    with pytest.raises(JobWaitCancelledError):
        await service.wait_for_terminal_state(
            job_id,
            timeout_seconds=1.0,
            poll_interval_seconds=0.01,
            is_disconnected=disconnected,
        )
