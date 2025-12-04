from __future__ import annotations

import uuid

import pytest

from orchestrator.common.job_store import JobStatus
from orchestrator.services.job_service import JobWaitCancelledError, JobWaitTimeoutError
from orchestrator.services.sync_waiter import SyncCallbackResult, SyncCallbackWaiter


@pytest.mark.asyncio
async def test_waiter_resolves_on_callback() -> None:
    waiter = SyncCallbackWaiter()
    job_id = uuid.uuid4()
    await waiter.register(job_id)

    result = SyncCallbackResult(job_id=job_id, status=JobStatus.SUCCESS, payload={"ok": True})
    await waiter.resolve(result)

    observed = await waiter.wait_for_result(job_id, timeout_seconds=1.0, poll_interval_seconds=0.05)
    assert observed.status is JobStatus.SUCCESS
    assert observed.payload == {"ok": True}


@pytest.mark.asyncio
async def test_waiter_times_out() -> None:
    waiter = SyncCallbackWaiter()
    job_id = uuid.uuid4()

    await waiter.register(job_id)
    with pytest.raises(JobWaitTimeoutError):
        await waiter.wait_for_result(job_id, timeout_seconds=0.05, poll_interval_seconds=0.01)


@pytest.mark.asyncio
async def test_waiter_cancels_on_disconnect() -> None:
    waiter = SyncCallbackWaiter()
    job_id = uuid.uuid4()
    await waiter.register(job_id)

    async def disconnected() -> bool:
        return True

    with pytest.raises(JobWaitCancelledError):
        await waiter.wait_for_result(
            job_id,
            timeout_seconds=1.0,
            poll_interval_seconds=0.05,
            is_disconnected=disconnected,
        )
