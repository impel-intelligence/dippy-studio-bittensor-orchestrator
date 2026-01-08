from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from orchestrator.common.job_sources import JobSource
from orchestrator.runners.audit_broadcast import AuditBroadcastRunner


class _StubListenService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def _create_job(
        self, *, job_type, payload, miner, desired_job_id=None, source=None
    ):  # type: ignore[no-untyped-def]
        job_id = uuid4()
        self.calls.append(
            {
                "job_type": job_type,
                "payload": payload,
                "miner": miner,
                "source": source,
                "job_id": job_id,
            }
        )
        return SimpleNamespace(job_id=job_id)

    def _build_dispatch_payload(self, job):  # type: ignore[no-untyped-def]
        return {}

    def _resolve_inference_url(self, miner, job_type):  # type: ignore[no-untyped-def]
        return "http://example.test"

    async def _dispatch(self, job, miner, inference_url, payload):  # type: ignore[no-untyped-def]
        return True


@pytest.mark.asyncio()
async def test_audit_broadcast_dispatches_with_audit_check_source() -> None:
    listen_service = _StubListenService()
    runner = AuditBroadcastRunner(
        job_service=SimpleNamespace(),
        miner_metagraph_service=SimpleNamespace(),
        score_service=SimpleNamespace(),
        listen_service=listen_service,
        audit_miner=SimpleNamespace(hotkey="audit-hk"),
        netuid=1,
        network="testnet",
    )

    miners = [SimpleNamespace(hotkey="hk1"), SimpleNamespace(hotkey="hk2")]
    result = await runner._dispatch_to_miners(miners, payload={"foo": "bar"})

    assert len(listen_service.calls) == len(miners)
    assert all(call["source"] == JobSource.AUDIT_CHECK for call in listen_service.calls)
    assert set(result["job_ids"].keys()) == {miner.hotkey for miner in miners}
