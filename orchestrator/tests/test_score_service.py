from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.clients.miner_metagraph import Miner
from orchestrator.services.score_service import build_scores_from_state


class StubJobRelay(BaseJobRelayClient):
    def __init__(self, jobs_by_hotkey: dict[str, list[dict]]) -> None:
        self._jobs_by_hotkey = jobs_by_hotkey

    async def list_jobs_for_hotkey(self, hotkey: str) -> list[dict]:
        return self._jobs_by_hotkey.get(hotkey, [])


class FailingJobRelay(BaseJobRelayClient):

    async def list_jobs_for_hotkey(self, hotkey: str) -> list[dict]:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_build_scores_from_state_aggregates_recent_inference_jobs() -> None:
    now = datetime.now(timezone.utc)
    recent_completion = (now - timedelta(days=1)).isoformat()
    old_completion = (now - timedelta(days=8)).isoformat()

    hotkey = "hk1"
    miner = Miner(
        uid=1,
        network_address="https://miner.example.com",
        valid=True,
        alpha_stake=1000,
        hotkey=hotkey,
    )

    jobs_by_hotkey = {
        hotkey: [
            {
                "job_id": "job-recent",
                "job_type": "inference",
                "status": "success",
                "completed_at": recent_completion,
                "execution_duration_ms": 1000,
            },
            {
                "job_id": "job-old",
                "job_type": "inference",
                "status": "success",
                "completed_at": old_completion,
                "execution_duration_ms": 1000,
            },
            {
                "job_id": "job-failed",
                "job_type": "inference",
                "status": "failed",
                "completed_at": recent_completion,
                "execution_duration_ms": 1000,
            },
        ]
    }

    relay = StubJobRelay(jobs_by_hotkey)
    state = {hotkey: miner}

    response = await build_scores_from_state(state, job_relay_client=relay)

    assert hotkey in response.scores
    payload = response.scores[hotkey]
    # Recent success contributes, while the accompanying failure applies the penalty weight.
    assert payload.score.total_score == pytest.approx(0.8, rel=1e-3)
    assert response.stats["jobs_considered"] == 2
    assert response.stats["jobs_scored"] == 1
    assert response.stats["fetch_failures"] == 0


@pytest.mark.asyncio
async def test_build_scores_from_state_handles_fetch_errors() -> None:
    hotkey = "hk2"
    miner = Miner(
        uid=2,
        network_address="https://miner2.example.com",
        valid=True,
        alpha_stake=800,
        hotkey=hotkey,
    )

    relay = FailingJobRelay()
    state = {hotkey: miner}

    response = await build_scores_from_state(state, job_relay_client=relay)

    assert response.scores[hotkey].score.total_score == 0.0
    assert response.stats["fetch_failures"] == 1
