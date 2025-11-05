from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from orchestrator.clients.miner_metagraph import Miner
from orchestrator.schemas.scores import ScoresResponse
from orchestrator.services.job_scoring import job_to_score
from orchestrator.services.score_service import (
    ScoreHistory,
    ScoreSettings,
    build_scores_from_state,
)


def _make_success_job(completed_at: datetime, latency_ms: float) -> Dict[str, Any]:
    return {
        "status": "success",
        "completed_at": completed_at.isoformat(),
        "metrics": {"latency_ms": latency_ms},
        "job_type": "inference",
        "is_audit_job": False,
    }


def _make_failure_job(completed_at: datetime, status: str = "failed") -> Dict[str, Any]:
    return {
        "status": status,
        "completed_at": completed_at.isoformat(),
        "job_type": "inference",
        "is_audit_job": False,
    }


def test_score_history_success_sequence() -> None:
    settings = ScoreSettings().normalized()
    now = datetime.now(timezone.utc)
    jobs: List[Dict[str, Any]] = [
        _make_success_job(now - timedelta(minutes=10), latency_ms=1_000),
        _make_success_job(now - timedelta(minutes=5), latency_ms=5_000),
    ]

    history = ScoreHistory.from_jobs(
        jobs,
        existing_record=None,
        score_fn=job_to_score,
        settings=settings,
        reference_time=now,
    )

    assert history.success_count == 2
    assert history.failure_count == 0
    last_sample = job_to_score(jobs[-1])
    # Allow minor decay between the last sample timestamp and the reference time.
    assert history.ema_score == pytest.approx(last_sample, rel=1e-3)
    assert history.scores == pytest.approx(last_sample, rel=1e-3)


def test_score_history_decay_halflife() -> None:
    settings = ScoreSettings().normalized()
    past = datetime.now(timezone.utc) - timedelta(days=7)
    record_payload = {
        "scores": 1.0,
        "ema_score": 1.0,
        "success_count": 1,
        "failure_count": 0,
        "sample_count": 1,
        "last_sample_at": past.isoformat(),
        "ema_last_update_at": past.isoformat(),
        "is_slashed": False,
    }

    history = ScoreHistory.from_jobs(
        [],
        existing_record=record_payload,
        score_fn=job_to_score,
        settings=settings,
        reference_time=datetime.now(timezone.utc),
    )

    assert history.scores == pytest.approx(0.5, rel=1e-2)
    assert history.ema_score == pytest.approx(0.5, rel=1e-2)


def test_score_history_failure_penalty_caps_score() -> None:
    settings = ScoreSettings().normalized()
    now = datetime.now(timezone.utc)
    success = _make_success_job(now - timedelta(minutes=2), latency_ms=1_000)
    failure = _make_failure_job(now - timedelta(minutes=1))

    history = ScoreHistory.from_jobs(
        [success, failure],
        existing_record=None,
        score_fn=job_to_score,
        settings=settings,
        reference_time=now,
    )

    expected_penalized = max(1 - settings.failure_penalty_weight, 0)
    assert history.failure_count == 1
    assert history.success_count == 1
    # EMA decays slightly between the sample timestamp and reference time.
    assert history.ema_score == pytest.approx(job_to_score(success), rel=1e-3)
    assert history.scores == pytest.approx(expected_penalized)
    assert history.scores < history.ema_score


class _DummyRelay:
    def __init__(self, jobs: Dict[str, List[Dict[str, Any]]]) -> None:
        self._jobs = jobs

    async def list_jobs_for_hotkey(
        self,
        hotkey: str,
        since: datetime | None = None,
    ) -> List[Dict[str, Any]]:
        jobs = list(self._jobs.get(hotkey, []))
        if since is None:
            return jobs
        cutoff = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
        filtered: List[Dict[str, Any]] = []
        for job in jobs:
            completed = job.get("completed_at")
            event_time: datetime | None
            if isinstance(completed, datetime):
                event_time = completed if completed.tzinfo else completed.replace(tzinfo=timezone.utc)
            elif isinstance(completed, str):
                try:
                    parsed = datetime.fromisoformat(completed.replace("Z", "+00:00"))
                except ValueError:
                    parsed = None
                if parsed is not None and parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                event_time = parsed
            else:
                event_time = None
            if event_time is None or event_time < cutoff:
                continue
            filtered.append(job)
        return filtered


@pytest.mark.asyncio
async def test_build_scores_from_state_applies_failure_penalty() -> None:
    now = datetime.now(timezone.utc)
    success = _make_success_job(now - timedelta(minutes=3), latency_ms=1_000)
    failure = _make_failure_job(now - timedelta(minutes=1), status="failed")

    relay = _DummyRelay({"hk": [success, failure]})
    miner = Miner(
        uid=1,
        network_address="http://miner",
        valid=True,
        alpha_stake=0,
        capacity={},
        hotkey="hk",
    )

    response: ScoresResponse = await build_scores_from_state(
        {"hk": miner},
        job_relay_client=relay,  # type: ignore[arg-type]
        lookback_window=timedelta(hours=1),
    )

    assert "hk" in response.scores
    score_value = response.scores["hk"].score.total_score
    expected_penalized = max(1 - ScoreSettings().failure_penalty_weight, 0)
    assert score_value == pytest.approx(expected_penalized)
    assert response.stats["failures_total"] == 1
