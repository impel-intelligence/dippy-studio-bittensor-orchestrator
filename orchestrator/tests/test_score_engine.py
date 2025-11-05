from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping

import pytest

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.services.score_service import (
    ScoreEngine,
    ScoreRecord,
    ScoreRunSummary,
    ScoreSettings,
)


class InMemoryScoreRepository:
    def __init__(self) -> None:
        self.store: dict[str, ScoreRecord] = {}

    def put_many(self, records: Mapping[str, Mapping[str, Any] | ScoreRecord]) -> None:
        for hotkey, record in records.items():
            if not isinstance(record, ScoreRecord):
                if hasattr(ScoreRecord, "model_validate"):
                    record = ScoreRecord.model_validate(record)  # type: ignore[attr-defined]
                else:
                    record = ScoreRecord(**record)  # type: ignore[arg-type]
            self.store[hotkey] = record

    def all(self) -> Dict[str, ScoreRecord]:
        return dict(self.store)

    def clear(self) -> None:
        self.store.clear()


class MutableJobRelay(BaseJobRelayClient):
    def __init__(self) -> None:
        self.jobs_by_hotkey: dict[str, list[dict[str, Any]]] = {}
        self.fail_for: set[str] = set()

    async def list_jobs_for_hotkey(
        self,
        hotkey: str,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if hotkey in self.fail_for:
            raise RuntimeError("relay unavailable")
        jobs = list(self.jobs_by_hotkey.get(hotkey, ()))
        if since is None:
            return jobs
        cutoff = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
        filtered: list[dict[str, Any]] = []
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


class StubJobService:
    def __init__(self, job_relay: BaseJobRelayClient) -> None:
        self.job_relay = job_relay


def _build_engine(repository: InMemoryScoreRepository, relay: MutableJobRelay) -> ScoreEngine:
    settings = ScoreSettings(ema_alpha=1.0, ema_half_life_seconds=60.0, failure_penalty_weight=0.2)
    return ScoreEngine(
        repository=repository,  # type: ignore[arg-type]
        job_service=StubJobService(relay),  # type: ignore[arg-type]
        fetch_concurrency=1,
        score_settings=settings,
    )


def _success_job(job_id: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "job_type": "inference",
        "status": "success",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {"latency_ms": 1_000},
    }


@pytest.mark.asyncio
async def test_run_once_zeroes_scores_when_jobs_cleared() -> None:
    hotkey = "hk-reset"
    repository = InMemoryScoreRepository()
    relay = MutableJobRelay()
    engine = _build_engine(repository, relay)

    relay.jobs_by_hotkey[hotkey] = [_success_job("job-initial")]
    summary = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(summary, ScoreRunSummary)
    assert summary.success

    stored = repository.store[hotkey]
    assert stored.scores > 0.0

    # Simulate the nuclear wipe: the relay now returns no jobs for the hotkey.
    relay.jobs_by_hotkey[hotkey] = []

    summary = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(summary, ScoreRunSummary)
    assert summary.success

    reset_record = repository.store[hotkey]
    assert reset_record.scores == pytest.approx(0.0)
    assert getattr(reset_record, "sample_count", 0) == 0


@pytest.mark.asyncio
async def test_run_once_preserves_scores_on_fetch_failure() -> None:
    hotkey = "hk-failure"
    repository = InMemoryScoreRepository()
    relay = MutableJobRelay()
    engine = _build_engine(repository, relay)

    relay.jobs_by_hotkey[hotkey] = [_success_job("job-prime")]
    await engine.run_once(trace_hotkeys=[hotkey])
    baseline = repository.store[hotkey]
    assert baseline.scores > 0.0

    relay.fail_for.add(hotkey)

    summary = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(summary, ScoreRunSummary)
    assert summary.success

    preserved = repository.store[hotkey]
    assert preserved.scores == pytest.approx(baseline.scores, rel=1e-4)


@pytest.mark.asyncio
async def test_run_once_uses_rolling_window_without_double_counting() -> None:
    hotkey = "hk-rolling"
    repository = InMemoryScoreRepository()
    relay = MutableJobRelay()
    engine = _build_engine(repository, relay)

    job = _success_job("job-rolling")
    relay.jobs_by_hotkey[hotkey] = [job]

    summary1 = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(summary1, ScoreRunSummary)
    record1 = repository.store[hotkey]
    samples_first = getattr(record1, "sample_count", 0)
    successes_first = getattr(record1, "success_count", 0)
    assert samples_first == 1
    assert successes_first == 1

    summary2 = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(summary2, ScoreRunSummary)
    record2 = repository.store[hotkey]
    samples_second = getattr(record2, "sample_count", 0)
    successes_second = getattr(record2, "success_count", 0)

    assert samples_second == samples_first
    assert successes_second == successes_first
