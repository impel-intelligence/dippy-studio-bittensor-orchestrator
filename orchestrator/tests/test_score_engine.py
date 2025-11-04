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

    async def list_jobs_for_hotkey(self, hotkey: str) -> list[dict[str, Any]]:
        if hotkey in self.fail_for:
            raise RuntimeError("relay unavailable")
        return list(self.jobs_by_hotkey.get(hotkey, ()))


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


def _failure_job(job_id: str, *, completed_at: str | None = None) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "job_type": "inference",
        "status": "failed",
        "completed_at": completed_at or datetime.now(timezone.utc).isoformat(),
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
async def test_run_once_ignores_previously_processed_jobs() -> None:
    hotkey = "hk-no-dup"
    repository = InMemoryScoreRepository()
    relay = MutableJobRelay()
    engine = _build_engine(repository, relay)

    relay.jobs_by_hotkey[hotkey] = [_success_job("job-a"), _failure_job("job-b")]

    first_summary = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(first_summary, ScoreRunSummary)

    initial_record = repository.store[hotkey]
    assert initial_record.success_count == 1
    assert initial_record.failure_count == 1

    second_summary = await engine.run_once(trace_hotkeys=[hotkey])
    assert isinstance(second_summary, ScoreRunSummary)

    repeat_record = repository.store[hotkey]
    assert repeat_record.success_count == initial_record.success_count
    assert repeat_record.failure_count == initial_record.failure_count
    assert repeat_record.sample_count == initial_record.sample_count


@pytest.mark.asyncio
async def test_run_once_processes_new_jobs_with_same_timestamp() -> None:
    hotkey = "hk-same-timestamp"
    repository = InMemoryScoreRepository()
    relay = MutableJobRelay()
    engine = _build_engine(repository, relay)

    timestamp = datetime.now(timezone.utc).isoformat()
    relay.jobs_by_hotkey[hotkey] = [
        {
            "job_id": "job-1",
            "job_type": "inference",
            "status": "success",
            "completed_at": timestamp,
            "metrics": {"latency_ms": 900},
        },
        _failure_job("job-2", completed_at=timestamp),
    ]

    await engine.run_once(trace_hotkeys=[hotkey])
    baseline = repository.store[hotkey]
    assert baseline.success_count == 1
    assert baseline.failure_count == 1

    relay.jobs_by_hotkey[hotkey].append(
        {
            "job_id": "job-3",
            "job_type": "inference",
            "status": "success",
            "completed_at": timestamp,
            "metrics": {"latency_ms": 700},
        }
    )

    await engine.run_once(trace_hotkeys=[hotkey])
    updated = repository.store[hotkey]
    assert updated.success_count == 2
    assert updated.failure_count == 1
