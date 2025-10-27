from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pytest

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.services.database_service import DatabaseService
from orchestrator.services.job_scoring import job_to_score
from orchestrator.services.score_service import ScoreRecord, ScoreService


INTEGRATION_DSN = os.getenv(
    "INTEGRATION_DATABASE_URL",
    "postgresql://orchestrator:orchestrator@localhost:15432/orchestrator_test?sslmode=disable",
)


def _wipe_miners(service: DatabaseService) -> None:
    with service.cursor() as cur:
        cur.execute("DELETE FROM miners")


def _decode_scores_payload(service: DatabaseService, hotkey: str) -> Dict[str, Any]:
    with service.cursor() as cur:
        cur.execute("SELECT scores FROM miners WHERE hotkey = %s", (hotkey,))
        row = cur.fetchone()
    assert row is not None, f"expected scores payload for {hotkey}"
    payload = row[0]
    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload = bytes(payload).decode()
    if isinstance(payload, str):
        payload = json.loads(payload)
    return dict(payload)


@pytest.fixture(scope="module")
def database_service() -> DatabaseService:
    service = DatabaseService(INTEGRATION_DSN)
    try:
        yield service
    finally:
        service.close()


@pytest.fixture(autouse=True)
def cleanup_miners(database_service: DatabaseService) -> None:
    _wipe_miners(database_service)
    yield
    _wipe_miners(database_service)


@pytest.mark.integration
def test_scores_survive_metagraph_updates(database_service: DatabaseService) -> None:
    score_service = ScoreService(database_service)
    hotkey = "hk-integration"

    score_service.put(hotkey, ScoreRecord(scores=5.5, is_slashed=False))
    stored = score_service.get(hotkey)
    assert stored is not None
    assert stored.scores == pytest.approx(5.5)

    metagraph_client = LiveMinerMetagraphClient(database_service)
    metagraph_client.update_state(
        {
            hotkey: Miner(
                uid=9,
                network_address="http://integration.test",
                valid=True,
                alpha_stake=123,
                capacity={},
                hotkey=hotkey,
            )
        }
    )

    refreshed = score_service.get(hotkey)
    assert refreshed is not None
    assert refreshed.scores == pytest.approx(5.5)

    payload = _decode_scores_payload(database_service, hotkey)
    assert payload["scores"] == pytest.approx(5.5)
    assert payload["is_slashed"] is False


@pytest.mark.integration
def test_jobs_to_score_persists_ema_fields(database_service: DatabaseService) -> None:
    score_service = ScoreService(database_service)
    hotkey = "hk-ema"
    now = datetime.now(timezone.utc)
    jobs = [
        {
            "job_id": "job-success",
            "status": "success",
            "job_type": "inference",
            "is_audit_job": False,
            "completed_at": (now - timedelta(seconds=5)).isoformat(),
            "metrics": {"latency_ms": 2_000},
        }
    ]

    score_records = score_service.jobs_to_score({hotkey: jobs}, reference_time=now)
    assert hotkey in score_records

    expected_payload = score_records[hotkey].model_dump()
    assert score_service.update_scores(score_records)

    stored = score_service.get(hotkey)
    assert stored is not None
    payload = stored.model_dump()

    assert payload["ema_score"] == pytest.approx(expected_payload["ema_score"], rel=1e-5)
    assert payload["scores"] == pytest.approx(expected_payload["scores"], rel=1e-5)
    assert payload["success_count"] == 1
    assert payload["failure_count"] == 0
    assert payload["sample_count"] == 1
    assert payload.get("last_sample_at") is not None
    assert payload.get("ema_last_update_at") is not None

    db_payload = _decode_scores_payload(database_service, hotkey)
    assert db_payload["ema_score"] == pytest.approx(expected_payload["ema_score"], rel=1e-5)
    assert db_payload["scores"] == pytest.approx(expected_payload["scores"], rel=1e-5)
    assert db_payload["success_count"] == 1
    assert db_payload["failure_count"] == 0
    assert db_payload["sample_count"] == 1
    assert db_payload["ema_alpha"] == pytest.approx(1.0)
    assert db_payload["failure_penalty_weight"] == pytest.approx(0.2)
    assert db_payload["decay_half_life_seconds"] == pytest.approx(604_800.0)

    for key in ("last_sample_at", "ema_last_update_at"):
        timestamp = db_payload.get(key)
        assert isinstance(timestamp, str)
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None


@pytest.mark.integration
def test_failure_penalty_with_existing_record(database_service: DatabaseService) -> None:
    score_service = ScoreService(database_service)
    hotkey = "hk-penalty"
    base_time = datetime.now(timezone.utc) - timedelta(minutes=2)

    success_job = {
        "job_id": "success-1",
        "status": "success",
        "job_type": "inference",
        "is_audit_job": False,
        "completed_at": (base_time + timedelta(seconds=30)).isoformat(),
        "metrics": {"latency_ms": 1_000},
    }

    first_records = score_service.jobs_to_score(
        {hotkey: [success_job]},
        reference_time=base_time + timedelta(minutes=1),
    )
    assert score_service.update_scores(first_records)
    existing_record = score_service.get(hotkey)
    assert existing_record is not None

    failure_job = {
        "job_id": "failure-1",
        "status": "failed",
        "job_type": "inference",
        "is_audit_job": False,
        "completed_at": (base_time + timedelta(minutes=1, seconds=30)).isoformat(),
    }

    updated_records = score_service.jobs_to_score(
        {hotkey: [failure_job]},
        reference_time=base_time + timedelta(minutes=2),
        existing_records={hotkey: existing_record},
    )
    assert score_service.update_scores(updated_records)

    stored = score_service.get(hotkey)
    assert stored is not None
    payload = stored.model_dump()

    assert payload["success_count"] == 1
    assert payload["failure_count"] == 1
    expected_penalty = max(payload["success_count"] - payload["failure_penalty_weight"] * payload["failure_count"], 0)
    assert payload["legacy_score"] == pytest.approx(expected_penalty)
    assert payload["scores"] == pytest.approx(min(payload["ema_score"], expected_penalty))
    assert payload["scores"] < payload["ema_score"]

    db_payload = _decode_scores_payload(database_service, hotkey)
    assert db_payload["failure_count"] == 1
    assert db_payload["success_count"] == 1
    assert db_payload["sample_count"] == 1
    assert db_payload["legacy_score"] == pytest.approx(expected_penalty)
    assert db_payload["scores"] == pytest.approx(payload["scores"])
    assert db_payload.get("last_job_id") == "failure-1"
    assert db_payload.get("ema_last_update_at") is not None


@pytest.mark.integration
def test_last_update_recovers_from_persisted_state(database_service: DatabaseService) -> None:
    score_service = ScoreService(database_service)
    hotkey = "hk-last-update"
    event_time = datetime.now(timezone.utc)

    record = ScoreRecord(
        scores=1.0,
        is_slashed=False,
        ema_last_update_at=event_time.isoformat(),
    )
    assert score_service.update_scores({hotkey: record})

    reloaded_service = ScoreService(database_service)
    restored = reloaded_service.last_update()

    assert restored is not None
    assert abs((restored - event_time).total_seconds()) < 5.0
