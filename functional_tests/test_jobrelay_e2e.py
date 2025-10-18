from __future__ import annotations

import os
import time
import uuid
from typing import Callable, Dict, Optional, Tuple

import httpx
import pytest


JOBRELAY_AUTH_HEADER = "X-Service-Auth-Secret"


def _jobrelay_settings() -> Tuple[str, Dict[str, str]]:
    base_url = os.getenv("JOBRELAY_BASE_URL", "http://localhost:8081").rstrip("/")
    token = os.getenv("JOBRELAY_E2E_AUTH_TOKEN") or os.getenv("JOBRELAY_AUTH_TOKEN")
    header_name = JOBRELAY_AUTH_HEADER
    if not token:
        pytest.skip("Set JOBRELAY_AUTH_TOKEN or JOBRELAY_E2E_AUTH_TOKEN for jobrelay tests")
    return base_url, {header_name: token}


@pytest.fixture(scope="module")
def jobrelay_client() -> Tuple[httpx.Client, Dict[str, str]]:
    base_url, headers = _jobrelay_settings()
    client = httpx.Client(base_url=base_url, timeout=10.0)
    try:
        yield client, headers
    finally:
        client.close()


def _wait_for_job(
    client: httpx.Client,
    headers: Dict[str, str],
    job_id: str,
    retries: int = 10,
    delay_seconds: float = 0.5,
    predicate: Optional[Callable[[Dict[str, object]], bool]] = None,
) -> Dict[str, object]:
    last_payload: Optional[Dict[str, object]] = None
    for _ in range(retries):
        response = client.get(f"/jobs/{job_id}", headers=headers)
        if response.status_code == 200:
            payload = response.json()
            last_payload = payload
            if predicate is None or predicate(payload):
                return payload
        time.sleep(delay_seconds)
    if last_payload is None:
        pytest.fail(f"Job {job_id} not available after {retries} attempts")
    pytest.fail(
        f"Job {job_id} available but predicate not satisfied after {retries} attempts; "
        f"last payload: {last_payload}"
    )


@pytest.mark.functional
def test_jobrelay_end_to_end(jobrelay_client: Tuple[httpx.Client, Dict[str, str]]) -> None:
    client, headers = jobrelay_client
    job_id = str(uuid.uuid4())
    miner_hotkey = "jobrelay-e2e-hotkey"
    payload = {
        "job_type": "inference",
        "miner_hotkey": miner_hotkey,
        "payload": {"input": "value"},
        "status": "pending",
        "audit_status": "not_audited",
        "verification_status": "nonverified",
        "is_audit_job": False,
    }

    create_response = client.post(f"/jobs/{job_id}", json=payload, headers=headers)
    assert create_response.status_code == 200, create_response.text

    job = _wait_for_job(client, headers, job_id)
    assert job["job_id"] == job_id
    assert job["payload"] == {"input": "value"}
    assert job["status"] == "pending"

    update_response = client.patch(
        f"/jobs/{job_id}",
        json={"status": "success", "execution_duration_ms": 1234},
        headers=headers,
    )
    assert update_response.status_code == 200, update_response.text

    updated_job = _wait_for_job(
        client,
        headers,
        job_id,
        predicate=lambda job: job.get("status") == "success"
        and job.get("execution_duration_ms") == 1234,
    )
    assert updated_job["status"] == "success"
    assert updated_job["execution_duration_ms"] == 1234

    list_response = client.get("/jobs", headers=headers)
    assert list_response.status_code == 200, list_response.text
    jobs_payload = list_response.json().get("jobs", [])
    assert any(item["job_id"] == job_id for item in jobs_payload)

    hotkey_response = client.get(f"/hotkeys/{miner_hotkey}/jobs", headers=headers)
    assert hotkey_response.status_code == 200, hotkey_response.text
    jobs = hotkey_response.json().get("jobs", [])
    assert any(item["job_id"] == job_id for item in jobs)

    unauthorized = client.get(f"/jobs/{job_id}")
    assert unauthorized.status_code == 401

    missing_job_id = str(uuid.uuid4())
    not_found = client.get(f"/jobs/{missing_job_id}", headers=headers)
    assert not_found.status_code == 404
