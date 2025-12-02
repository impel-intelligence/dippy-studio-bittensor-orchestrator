from __future__ import annotations

from copy import deepcopy

from orchestrator.runners.audit import AuditSeedRunner
from orchestrator.services.job_service import JobService


class _DummyAuditService:
    def __init__(self, job_service: JobService) -> None:
        self._job_service = job_service


def _make_runner() -> AuditSeedRunner:
    job_service = JobService(job_relay=object())
    return AuditSeedRunner(
        audit_service=_DummyAuditService(job_service),
        netuid=1,
        network="test",
        preview_only=True,
    )


def test_summarize_job_for_logging_strips_prompt_and_secrets() -> None:
    runner = _make_runner()
    job = {
        "job_id": "1234",
        "miner_hotkey": "miner",
        "callback_secret": "top-secret",
        "prompt_seed": 42,
        "payload": {
            "prompt": "visible prompt",
            "seed": 7,
            "list_prompts": ["one", "two"],
            "nested": {"prompt": "nested prompt", "keep": "value"},
            "list": [{"prompt": "remove me"}, "abc", {"keep": True}],
            "callback_secret": "inner-secret",
        },
        "response_payload": {
            "prompt": "response prompt",
            "image_url": "https://example.com/path/image.png?sig=123",
        },
    }
    original = deepcopy(job)

    summary = runner._summarize_job_for_logging(job)

    assert job == original
    assert "callback_secret" not in summary
    assert "callback_secret" not in summary["payload"]
    assert "prompt" not in summary["payload"]
    assert "list_prompts" not in summary["payload"]
    assert summary["payload"]["seed"] == 7
    assert summary["payload"]["nested"] == {"keep": "value"}
    assert summary["payload"]["list"][0] == {}
    assert summary["payload"]["list"][1] == "abc"
    assert summary["payload"]["list"][2] == {"keep": True}
    assert summary["prompt_seed"] == 42
    assert summary["response_payload"] == {"image_url": "image.png"}
