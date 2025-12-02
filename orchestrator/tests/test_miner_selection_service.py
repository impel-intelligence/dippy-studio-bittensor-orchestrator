from __future__ import annotations

from types import SimpleNamespace

from orchestrator.services.miner_selection_service import MinerSelectionService


class _StubRepository:
    pass


def test_failure_penalty_reduces_multiplier() -> None:
    service = MinerSelectionService(repository=_StubRepository())
    miner_ok = SimpleNamespace(failure_count=0, failed_audits=0)
    miner_fail = SimpleNamespace(failure_count=3, failed_audits=0)

    ok_multiplier = service._score_multiplier(miner_ok, payload=None)
    penalized_multiplier = service._score_multiplier(miner_fail, payload=None)

    assert ok_multiplier == 1.0
    assert penalized_multiplier < ok_multiplier
    assert penalized_multiplier == (1.0 / (1.0 + 3))
