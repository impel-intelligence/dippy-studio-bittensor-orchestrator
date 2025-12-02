from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from orchestrator.clients.database import PostgresClient
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.domain.miner import Miner
from orchestrator.repositories import MinerRepository
from orchestrator.services.miner_health_service import MinerHealthService
from orchestrator.services.miner_selection_service import MinerSelectionService
from sn_uuid import uuid7


PLACEHOLDER_MINER = Miner(
    uid=74,
    valid=True,
    network_address="https://tmp-test.dippy-bittensor-subnet.com",
    alpha_stake=100000,
    capacity={},
    hotkey="5EtM9iXMAYRsmt6aoQAoWNDX6yaBnjhmnEQhWKv8HpwkVtML",
    failed_audits=0,
    failure_count=0,
)


class MinerMetagraphService:
    """High-level façade for miner persistence, validation, and selection."""

    def __init__(
        self,
        database_service: PostgresClient,
        epistula_client: Optional[EpistulaClient] = None,
        *,
        repository: MinerRepository | None = None,
        health_service: MinerHealthService | None = None,
        selection_service: MinerSelectionService | None = None,
    ) -> None:
        self._instance_id = f"metagraph-{uuid7().hex}"
        self._database_service = database_service
        self._repository = repository or MinerRepository(database_service)
        self._health_service = health_service or MinerHealthService(
            self._repository,
            epistula_client=epistula_client,
        )
        self._selection_service = selection_service or MinerSelectionService(self._repository)
        self._last_update: Optional[datetime] = None
        self._last_block: Optional[int] = None
        self._logger = logging.getLogger(__name__)

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def db_path(self) -> str:
        return self._database_service.safe_dsn

    def validate_state(self, state: Dict[str, Miner]) -> Dict[str, Miner]:
        return self._health_service.validate_state(state)

    def update_state(
        self,
        state: Dict[str, Miner],
        *,
        block: Optional[int] = None,
        fetched_at: Optional[datetime] = None,
    ) -> None:
        self._repository.sync_state(state)
        self._last_update = fetched_at or datetime.now(timezone.utc)
        self._last_block = block

    def dump_state(self) -> Dict[str, Miner]:
        return self._repository.dump_state()

    def dump_filtered_state(self) -> Dict[str, Miner]:
        return self._repository.dump_filtered_state()

    def dump_full_state(self) -> Dict[str, Miner]:
        return self.dump_state()

    def last_update(self) -> Optional[datetime]:
        return self._last_update

    def last_block(self) -> Optional[int]:
        return self._last_block

    def fetch_miners(self) -> Dict[str, Miner]:
        return self._repository.fetch_miners(valid_only=True)

    def fetch_candidate(self, task_type: str | None = None) -> Optional[Miner]:
        return self._selection_service.fetch_candidate(task_type=task_type)

    def get_last_candidates(self) -> list[tuple[datetime, Miner]]:
        return self._selection_service.get_last_candidates()

    def upsert_miner(self, miner: Miner | Dict[str, object]) -> Miner:
        record = self._repository.upsert_miner(miner)
        self._last_update = datetime.now(timezone.utc)
        return record

    def get_miner(self, hotkey: str) -> Optional[Miner]:
        return self._repository.get_miner(hotkey)

    def delete_miner(self, hotkey: str) -> bool:
        deleted = self._repository.delete_miner(hotkey)
        if deleted:
            self._last_update = datetime.now(timezone.utc)
        return deleted

    def record_request_failure(self, hotkey: str, *, increment: int = 1) -> None:
        try:
            self._repository.increment_failure_count(hotkey, increment=increment)
        except Exception as exc:  # pragma: no cover - defensive guard
            self._logger.debug(
                "miner_metagraph.record_request_failure.failed",
                exc_info=exc,
                extra={"hotkey": hotkey},
            )


__all__ = ["MinerMetagraphService", "PLACEHOLDER_MINER"]
