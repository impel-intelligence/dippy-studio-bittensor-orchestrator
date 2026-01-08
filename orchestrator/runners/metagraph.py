from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING

from orchestrator.domain.miner import Miner
from orchestrator.common.model_utils import dump_model
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.runners.base import InstrumentedRunner
from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.clients.ss58_client import SS58Client

if TYPE_CHECKING:
    from orchestrator.clients.subnet_state_client import SubnetStateClient


StateResult = Tuple[dict[str, Miner], Optional[int]]


class MetagraphStateRunner(InstrumentedRunner[dict[str, Any]]):
    """Fetch, validate, and persist a single snapshot of the metagraph state."""

    def __init__(
        self,
        *,
        miner_metagraph_client: MinerMetagraphService,
        netuid: int,
        network: str,
        subnet_fetcher: Callable[[int, str], Optional[StateResult]],
        subnet_state_client: "SubnetStateClient | None" = None,
        ss58_client: SS58Client | None = None,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        self._miner_metagraph_client = miner_metagraph_client
        self._netuid = netuid
        self._network = network
        self._fetch_subnet_state = subnet_fetcher
        self._subnet_state_client = subnet_state_client
        self._ss58_client = ss58_client
        super().__init__(name="metagraph.sync", logger=logger)

    async def _run(self) -> dict[str, Any] | None:
        """Execute one metagraph refresh cycle."""
        try:
            state_result = await asyncio.to_thread(
                self._fetch_subnet_state,
                self._netuid,
                self._network,
            )
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "warning",
                "metagraph.sync.fetch_failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        if state_result is None:
            self._log(
                "warning",
                "metagraph.sync.empty",
                netuid=self._netuid,
                network=self._network,
            )
            return None

        state, block = state_result

        filtered_state, banned_filtered = await self._filter_banned_hotkeys(state)

        try:
            validated_state = self._miner_metagraph_client.validate_state(
                filtered_state
            )
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "error",
                "metagraph.sync.validate_failed",
                block=block,
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        enriched_state = validated_state
        try:
            enriched_state = self._populate_alpha_stake(validated_state)
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "warning",
                "metagraph.sync.populate_alpha_failed",
                block=block,
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            enriched_state = validated_state

        try:
            fetched_at = datetime.now(timezone.utc)
            self._miner_metagraph_client.update_state(
                enriched_state,
                block=block,
                fetched_at=fetched_at,
            )
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "error",
                "metagraph.sync.update_failed",
                block=block,
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        if self._should_log_debug():
            self._log_debug_state(block=block)
        return {
            "block": block,
            "miner_count": len(validated_state),
            "banned_filtered": banned_filtered,
            "client_instance_id": getattr(
                self._miner_metagraph_client, "instance_id", None
            ),
            "client_db_path": getattr(self._miner_metagraph_client, "db_path", None),
        }

    async def _filter_banned_hotkeys(
        self, state: Dict[str, Miner]
    ) -> tuple[Dict[str, Miner], int]:
        if not state or self._ss58_client is None:
            return state, 0

        try:
            banned_hotkeys = await self._ss58_client.list_addresses()
        except Exception as exc:  # pragma: no cover - defensive guard
            self._log(
                "warning",
                "metagraph.sync.banned_fetch_failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return state, 0

        if not banned_hotkeys:
            return state, 0

        filtered = {
            hotkey: miner
            for hotkey, miner in state.items()
            if hotkey not in banned_hotkeys
        }
        removed = len(state) - len(filtered)

        if removed:
            self._log(
                "info",
                "metagraph.sync.banned_filtered",
                netuid=self._netuid,
                network=self._network,
                removed=removed,
                banned_count=len(banned_hotkeys),
            )

        return filtered, removed

    def _populate_alpha_stake(self, state: Dict[str, Miner]) -> Dict[str, Miner]:
        if not state:
            return state
        if self._subnet_state_client is None:
            return state
        return self._subnet_state_client.populate_alpha_stake(
            state,
            netuid=self._netuid,
            network=self._network,
        )

    def _start_fields(self) -> Dict[str, Any]:
        return {"netuid": self._netuid, "network": self._network}

    def _complete_fields(
        self, result: dict[str, Any] | None, start_fields: dict[str, Any]
    ) -> dict[str, Any]:
        if result is None:
            return {**start_fields, "status": "skipped"}
        return {**start_fields, "status": "success", **result}

    def _should_log_debug(self) -> bool:
        logger = self._logger
        if isinstance(logger, StructuredLogger):
            # StructuredLogger wraps a stdlib logger in `_logger`.
            return logger._logger.isEnabledFor(logging.DEBUG)  # type: ignore[attr-defined]
        return logger.isEnabledFor(logging.DEBUG)

    def _log_debug_state(self, *, block: Optional[int]) -> None:
        persisted_state = self._miner_metagraph_client.dump_full_state()
        serialized_state: dict[str, Any] = {}
        for hotkey, miner in persisted_state.items():
            serialized_state[hotkey] = dump_model(miner)

        self._log(
            "debug",
            "metagraph.sync.state",
            block=block,
            netuid=self._netuid,
            network=self._network,
            client_instance_id=getattr(
                self._miner_metagraph_client, "instance_id", None
            ),
            client_db_path=getattr(self._miner_metagraph_client, "db_path", None),
            miner_state=serialized_state,
        )
