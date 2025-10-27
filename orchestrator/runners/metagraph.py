from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.common.model_utils import dump_model
from orchestrator.common.structured_logging import StructuredLogger


StateResult = Tuple[dict[str, Miner], Optional[int]]


class MetagraphStateRunner:
    """Fetch, validate, and persist a single snapshot of the metagraph state."""

    def __init__(
        self,
        *,
        miner_metagraph_client: LiveMinerMetagraphClient,
        netuid: int,
        network: str,
        subnet_fetcher: Callable[[int, str], Optional[StateResult]],
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        self._miner_metagraph_client = miner_metagraph_client
        self._netuid = netuid
        self._network = network
        self._fetch_subnet_state = subnet_fetcher
        self._logger: StructuredLogger | logging.Logger = (
            logger if logger is not None else logging.getLogger(__name__)
        )

    async def run_once(self) -> None:
        """Execute one metagraph refresh cycle."""
        self._log(
            "info",
            "metagraph.sync.start",
            netuid=self._netuid,
            network=self._network,
        )

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
            return

        if state_result is None:
            self._log(
                "warning",
                "metagraph.sync.empty",
                netuid=self._netuid,
                network=self._network,
            )
            return

        state, block = state_result

        try:
            validated_state = self._miner_metagraph_client.validate_state(state)
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "error",
                "metagraph.sync.validate_failed",
                block=block,
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return

        try:
            fetched_at = datetime.now(timezone.utc)
            self._miner_metagraph_client.update_state(
                validated_state,
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
            return

        self._log(
            "info",
            "metagraph.sync.complete",
            block=block,
            miner_count=len(validated_state),
            netuid=self._netuid,
            network=self._network,
            client_instance_id=getattr(self._miner_metagraph_client, "instance_id", None),
            client_db_path=getattr(self._miner_metagraph_client, "db_path", None),
        )

        if self._should_log_debug():
            self._log_debug_state(block=block)

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
            client_instance_id=getattr(self._miner_metagraph_client, "instance_id", None),
            client_db_path=getattr(self._miner_metagraph_client, "db_path", None),
            miner_state=serialized_state,
        )

    def _log(self, level: str, event: str, **fields: Any) -> None:
        """Emit log entries using either structured or stdlib loggers."""
        logger = self._logger
        if isinstance(logger, StructuredLogger):
            log_method = getattr(logger, level)
            log_method(event, **fields)
            return

        log_method = getattr(logger, level)
        if fields:
            log_method("%s %s", event, fields)
        else:
            log_method(event)
