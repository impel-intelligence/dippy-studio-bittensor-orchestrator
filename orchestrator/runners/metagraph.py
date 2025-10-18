from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.common.structured_logging import StructuredLogger


StateResult = Tuple[dict[str, Miner], Optional[int]]


class MetagraphStateRunner:
    """Periodically refresh the metagraph state using a background fetcher."""

    def __init__(
        self,
        *,
        miner_metagraph_client: LiveMinerMetagraphClient,
        netuid: int,
        network: str,
        interval_seconds: float = 300.0,
        logger: StructuredLogger | logging.Logger | None = None,
        subnet_fetcher: Optional[
            Callable[[int, str], Optional[StateResult]]
        ] = None,
    ) -> None:
        self._miner_metagraph_client = miner_metagraph_client
        self._netuid = netuid
        self._network = network
        self._interval_seconds = interval_seconds
        self._fetch_subnet_state = subnet_fetcher
        self._logger: StructuredLogger | logging.Logger = (
            logger if logger is not None else logging.getLogger(__name__)
        )
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: Optional[asyncio.Event] = None

    async def start(self) -> None:
        """Begin periodic refresh loop."""
        if self._task is not None:
            return

        if self._fetch_subnet_state is None:
            self._log(
                "debug",
                "metagraph.sync.skipped_no_fetcher",
                netuid=self._netuid,
                network=self._network,
            )
            return

        self._stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        self._task = loop.create_task(self._run())

    async def stop(self) -> None:
        """Stop the refresh loop if running."""
        if self._task is None or self._stop_event is None:
            return

        self._stop_event.set()
        try:
            await self._task
        finally:
            self._task = None
            self._stop_event = None

    async def run_once(self) -> None:
        """Execute a single refresh cycle immediately."""
        await self._sync()

    async def _run(self) -> None:
        assert self._stop_event is not None

        while not self._stop_event.is_set():
            await self._sync()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._interval_seconds
                )
            except asyncio.TimeoutError:
                continue

    async def _sync(self) -> None:
        if self._fetch_subnet_state is None:
            self._log(
                "debug",
                "metagraph.sync.skipped_no_fetcher",
                netuid=self._netuid,
                network=self._network,
            )
            return

        loop = asyncio.get_running_loop()

        self._log(
            "info",
            "metagraph.sync.start",
            netuid=self._netuid,
            network=self._network,
        )

        try:
            state_result: Optional[StateResult] = await loop.run_in_executor(
                None,
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

        persisted_state = self._miner_metagraph_client.dump_full_state()
        serialized_state: dict[str, Any] = {}
        for hotkey, miner in persisted_state.items():
            if hasattr(miner, "model_dump"):
                serialized_state[hotkey] = miner.model_dump()
            elif hasattr(miner, "dict"):
                serialized_state[hotkey] = miner.dict()
            else:
                serialized_state[hotkey] = miner

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
