"""Run orchestrator background tasks on demand."""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Sequence

from orchestrator.runners.metagraph import MetagraphStateRunner
from orchestrator.runners.score_etl import ScoreETLRunner
from orchestrator.server import Orchestrator

RunnerTarget = str
WORKER_LOGGER = logging.getLogger("orchestrator.workers")


async def _run_metagraph(orchestrator: Orchestrator) -> None:
    WORKER_LOGGER.info(
        "worker.metagraph.start netuid=%s network=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
    )
    runner = MetagraphStateRunner(
        miner_metagraph_client=orchestrator.miner_metagraph_client,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        subnet_fetcher=orchestrator.subnet_state_service.fetch_state,
        logger=orchestrator.server_context.logger,
    )
    await runner.run_once()
    WORKER_LOGGER.info(
        "worker.metagraph.complete netuid=%s network=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
    )


async def _run_score(orchestrator: Orchestrator, trace_hotkeys: Sequence[str] | None = None) -> None:
    WORKER_LOGGER.info(
        "worker.score.start netuid=%s network=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
    )
    runner = ScoreETLRunner(
        score_service=orchestrator.score_service,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        trace_hotkeys=list(trace_hotkeys or ()),
        logger=orchestrator.server_context.logger,
    )
    summary = await runner.run_once()
    if summary is None:
        WORKER_LOGGER.info(
            "worker.score.complete netuid=%s network=%s result=skipped",
            orchestrator.config.subnet.netuid,
            orchestrator.config.subnet.network,
        )
        return

    WORKER_LOGGER.info(
        "worker.score.complete netuid=%s network=%s success=%s hotkeys=%s zeroed=%s jobs=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        summary.success,
        summary.hotkeys_considered,
        summary.zeroed_hotkeys,
        summary.jobs_considered,
    )


async def _run_targets(
    orchestrator: Orchestrator,
    targets: Sequence[RunnerTarget],
    *,
    trace_hotkeys: Sequence[str] | None = None,
) -> None:
    for target in targets:
        normalized = target.strip().lower()
        if normalized == "metagraph":
            await _run_metagraph(orchestrator)
        elif normalized == "score":
            await _run_score(orchestrator, trace_hotkeys=trace_hotkeys)
        else:
            raise ValueError(f"Unknown runner target: {target}")


def run_targets(
    targets: Iterable[RunnerTarget],
    *,
    config_path: str | None = None,
    database_url: str | None = None,
    trace_hotkeys: Sequence[str] | None = None,
) -> None:
    sequence = list(targets)
    if not sequence:
        raise ValueError("At least one runner target must be provided")

    WORKER_LOGGER.info("worker.sequence.start targets=%s", ",".join(sequence))

    orchestrator = Orchestrator(config_path=config_path, database_url=database_url)
    try:
        asyncio.run(
            _run_targets(
                orchestrator,
                sequence,
                trace_hotkeys=trace_hotkeys,
            )
        )
    finally:
        try:
            orchestrator.database_service.close()
        except Exception:
            pass

    WORKER_LOGGER.info("worker.sequence.complete targets=%s", ",".join(sequence))


__all__ = ["run_targets", "RunnerTarget"]
