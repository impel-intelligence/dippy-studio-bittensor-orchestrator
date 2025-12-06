"""Run orchestrator background tasks on demand."""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable, Sequence

from orchestrator.runners.audit import AuditCheckRunner, AuditSeedRunner
from orchestrator.runners.metagraph import MetagraphStateRunner
from orchestrator.runners.score_etl import ScoreETLRunner
from orchestrator.runners.seed_requests import SeedRequestsRunner
from orchestrator.services.listen_service import ListenService
from orchestrator.server import Orchestrator, _configure_opentelemetry

RunnerTarget = str
WORKER_LOGGER = logging.getLogger("orchestrator.workers")


async def _run_metagraph(orchestrator: Orchestrator) -> None:
    WORKER_LOGGER.info(
        "worker.metagraph.start netuid=%s network=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
    )
    runner = MetagraphStateRunner(
        miner_metagraph_client=orchestrator.miner_metagraph_service,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        subnet_fetcher=orchestrator.subnet_state_service.fetch_state,
        subnet_state_client=orchestrator.subnet_state_service,
        logger=orchestrator.server_context.logger,
    )
    await runner.execute()
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
    summary = await runner.execute()
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

async def _run_audit_seed(
    orchestrator: Orchestrator,
    *,
    preview_only: bool = False,
    job_type: str | None = None,
) -> None:
    WORKER_LOGGER.info(
        "worker.audit_seed.start netuid=%s network=%s job_type=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        job_type or "img-h100_pcie",
    )
    seed_cfg = getattr(orchestrator.config, "audit_seed", None)
    redis_url = None
    redis_namespace = "audit_seed"
    dispatch_delay = 15.0
    audit_limit = 10
    if seed_cfg is not None:
        redis_url = seed_cfg.redis_url or orchestrator.config.listen.sync.redis_url
        redis_namespace = seed_cfg.redis_namespace
        dispatch_delay = seed_cfg.dispatch_delay_seconds
        audit_limit = seed_cfg.limit
    else:  # pragma: no cover - defensive fallback for older configs
        redis_url = orchestrator.config.listen.sync.redis_url

    runner = AuditSeedRunner(
        audit_service=orchestrator.audit_service,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        callback_url=orchestrator.config.callback.resolved_callback_url(),
        audit_miner=orchestrator.audit_miner,
        preview_only=preview_only,
        job_type=job_type,
        limit=audit_limit,
        dispatch_delay_seconds=dispatch_delay,
        redis_url=redis_url,
        redis_namespace=redis_namespace,
        logger=orchestrator.server_context.logger,
    )
    summary = await runner.execute()
    if summary is None:
        WORKER_LOGGER.info(
            "worker.audit_seed.complete netuid=%s network=%s result=skipped",
            orchestrator.config.subnet.netuid,
            orchestrator.config.subnet.network,
        )
        return

    WORKER_LOGGER.info(
        "worker.audit_seed.complete netuid=%s network=%s jobs=%s candidates=%s valid=%s invalid=%s applied=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        summary.jobs_examined,
        summary.audit_candidates,
        summary.miners_marked_valid,
        summary.miners_marked_invalid,
        summary.applied_changes,
    )


async def _run_audit_check(orchestrator: Orchestrator, *, apply_changes: bool = False) -> None:
    WORKER_LOGGER.info(
        "worker.audit_check.start netuid=%s network=%s apply_changes=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        apply_changes,
    )
    runner = AuditCheckRunner(
        audit_service=orchestrator.audit_service,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        apply_changes=apply_changes,
        audit_failure_repository=orchestrator.audit_failure_repository,
        logger=orchestrator.server_context.logger,
    )
    summary = await runner.execute()
    if summary is None:
        WORKER_LOGGER.info(
            "worker.audit_check.complete netuid=%s network=%s result=skipped",
            orchestrator.config.subnet.netuid,
            orchestrator.config.subnet.network,
        )
        return

    WORKER_LOGGER.info(
        "worker.audit_check.complete netuid=%s network=%s jobs=%s candidates=%s valid=%s invalid=%s applied=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        summary.jobs_examined,
        summary.audit_candidates,
        summary.miners_marked_valid,
        summary.miners_marked_invalid,
        summary.applied_changes,
    )


async def _run_seed_requests(orchestrator: Orchestrator) -> None:
    WORKER_LOGGER.info(
        "worker.seed_requests.start netuid=%s network=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
    )

    listen_service = ListenService(
        job_service=orchestrator.job_service,
        metagraph=orchestrator.miner_metagraph_service,
        logger=orchestrator.server_context.logger,
        callback_url=orchestrator.config.callback.resolved_callback_url(),
        epistula_client=orchestrator.epistula_client,
    )

    runner = SeedRequestsRunner(
        job_service=orchestrator.job_service,
        miner_metagraph_service=orchestrator.miner_metagraph_service,
        listen_service=listen_service,
        netuid=orchestrator.config.subnet.netuid,
        network=orchestrator.config.subnet.network,
        logger=orchestrator.server_context.logger,
    )
    summary = await runner.execute()
    if summary is None:
        WORKER_LOGGER.info(
            "worker.seed_requests.complete netuid=%s network=%s result=skipped",
            orchestrator.config.subnet.netuid,
            orchestrator.config.subnet.network,
        )
        return

    WORKER_LOGGER.info(
        "worker.seed_requests.complete netuid=%s network=%s source_job=%s targets=%s dispatched=%s",
        orchestrator.config.subnet.netuid,
        orchestrator.config.subnet.network,
        summary.source_job_id,
        summary.target_miners,
        len(summary.dispatched_job_ids),
    )


async def _run_targets(
    orchestrator: Orchestrator,
    targets: Sequence[RunnerTarget],
    *,
    trace_hotkeys: Sequence[str] | None = None,
    audit_apply_changes: bool = False,
    audit_seed_preview: bool = False,
    audit_seed_job_type: str | None = None,
) -> None:
    for target in targets:
        normalized = target.strip().lower()
        if normalized == "metagraph":
            await _run_metagraph(orchestrator)
        elif normalized == "score":
            await _run_score(orchestrator, trace_hotkeys=trace_hotkeys)
        elif normalized == "audit-seed":
            await _run_audit_seed(
                orchestrator,
                preview_only=audit_seed_preview,
                job_type=audit_seed_job_type,
            )
        elif normalized == "audit-check":
            await _run_audit_check(orchestrator, apply_changes=audit_apply_changes)
        elif normalized == "audit":
            await _run_audit_check(orchestrator, apply_changes=audit_apply_changes)
        elif normalized == "seed-requests":
            await _run_seed_requests(orchestrator)
        else:
            raise ValueError(f"Unknown runner target: {target}")


def run_targets(
    targets: Iterable[RunnerTarget],
    *,
    config_path: str | None = None,
    database_url: str | None = None,
    trace_hotkeys: Sequence[str] | None = None,
    audit_apply_changes: bool = False,
    audit_seed_preview: bool = False,
    audit_seed_job_type: str | None = None,
) -> None:
    sequence = list(targets)
    if not sequence:
        raise ValueError("At least one runner target must be provided")

    orchestrator = Orchestrator(config_path=config_path, database_url=database_url)

    # Ensure OTEL logging/tracing is active for CLI workers so logs land in SigNoz with trace context.
    try:
        _configure_opentelemetry(orchestrator.app)
    except Exception:  # noqa: BLE001
        WORKER_LOGGER.exception("worker.otel_setup_failed")

    WORKER_LOGGER.info("worker.sequence.start targets=%s", ",".join(sequence))
    try:
        asyncio.run(
            _run_targets(
                orchestrator,
                sequence,
                trace_hotkeys=trace_hotkeys,
                audit_apply_changes=audit_apply_changes,
                audit_seed_preview=audit_seed_preview,
                audit_seed_job_type=audit_seed_job_type,
            )
        )
    finally:
        try:
            orchestrator.database_service.close()
        except Exception:
            pass

    WORKER_LOGGER.info("worker.sequence.complete targets=%s", ",".join(sequence))


__all__ = ["run_targets", "RunnerTarget"]
