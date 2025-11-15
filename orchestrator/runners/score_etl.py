from __future__ import annotations

import logging
from typing import Any, Iterable

from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.services.score_service import ScoreRunSummary, ScoreService


class ScoreETLRunner:
    """Execute the score ETL pipeline once and report progress."""

    def __init__(
        self,
        *,
        score_service: ScoreService,
        netuid: int,
        network: str,
        trace_hotkeys: Iterable[str] | None = None,
        logger: StructuredLogger | logging.Logger | None = None,
    ) -> None:
        self._score_service = score_service
        self._netuid = netuid
        self._network = network
        self._trace_hotkeys: list[str] = list(trace_hotkeys or [])
        self._logger: StructuredLogger | logging.Logger = (
            logger if logger is not None else logging.getLogger(__name__)
        )

    async def run_once(self) -> ScoreRunSummary | None:
        self._log(
            "info",
            "score_etl.run.start",
            netuid=self._netuid,
            network=self._network,
            trace_hotkeys=self._trace_hotkeys,
        )

        try:
            summary = await self._score_service.run_once(trace_hotkeys=self._trace_hotkeys)
        except Exception as exc:  # pragma: no cover - logging safeguard
            self._log(
                "error",
                "score_etl.run.failed",
                netuid=self._netuid,
                network=self._network,
                error=str(exc),
            )
            return None

        if summary is None:
            self._log(
                "info",
                "score_etl.run.skipped",
                netuid=self._netuid,
                network=self._network,
            )
            return None

        level = "info" if summary.success else "error"
        self._log(
            level,
            "score_etl.run.complete",
            netuid=self._netuid,
            network=self._network,
            success=summary.success,
            hotkeys_considered=summary.hotkeys_considered,
            hotkeys_updated=summary.hotkeys_updated,
            zeroed_hotkeys=summary.zeroed_hotkeys,
            jobs_considered=summary.jobs_considered,
            completed_at=summary.timestamp.isoformat(),
        )

        if summary.trace_details and self._should_log_info():
            self._log_trace_details(summary.trace_details)
        return summary

    def _should_log_info(self) -> bool:
        logger = self._logger
        if isinstance(logger, StructuredLogger):
            return logger._logger.isEnabledFor(logging.INFO)  # type: ignore[attr-defined]
        return logger.isEnabledFor(logging.INFO)

    def _log_trace_details(self, details: dict[str, Any]) -> None:
        for hotkey, payload in details.items():
            self._log(
                "info",
                "score_etl.trace.hotkey",
                hotkey=hotkey,
                jobs_fetched=payload.get("jobs_fetched"),
                job_statuses=payload.get("job_statuses"),
                score=payload.get("score"),
                considered=payload.get("considered"),
            )

    def _log(self, level: str, event: str, **fields: Any) -> None:
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
