from __future__ import annotations

import logging
from typing import Any, Iterable

from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.runners.base import InstrumentedRunner
from orchestrator.services.score_service import ScoreRunSummary, ScoreService


class ScoreETLRunner(InstrumentedRunner[ScoreRunSummary]):
    """Execute the score ETL pipeline and report progress."""

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
        super().__init__(name="score_etl.run", logger=logger)

    async def _run(self) -> ScoreRunSummary | None:
        summary = await self._score_service.run_once(trace_hotkeys=self._trace_hotkeys)
        if summary is None:
            self._log(
                "info",
                "score_etl.run.skipped",
                netuid=self._netuid,
                network=self._network,
            )
            return None

        if summary.trace_details and self._should_log_info():
            self._log_trace_details(summary.trace_details)
        return summary

    def _start_fields(self) -> dict[str, Any]:
        return {
            "netuid": self._netuid,
            "network": self._network,
            "trace_hotkeys": self._trace_hotkeys,
        }

    def _complete_fields(
        self,
        summary: ScoreRunSummary | None,
        start_fields: dict[str, Any],
    ) -> dict[str, Any]:
        if summary is None:
            return {**start_fields, "status": "skipped"}

        return {
            **start_fields,
            "status": "success" if summary.success else "failed",
            "success": summary.success,
            "hotkeys_considered": summary.hotkeys_considered,
            "hotkeys_updated": summary.hotkeys_updated,
            "zeroed_hotkeys": summary.zeroed_hotkeys,
            "jobs_considered": summary.jobs_considered,
            "completed_at": summary.timestamp.isoformat(),
        }

    def _complete_level(self, summary: ScoreRunSummary | None, start_fields: dict[str, Any]) -> str:  # noqa: ARG002
        if summary is None:
            return "info"
        return "info" if summary.success else "error"

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
