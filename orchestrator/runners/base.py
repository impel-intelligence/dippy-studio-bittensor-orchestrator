from __future__ import annotations

import logging
import time
from typing import Any, Generic, TypeVar

from orchestrator.common.structured_logging import StructuredLogger

ResultT = TypeVar("ResultT")


class InstrumentedRunner(Generic[ResultT]):
    """Shared runner plumbing to log lifecycle and duration."""

    def __init__(self, *, name: str, logger: StructuredLogger | logging.Logger | None = None) -> None:
        self._name = name
        self._logger: StructuredLogger | logging.Logger = logger if logger is not None else logging.getLogger(__name__)

    async def execute(self) -> ResultT | None:
        start_fields = {**self._start_fields()}
        started_at = time.monotonic()
        self._log("info", f"{self._name}.start", **start_fields)

        try:
            result = await self._run()
        except Exception as exc:  # pragma: no cover - defensive logging wrapper
            duration_ms = self._duration_ms(started_at)
            error_fields = self._error_fields(exc, start_fields)
            error_fields.setdefault("duration_ms", duration_ms)
            self._log("error", f"{self._name}.failed", **error_fields)
            return None

        duration_ms = self._duration_ms(started_at)
        complete_fields = self._complete_fields(result, start_fields)
        complete_fields.setdefault("duration_ms", duration_ms)
        level = self._complete_level(result, start_fields)
        self._log(level, f"{self._name}.complete", **complete_fields)
        return result

    async def _run(self) -> ResultT | None:
        raise NotImplementedError

    def _start_fields(self) -> dict[str, Any]:
        return {}

    def _complete_fields(self, result: ResultT | None, start_fields: dict[str, Any]) -> dict[str, Any]:
        status = "success" if result is not None else "skipped"
        return {**start_fields, "status": status}

    def _complete_level(self, result: ResultT | None, start_fields: dict[str, Any]) -> str:  # noqa: ARG002
        return "info"

    def _error_fields(self, exc: Exception, start_fields: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        return {**start_fields, "error": str(exc)}

    def _duration_ms(self, started_at: float) -> int:
        return int((time.monotonic() - started_at) * 1000)

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
