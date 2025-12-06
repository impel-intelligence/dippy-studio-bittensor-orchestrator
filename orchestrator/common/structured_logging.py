from __future__ import annotations

import json
import logging
import logging.handlers
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict


def _patch_queue_listener_for_eof() -> None:
    """Ensure stdlib QueueListener threads stop cleanly when queues close."""

    listener = logging.handlers.QueueListener

    if getattr(listener, "_handles_eof_safely", False):
        return

    original_dequeue = listener.dequeue

    def _safe_dequeue(self, block):  # type: ignore[override]
        try:
            return original_dequeue(self, block)
        except EOFError:
            return self._sentinel

    listener.dequeue = _safe_dequeue  # type: ignore[assignment]
    setattr(listener, "_handles_eof_safely", True)


_patch_queue_listener_for_eof()


def _get_current_trace_context() -> Dict[str, str]:
    """Extract current trace context from OpenTelemetry if available."""
    try:
        from opentelemetry import trace
        from opentelemetry.trace import format_trace_id, format_span_id

        span = trace.get_current_span()
        if span and span.is_recording():
            ctx = span.get_span_context()
            if ctx.is_valid:
                return {
                    "trace_id": format_trace_id(ctx.trace_id),
                    "span_id": format_span_id(ctx.span_id),
                }
    except ImportError:
        pass
    except Exception:
        pass
    return {}


def _json_dumps(data: Dict[str, Any]) -> str:
    try:
        return json.dumps(data, separators=(",", ":"), default=str)
    except Exception:
        def _fallback(o: Any) -> str:
            try:
                return str(o)
            except Exception:
                return "<unserializable>"

        return json.dumps(data, separators=(",", ":"), default=_fallback)


@dataclass
class StructuredLogger:
    """Tiny structured logger that emits JSON lines via stdlib logging.

    This logger is now OTEL-aware: it propagates logs to the root logger
    so that OTEL LoggingHandler can export them with trace context.

    Usage:
      logger = StructuredLogger(name="orchestrator", base_context={"service":"orchestrator"})
      logger.info("job.created", job_id="...", hotkey="...")
      req_logger = logger.with_context(request_id="...")
      req_logger.error("miner.failed", detail="...")
    """

    name: str = "orchestrator"
    base_context: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(self.name)
        self._ensure_logger_configured()

    def _ensure_logger_configured(self) -> None:
        """Configure logger to propagate to root for OTEL integration.

        IMPORTANT: We now set propagate=True so logs reach the root logger's
        OTEL LoggingHandler for export to SigNoz with trace context.
        """
        # Only add a local handler if the root logger has no handlers
        # (i.e., OTEL hasn't been configured yet)
        root_logger = logging.getLogger()
        if not root_logger.handlers:
            handler_attached = any(
                getattr(handler, "_structured_logger_handler", False)
                for handler in self._logger.handlers
            )
            if not handler_attached:
                handler = logging.StreamHandler(stream=sys.stdout)
                handler.setFormatter(logging.Formatter("%(message)s"))
                handler.setLevel(logging.INFO)
                setattr(handler, "_structured_logger_handler", True)
                self._logger.addHandler(handler)

        if self._logger.level == logging.NOTSET:
            self._logger.setLevel(logging.INFO)

        # CRITICAL: Enable propagation so logs reach OTEL LoggingHandler on root
        self._logger.propagate = True

    def with_context(self, **ctx: Any) -> "StructuredLogger":
        merged = {**self.base_context, **ctx}
        return StructuredLogger(name=self.name, base_context=merged)

    def _emit(self, level: int, event: str, **fields: Any) -> None:
        # Include trace context in the structured log record
        trace_ctx = _get_current_trace_context()

        record = {
            "ts": time.time(),
            "event": event,
            **trace_ctx,  # Add trace_id/span_id to JSON payload
            **self.base_context,
            **fields,
        }

        # Pass fields as extra for OTEL log attribute extraction
        # The LoggingHandler will use these to set log record attributes
        extra_attrs = {
            "event": event,
            **trace_ctx,
            **self.base_context,
            **fields,
        }
        self._logger.log(level, _json_dumps(record), extra=extra_attrs)

    def debug(self, event: str, **fields: Any) -> None:
        self._emit(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._emit(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._emit(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._emit(logging.ERROR, event, **fields)
