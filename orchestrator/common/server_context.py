from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from .structured_logging import StructuredLogger

if TYPE_CHECKING:
    from orchestrator.services.score_service import ScoreService


@dataclass
class ServerContext:
    """Holds process-wide shared utilities and context, e.g. logging.

    Extend this as needed to include metrics, tracing, config, etc.
    """

    logger: StructuredLogger
    score_service: Optional["ScoreService"] = None

    @staticmethod
    def default(
        service_name: str = "orchestrator",
        *,
        score_service: "ScoreService | None" = None,
    ) -> "ServerContext":
        return ServerContext(
            logger=StructuredLogger(name=service_name, base_context={"service": service_name}),
            score_service=score_service,
        )
