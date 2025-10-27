from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import Depends

from orchestrator.clients.jobrelay_client import BaseJobRelayClient
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.server_context import ServerContext
from orchestrator.common.structured_logging import StructuredLogger
from orchestrator.config import OrchestratorConfig
from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient
from orchestrator.services.callback_service import CallbackService
from orchestrator.services.database_service import DatabaseService
from orchestrator.services.health_service import HealthService
from orchestrator.services.job_service import JobService
from orchestrator.services.listen_service import ListenService
from orchestrator.services.subnet_state_service import SubnetStateService
from orchestrator.services.score_service import ScoreService


@dataclass
class DependencyRegistry:
    miner_metagraph_client: Optional[LiveMinerMetagraphClient] = None
    database_service: Optional[DatabaseService] = None
    server_context: Optional[ServerContext] = None
    callback_service: Optional[CallbackService] = None
    job_relay_client: Optional[BaseJobRelayClient] = None
    config: Optional[OrchestratorConfig] = None
    subnet_state_service: Optional[SubnetStateService] = None
    score_service: Optional[ScoreService] = None
    epistula_client: Optional[EpistulaClient] = None
    job_service: Optional[JobService] = None


_registry = DependencyRegistry()


def _require(attribute: str, message: str) -> object:
    value = getattr(_registry, attribute)
    if value is None:
        raise RuntimeError(message)
    return value


def set_dependencies(
    *,
    miner_metagraph_client: Optional[LiveMinerMetagraphClient] = None,
    database_service: Optional[DatabaseService] = None,
    server_context: Optional[ServerContext] = None,
    callback_service: Optional[CallbackService] = None,
    job_service: Optional[JobService] = None,
    job_relay_client: BaseJobRelayClient,
    config: Optional[OrchestratorConfig] = None,
    subnet_state_service: Optional[SubnetStateService] = None,
    score_service: Optional[ScoreService] = None,
    epistula_client: Optional[EpistulaClient] = None,
) -> None:
    """Set the global dependencies that will be injected into services."""
    if miner_metagraph_client is not None:
        _registry.miner_metagraph_client = miner_metagraph_client
    if database_service is not None:
        _registry.database_service = database_service
    if server_context is not None:
        _registry.server_context = server_context
    if callback_service is not None:
        _registry.callback_service = callback_service
    _registry.job_relay_client = job_relay_client
    if config is not None:
        _registry.config = config
    if subnet_state_service is not None:
        _registry.subnet_state_service = subnet_state_service
    if score_service is not None:
        _registry.score_service = score_service
    if job_service is not None:
        _registry.job_service = job_service
    if epistula_client is not None:
        _registry.epistula_client = epistula_client


def get_miner_metagraph_client() -> LiveMinerMetagraphClient:
    return _require("miner_metagraph_client", "MinerMetagraphClient not initialized")  # type: ignore[return-value]


def get_database_service() -> DatabaseService:
    return _require("database_service", "DatabaseService not initialized")  # type: ignore[return-value]


def get_server_context() -> ServerContext:
    return _require("server_context", "ServerContext not initialized")  # type: ignore[return-value]


def get_structured_logger(
    ctx: ServerContext = Depends(get_server_context),
) -> StructuredLogger:
    return ctx.logger


def get_config() -> OrchestratorConfig:
    return _require("config", "OrchestratorConfig not initialized")  # type: ignore[return-value]


def get_epistula_client() -> EpistulaClient:
    return _require("epistula_client", "EpistulaClient not initialized")  # type: ignore[return-value]


def get_subnet_state_service() -> SubnetStateService:
    return _require("subnet_state_service", "SubnetStateService not initialized")  # type: ignore[return-value]


def get_score_service() -> ScoreService:
    return _require("score_service", "ScoreService not initialized")  # type: ignore[return-value]


def get_callback_service() -> CallbackService:
    if _registry.callback_service is not None:
        return _registry.callback_service
    return CallbackService()


def get_job_relay_client() -> BaseJobRelayClient:
    return _require("job_relay_client", "Job relay client not initialized")  # type: ignore[return-value]


def get_job_service(
    job_relay_client: BaseJobRelayClient = Depends(get_job_relay_client),
) -> JobService:
    if _registry.job_service is not None:
        return _registry.job_service
    return JobService(job_relay=job_relay_client)


def get_listen_service(
    job_service: JobService = Depends(get_job_service),
    metagraph: LiveMinerMetagraphClient = Depends(get_miner_metagraph_client),
    config: OrchestratorConfig = Depends(get_config),
) -> ListenService:
    return ListenService(
        job_service=job_service,
        metagraph=metagraph,
        callback_url=config.callback.resolved_callback_url(),
    )


def get_health_service() -> HealthService:
    return HealthService() 
