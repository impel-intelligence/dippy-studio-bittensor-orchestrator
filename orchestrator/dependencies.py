from __future__ import annotations

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


_miner_metagraph_client: Optional[LiveMinerMetagraphClient] = None
_database_service: Optional[DatabaseService] = None
_server_context: Optional[ServerContext] = None
_callback_service: Optional[CallbackService] = None
_job_relay_client: Optional[BaseJobRelayClient] = None
_config: Optional[OrchestratorConfig] = None
_subnet_state_service: Optional[SubnetStateService] = None
_score_service: Optional[ScoreService] = None
_epistula_client: Optional[EpistulaClient] = None
_job_service: Optional[JobService] = None


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
    global _miner_metagraph_client, _database_service, _server_context, _callback_service
    global _job_relay_client, _config, _subnet_state_service, _score_service, _job_service
    global _epistula_client
    _miner_metagraph_client = miner_metagraph_client
    _database_service = database_service
    _server_context = server_context
    _callback_service = callback_service
    _job_relay_client = job_relay_client
    _config = config
    _subnet_state_service = subnet_state_service
    _score_service = score_service
    _job_service = job_service
    _epistula_client = epistula_client


def get_miner_metagraph_client() -> LiveMinerMetagraphClient:
    if _miner_metagraph_client is None:
        raise RuntimeError("MinerMetagraphClient not initialized")
    return _miner_metagraph_client


def get_database_service() -> DatabaseService:
    if _database_service is None:
        raise RuntimeError("DatabaseService not initialized")
    return _database_service


def get_server_context() -> ServerContext:
    if _server_context is None:
        raise RuntimeError("ServerContext not initialized")
    return _server_context


def get_structured_logger(
    ctx: ServerContext = Depends(get_server_context),
) -> StructuredLogger:
    return ctx.logger


def get_config() -> OrchestratorConfig:
    if _config is None:
        raise RuntimeError("OrchestratorConfig not initialized")
    return _config


def get_epistula_client() -> EpistulaClient:
    if _epistula_client is None:
        raise RuntimeError("EpistulaClient not initialized")
    return _epistula_client


def get_subnet_state_service() -> SubnetStateService:
    if _subnet_state_service is None:
        raise RuntimeError("SubnetStateService not initialized")
    return _subnet_state_service


def get_score_service() -> ScoreService:
    if _score_service is None:
        raise RuntimeError("ScoreService not initialized")
    return _score_service


def get_callback_service() -> CallbackService:
    if _callback_service is not None:
        return _callback_service
    return CallbackService()


def get_job_relay_client() -> BaseJobRelayClient:
    if _job_relay_client is None:
        raise RuntimeError("Job relay client not initialized")
    return _job_relay_client


def get_job_service(
    job_relay_client: BaseJobRelayClient = Depends(get_job_relay_client),
) -> JobService:
    if _job_service is not None:
        return _job_service
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
