from __future__ import annotations

import argparse
import os
import uuid
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI

from orchestrator.clients.jobrelay_client import BaseJobRelayClient, JobRelayHttpClient, JobRelaySettings
from orchestrator.config import OrchestratorConfig, load_config
from orchestrator.dependencies import set_dependencies
from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient
from orchestrator.common.epistula_client import EpistulaClient
from orchestrator.common.server_context import ServerContext
from orchestrator.runners.metagraph import MetagraphStateRunner
from orchestrator.runners.score_etl import ScoreETLRunner
from orchestrator.services.callback_service import CallbackService
from orchestrator.services.callback_uploader import BaseUploader, GCSUploader
from orchestrator.services.database_service import DatabaseService
from orchestrator.services.job_service import JobService
from orchestrator.services.subnet_state_service import SubnetStateService
from orchestrator.services.score_service import ScoreService
from orchestrator.routes import create_internal_router, create_public_router

__all__ = ["orchestrator"]


METAGRAPH_RUN_INTERVAL_SECONDS = 300


class Orchestrator:  # noqa: D101 – thin wrapper around FastAPI app
    def __init__(
        self,
        *,
        config: OrchestratorConfig | None = None,
        config_path: str | Path | None = None,
        audit_sample_size: float | None = None,
        metagraph_runner_interval: float | None = None,
        metagraph_db_path: Optional[str] = None,
    ) -> None:  # noqa: D401 – simple init
        self.config = config or load_config(config_path)

        if audit_sample_size is not None:
            self.config.audit_sample_size = audit_sample_size
        if metagraph_runner_interval is not None:
            self.config.metagraph_runner_interval = metagraph_runner_interval
        if metagraph_db_path is not None:
            override_path = Path(metagraph_db_path)
            self.config.database.path = override_path
            self.config.metagraph.db_path = override_path

        self.miner_registry = None

        database_path = self.config.database.path
        if database_path is not None:
            database_path = Path(database_path)
        else:
            database_path = Path(
                "/tmp", f"orchestrator_state-{os.getpid()}-{uuid.uuid4().hex}.db"
            )
        self.config.database.path = database_path
        self.config.metagraph.db_path = database_path
        self.database_service = DatabaseService(db_path=database_path)

        self.epistula_client = EpistulaClient()
        self.miner_metagraph_client = LiveMinerMetagraphClient(
            database_service=self.database_service,
            epistula_client=self.epistula_client
        )
        netuid = 11
        network = "finney"
        self.subnet_state_service = SubnetStateService(network=network)

        self.server_context = ServerContext.default(service_name="orchestrator")

        callback_cfg = self.config.callback
        uploader: BaseUploader
        if callback_cfg.effective_uploader() == "gcs":
            try:
                uploader = GCSUploader(
                    bucket=callback_cfg.gcs.bucket or "",
                    prefix=callback_cfg.gcs.prefix,
                    credentials_path=callback_cfg.gcs.credentials_path,
                )
            except Exception as exc:  # noqa: BLE001
                self.server_context.logger.warning(
                    "callback.gcs_uploader_init_failed bucket=%s -- using noop uploader: %s",
                    callback_cfg.gcs.bucket,
                    exc,
                )
                uploader = BaseUploader()
        else:
            uploader = BaseUploader()

        self.callback_uploader = uploader
        self.callback_service = CallbackService(uploader=self.callback_uploader)

        jobrelay_cfg = self.config.jobrelay
        if not jobrelay_cfg.is_enabled():
            raise RuntimeError("Job relay integration must be enabled for the orchestrator")

        if not jobrelay_cfg.base_url:
            raise RuntimeError("Job relay base URL must be configured")

        try:
            jobrelay_settings = JobRelaySettings(
                base_url=jobrelay_cfg.base_url,
                auth_token=jobrelay_cfg.auth_token,
                timeout_seconds=jobrelay_cfg.timeout_seconds,
            )
            self.job_relay_client = JobRelayHttpClient(jobrelay_settings)
        except Exception as exc:  # noqa: BLE001
            self.server_context.logger.error(
                "jobrelay.client_init_failed url=%s error=%s",
                jobrelay_cfg.base_url,
                exc,
            )
            raise

        self.job_service = JobService(job_relay=self.job_relay_client)
        self.score_service = ScoreService(
            database_service=self.database_service,
            job_service=self.job_service,
            subnet_state_service=self.subnet_state_service,
            netuid=netuid,
            network=network,
            miner_metagraph_client=self.miner_metagraph_client,
        )
        self.server_context.score_service = self.score_service

        self.metagraph_runner = MetagraphStateRunner(
            miner_metagraph_client=self.miner_metagraph_client,
            netuid=netuid,
            network=network,
            interval_seconds=self.config.metagraph_runner_interval,
            logger=self.server_context.logger,
            subnet_fetcher=self.subnet_state_service.fetch_state,
        )

        self.score_runner = ScoreETLRunner(
            miner_metagraph_client=self.miner_metagraph_client,
            job_relay_client=self.job_relay_client,
            score_service=self.score_service,
            netuid=netuid,
            network=network,
            logger=self.server_context.logger,
        )

        self.app = FastAPI(title="Orchestrator Service", version="0.0.1")
        self._setup_dependencies_and_routes()

    def _setup_dependencies_and_routes(self) -> None:  # noqa: D401 – helper method
        set_dependencies(
            miner_metagraph_client=self.miner_metagraph_client,
            database_service=self.database_service,
            server_context=self.server_context,
            callback_service=self.callback_service,
            job_service=self.job_service,
            job_relay_client=self.job_relay_client,
            config=self.config,
            subnet_state_service=self.subnet_state_service,
            score_service=self.score_service,
            epistula_client=self.epistula_client,
        )
        self.app.include_router(create_internal_router())
        self.app.include_router(create_public_router())

        async def _verify_jobrelay_connection() -> None:
            self.server_context.logger.info(
                "jobrelay.connection_check.start",
                url=self.config.jobrelay.base_url,
                auth_token=self.config.jobrelay.auth_token,
            )
            try:
                await self.job_relay_client.verify_connection()
            except Exception as exc:  # noqa: BLE001
                self.server_context.logger.error(
                    "jobrelay.connection_check_failed",
                    url=self.config.jobrelay.base_url,
                    error=str(exc),
                )
                raise RuntimeError("Job relay connectivity check failed") from exc
            self.server_context.logger.info(
                "jobrelay.connection_check.ok",
                url=self.config.jobrelay.base_url,
            )

        async def _start_metagraph_runner() -> None:
            await self.metagraph_runner.run_once()
            self.server_context.logger.info(
                "metagraph.full_init.complete",
            )
            await self.metagraph_runner.start()

        async def _start_score_runner() -> None:
            await self.score_runner.run_once()
            self.server_context.logger.info(
                "score_etl.full_init.complete",
            )
            await self.score_runner.start()

        async def _stop_metagraph_runner() -> None:
            await self.metagraph_runner.stop()
            self.database_service.close()

        async def _stop_score_runner() -> None:
            await self.score_runner.stop()

        self.app.add_event_handler("startup", _verify_jobrelay_connection)
        self.app.add_event_handler("startup", _start_metagraph_runner)
        self.app.add_event_handler("startup", _start_score_runner)
        self.app.add_event_handler("shutdown", _stop_metagraph_runner)
        self.app.add_event_handler("shutdown", _stop_score_runner)


def create_app() -> FastAPI:
    orch = Orchestrator()
    return orch.app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orchestrator Service")
    parser.add_argument(
        "--live-reload",
        action="store_true",
        help="Enable hot reload for development (watches for file changes)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=42069,
        help="Port to bind the server (default: 42069)",
    )
    
    args = parser.parse_args()
    
    uvicorn.run(
        "orchestrator.server:create_app",
        host="0.0.0.0",
        port=args.port,
        reload=args.live_reload,
        factory=True,
    )
