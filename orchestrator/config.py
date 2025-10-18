"""Configuration loading helpers for the orchestrator service."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml


DEFAULT_CONFIG_PATH = Path("orchestrator/config.yaml")
FALLBACK_CONFIG_PATH = Path("orchestrator/config.example.yaml")


@dataclass
class DatabaseConfig:
    path: Optional[Path] = None


@dataclass
class MetagraphConfig:
    db_path: Optional[Path] = None


@dataclass
class GCSConfig:
    bucket: Optional[str] = None
    prefix: str = "callbacks"
    credentials_path: Optional[Path] = None


@dataclass
class CallbackConfig:
    uploader: str = "noop"
    gcs: GCSConfig = field(default_factory=GCSConfig)
    url: Optional[str] = None

    def effective_uploader(self) -> str:
        if self.uploader.lower() == "gcs" and self.gcs.bucket:
            return "gcs"
        return "noop"

    def resolved_callback_url(self) -> Optional[str]:
        return self.url


@dataclass
class JobRelayConfig:
    enabled: bool = True
    base_url: Optional[str] = "http://localhost:8081"
    auth_token: Optional[str] = None
    timeout_seconds: float = 5.0

    def is_enabled(self) -> bool:
        return self.enabled and bool(self.base_url)


@dataclass
class OrchestratorConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    metagraph: MetagraphConfig = field(default_factory=MetagraphConfig)
    callback: CallbackConfig = field(default_factory=CallbackConfig)
    jobrelay: JobRelayConfig = field(default_factory=JobRelayConfig)
    audit_sample_size: float = 0.1
    metagraph_runner_interval: float = 300.0
    audit_target_domain: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrchestratorConfig":
        database_data = data.get("database", {}) or {}
        database_path_raw = database_data.get("path") or database_data.get("db_path")

        metagraph_data = data.get("metagraph", {}) or {}
        metagraph_path_raw = metagraph_data.get("db_path")

        resolved_path_raw = database_path_raw or metagraph_path_raw
        resolved_path = Path(resolved_path_raw) if resolved_path_raw else None

        database = DatabaseConfig(path=resolved_path)
        metagraph = MetagraphConfig(db_path=resolved_path)

        callback_data = data.get("callback", {}) or {}
        uploader_value = str(callback_data.get("uploader", "noop")).lower()
        gcs_data = callback_data.get("gcs", {}) or {}
        gcs_credentials = gcs_data.get("credentials_path")
        callback = CallbackConfig(
            uploader=uploader_value,
            gcs=GCSConfig(
                bucket=gcs_data.get("bucket"),
                prefix=gcs_data.get("prefix", "callbacks"),
                credentials_path=Path(gcs_credentials) if gcs_credentials else None,
            ),
            url=callback_data.get("url") or callback_data.get("callback_url"),
        )

        jobrelay_defaults = JobRelayConfig()
        jobrelay_data = data.get("jobrelay", {}) or {}
        enabled_raw = jobrelay_data.get("enabled")
        if enabled_raw is None:
            enabled_value = jobrelay_defaults.enabled
        elif isinstance(enabled_raw, str):
            enabled_value = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
        else:
            enabled_value = bool(enabled_raw)

        base_url_value = jobrelay_data.get("base_url", jobrelay_defaults.base_url)
        timeout_value = jobrelay_data.get("timeout_seconds", jobrelay_defaults.timeout_seconds)
        jobrelay_timeout = _maybe_float(timeout_value, jobrelay_defaults.timeout_seconds)
        if jobrelay_timeout is None:
            jobrelay_timeout = jobrelay_defaults.timeout_seconds

        jobrelay = JobRelayConfig(
            enabled=enabled_value,
            base_url=base_url_value or jobrelay_defaults.base_url,
            auth_token=jobrelay_data.get("auth_token", jobrelay_defaults.auth_token),
            timeout_seconds=jobrelay_timeout,
        )

        audit_sample_size = float(data.get("audit_sample_size", 0.1))
        metagraph_runner_interval = float(data.get("metagraph_runner_interval", 300.0))
        audit_target_domain = (data.get("audit_target_domain") or None)

        return cls(
            database=database,
            metagraph=metagraph,
            callback=callback,
            jobrelay=jobrelay,
            audit_sample_size=audit_sample_size,
            metagraph_runner_interval=metagraph_runner_interval,
            audit_target_domain=audit_target_domain,
        )

    def apply_env_overrides(self, env: Mapping[str, str]) -> None:
        if "METAGRAPH_DB_PATH" in env:
            value = env["METAGRAPH_DB_PATH"]
            override = Path(value) if value else None
            self.database.path = override
            self.metagraph.db_path = override

        if "DATABASE_PATH" in env:
            value = env["DATABASE_PATH"]
            override = Path(value) if value else None
            self.database.path = override
            self.metagraph.db_path = override

        if "AUDIT_SAMPLE_SIZE" in env:
            self.audit_sample_size = _maybe_float(env["AUDIT_SAMPLE_SIZE"], self.audit_sample_size)
        if "AUDIT_TARGET_DOMAIN" in env:
            value = env["AUDIT_TARGET_DOMAIN"].strip()
            self.audit_target_domain = value or None
        if "METAGRAPH_RUN_INTERVAL" in env:
            self.metagraph_runner_interval = _maybe_float(env["METAGRAPH_RUN_INTERVAL"], self.metagraph_runner_interval)

        callback_uploader = env.get("CALLBACK_UPLOADER")
        if callback_uploader:
            self.callback.uploader = callback_uploader.lower()

        if "CALLBACK_GCS_BUCKET" in env:
            self.callback.gcs.bucket = env["CALLBACK_GCS_BUCKET"] or None
            if self.callback.gcs.bucket:
                self.callback.uploader = "gcs"
        if "CALLBACK_GCS_PREFIX" in env:
            self.callback.gcs.prefix = env["CALLBACK_GCS_PREFIX"] or self.callback.gcs.prefix
        credentials_override = env.get("CALLBACK_GCS_CREDENTIALS") or env.get("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_override:
            self.callback.gcs.credentials_path = Path(credentials_override)
        if "CALLBACK_URL" in env:
            self.callback.url = env["CALLBACK_URL"].strip() or None


def load_config(path: str | Path | None = None, *, env: Mapping[str, str] | None = None) -> OrchestratorConfig:
    env = env or os.environ

    candidate_paths: list[Path] = []
    if path:
        candidate_paths.append(Path(path))
    elif env.get("ORCHESTRATOR_CONFIG_PATH"):
        candidate_paths.append(Path(env["ORCHESTRATOR_CONFIG_PATH"]))

    candidate_paths.extend([DEFAULT_CONFIG_PATH, FALLBACK_CONFIG_PATH])

    config_data: dict[str, Any] = {}
    for candidate in candidate_paths:
        if candidate and candidate.exists():
            with candidate.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
            config_data = loaded if isinstance(loaded, dict) else {}
            break

    config = OrchestratorConfig.from_dict(config_data)
    config.apply_env_overrides(env)
    return config

def _maybe_float(value: Any, default: Optional[float]) -> Optional[float]:
    if value in {None, "", "None"}:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "OrchestratorConfig",
    "DatabaseConfig",
    "MetagraphConfig",
    "CallbackConfig",
    "GCSConfig",
    "JobRelayConfig",
    "load_config",
]
