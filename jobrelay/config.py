"""Configuration utilities for the job relay service."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the job relay service."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="JOBRELAY_", extra="ignore")

    duckdb_path: Path = Field(default=Path("jobrelay/state/jobrelay.duckdb"))
    auth_token: str = Field(default="my-secret-key-here")

    ttl_days: int = Field(default=7, ge=1)
    cleanup_interval_seconds: int = Field(default=3600, ge=60)
    snapshot_interval_seconds: int = Field(default=3600, ge=60)

    duckdb_memory_limit: str = Field(default="4GB")
    duckdb_threads: int = Field(default=4, ge=1)
    cache_db_path: Optional[Path] = Field(default=None)
    cache_max_size_gb: int = Field(default=50, ge=1)

    postgres_dsn: Optional[str] = Field(default=None)
    postgres_min_connections: int = Field(default=1, ge=1)
    postgres_max_connections: int = Field(default=10, ge=1)

    sync_interval_seconds: int = Field(default=3600, ge=60)
    partition_strategy: str = Field(default="hourly")
    row_group_size: int = Field(default=100_000, ge=1)
    compression_type: str = Field(default="ZSTD")

    gcs_bucket: Optional[str] = Field(default="subnet11_internal")
    gcs_prefix: str = Field(default="jobrelay_snapshots")
    gcp_credentials_path: Optional[Path] = Field(default=None)

    app_name: str = Field(default="jobrelay")

    @property
    def duckdb_directory(self) -> Path:
        return self.duckdb_path.expanduser().resolve().parent

    @property
    def resolved_cache_db_path(self) -> Path:
        path = self.cache_db_path or self.duckdb_path
        return Path(path).expanduser().resolve()

    @property
    def resolved_gcp_credentials(self) -> Optional[Path]:
        if self.gcp_credentials_path:
            return self.gcp_credentials_path.expanduser().resolve()
        return None

    @property
    def effective_sync_interval(self) -> int:
        if self.sync_interval_seconds:
            return self.sync_interval_seconds
        return self.snapshot_interval_seconds


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
