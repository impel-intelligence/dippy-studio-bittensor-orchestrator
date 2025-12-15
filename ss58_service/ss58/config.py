"""Configuration for the SS58 signature service."""

from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class HeadState(BaseModel):
    """Represents the current head pointer."""

    cid: str
    updated_at: int


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    service_name: str = "ss58"
    service_version: str = "0.0.1"

    pinata_jwt: str | None = Field(default=None, alias="SS58_PINATA_JWT")
    pinata_api_key: str | None = Field(default=None, alias="SS58_PINATA_API_KEY")
    pinata_api_secret: str | None = Field(default=None, alias="SS58_PINATA_API_SECRET")
    pinata_api_base: HttpUrl = Field(default="https://api.pinata.cloud", alias="SS58_PINATA_API_BASE")
    pinata_timeout_seconds: int = Field(default=20, alias="SS58_PINATA_TIMEOUT_SECONDS")
    pinata_gateway_base: HttpUrl = Field(default="https://gateway.pinata.cloud/ipfs", alias="SS58_PINATA_GATEWAY_BASE")

    head_path: str = Field(default="/var/lib/ss58/head.json", alias="SS58_HEAD_PATH")

    signing_seed_hex: str | None = Field(default=None, alias="SS58_ED25519_SEED_HEX")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", populate_by_name=True)
