"""Pydantic models used by the SS58 service."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, validator


class EntryData(BaseModel):
    """Inner data payload that is signed and stored on IPFS."""

    addresses: List[str] = Field(default_factory=list)
    timestamp: int
    prev: Optional[str] = None

    @validator("addresses")
    def validate_addresses(cls, value: List[str]) -> List[str]:
        if not isinstance(value, list):
            raise ValueError("addresses must be a list of strings")
        cleaned = []
        for addr in value:
            if not isinstance(addr, str) or not addr.strip():
                raise ValueError("addresses must contain non-empty strings")
            cleaned.append(addr.strip())
        return cleaned


class Entry(BaseModel):
    """Full entry pinned to IPFS."""

    version: str = "1.0"
    data: EntryData
    signature: str


class AddRequest(BaseModel):
    """Request payload for /add."""

    addresses: List[str]

    @validator("addresses")
    def validate_addresses(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("addresses cannot be empty")
        cleaned = []
        for addr in value:
            if not isinstance(addr, str) or not addr.strip():
                raise ValueError("addresses must contain non-empty strings")
            cleaned.append(addr.strip())
        return cleaned


class AddResponse(BaseModel):
    cid: str
    prev: Optional[str]
    timestamp: int
    addresses: List[str]
    signature: str


class HeadResponse(BaseModel):
    cid: Optional[str]
    updated_at: Optional[int]
    public_key_hex: Optional[str] = None
