from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, Field


class Miner(BaseModel):
    uid: int
    network_address: str
    valid: bool
    alpha_stake: int
    capacity: Dict[str, bool] = Field(default_factory=dict)
    hotkey: Optional[str] = None
    failed_audits: int = Field(default=0, ge=0)
    failure_count: int = Field(default=0, ge=0)


__all__ = ["Miner"]
