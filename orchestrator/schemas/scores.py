from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel


class ScoreValue(BaseModel):
    total_score: float = 0.0


class ScorePayload(BaseModel):
    status: str
    score: ScoreValue


class ScoresResponse(BaseModel):
    scores: Dict[str, ScorePayload]
    stats: Dict[str, Any]


__all__ = [
    "ScoreValue",
    "ScorePayload",
    "ScoresResponse",
]
