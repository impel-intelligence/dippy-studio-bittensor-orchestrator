from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Type, TypeVar

T = TypeVar("T")


def dump_model(model: Any) -> dict[str, Any]:
    """Return a plain dictionary for Pydantic v1/v2 models or generic mappings."""
    if hasattr(model, "model_dump"):
        return model.model_dump()  # type: ignore[attr-defined]
    if hasattr(model, "dict"):
        return model.dict()  # type: ignore[attr-defined]
    if isinstance(model, Mapping):
        return dict(model)
    return dict(model)  # type: ignore[arg-type]


def validate_model(model_cls: Type[T], payload: Any) -> T:
    """Instantiate a Pydantic model regardless of v1/v2 API differences."""
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(payload)  # type: ignore[attr-defined]
    if hasattr(model_cls, "parse_obj"):
        return model_cls.parse_obj(payload)  # type: ignore[attr-defined]
    return model_cls(**payload)
