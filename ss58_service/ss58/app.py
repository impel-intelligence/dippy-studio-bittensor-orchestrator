"""FastAPI application for the SS58 signature service."""

from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import Depends, FastAPI, HTTPException, status

from .config import HeadState, Settings
from .pinata import PinataClient
from .repository import EntryRepository
from .schemas import AddRequest, AddResponse, HeadResponse
from .service import SS58Service
from .signing import load_signer
from .store import HeadStore


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = Settings()
    app = FastAPI(title="SS58 Signature Service", version=settings.service_version)

    def get_service() -> SS58Service:
        if not getattr(app.state, "service", None):
            try:
                signer = load_signer(settings.signing_seed_hex)
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Signing key not configured: {exc}",
                )
            try:
                pinata = PinataClient(
                    jwt=settings.pinata_jwt,
                    api_key=settings.pinata_api_key,
                    api_secret=settings.pinata_api_secret,
                    base_url=str(settings.pinata_api_base),
                    timeout_seconds=settings.pinata_timeout_seconds,
                )
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Pinata config error: {exc}",
                )
            head_store = HeadStore(settings.head_path)
            repository = EntryRepository(
                gateway_base=str(settings.pinata_gateway_base),
                timeout_seconds=settings.pinata_timeout_seconds,
                verify_key_hex=signer.public_hex,
            )
            app.state.service = SS58Service(
                settings=settings,
                signer=signer,
                pinata=pinata,
                head_store=head_store,
                repository=repository,
            )
        return app.state.service

    @app.get("/health", tags=["health"])
    def health() -> Dict[str, Any]:
        """Lightweight liveness/readiness probe."""
        return {
            "status": "ok",
            "service": "ss58",
            "revision": os.getenv("SS58_REVISION", "local"),
            "version": app.version,
        }

    @app.get("/head", response_model=HeadResponse, tags=["head"])
    def get_head(service: SS58Service = Depends(get_service)) -> HeadResponse:
        state: HeadState | None = service.head()
        if not state:
            return HeadResponse(cid=None, updated_at=None, public_key_hex=service.public_key_hex)
        return HeadResponse(cid=state.cid, updated_at=state.updated_at, public_key_hex=service.public_key_hex)

    @app.post("/genesis", response_model=AddResponse, tags=["append"])
    def create_genesis(service: SS58Service = Depends(get_service)) -> AddResponse:
        """Create an empty genesis entry."""
        return service.create_genesis()

    @app.post("/add", response_model=AddResponse, tags=["append"])
    def add_addresses(payload: AddRequest, service: SS58Service = Depends(get_service)) -> AddResponse:
        """Append a new entry containing SS58 addresses."""
        try:
            return service.append(payload.addresses)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to append entry: {exc}",
            )

    @app.get("/addresses", response_model=Dict[str, Any], tags=["read"])
    def dump_addresses(service: SS58Service = Depends(get_service)) -> Dict[str, Any]:
        """Return the full list of addresses (oldest to newest)."""
        addresses = service.dump_addresses()
        return {"count": len(addresses), "addresses": addresses}

    return app


app = create_app()
