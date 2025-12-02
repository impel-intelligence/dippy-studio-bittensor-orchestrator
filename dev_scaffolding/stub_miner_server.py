"""Minimal stubbed inference server for development/testing.

This FastAPI application mimics the endpoints exposed by ``miner_server.py`` but
serves a static image instead of running real inference. It can be pointed to
the reverse proxy's ``services.inference_server_url`` setting to test end-to-end
flows without GPU requirements.

Supports both async (callback-based) and sync (direct return) endpoints. The
image path defaults to ``dev_scaffolding/assets/stub_miner_sample.png`` but can
be overridden with the ``STUB_MINER_IMAGE_PATH`` environment variable.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict
from sn_uuid import uuid7

logger = logging.getLogger(__name__)


# =============================================================================
# TEMPORARY OVERRIDE - REMOVE THIS SECTION LATER
# =============================================================================
# Force all callbacks to go to a specific URL for testing
# Set ``STUB_MINER_OVERRIDE_CALLBACK_URL`` to override callback destinations.
OVERRIDE_CALLBACK_URL = os.getenv("STUB_MINER_OVERRIDE_CALLBACK_URL")
# =============================================================================
# END TEMPORARY OVERRIDE
# =============================================================================


DEFAULT_IMAGE_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAAWgmWQ0AAAAASUVORK5CYII="
)
DEFAULT_IMAGE_PATH = Path(
    os.getenv(
        "STUB_MINER_IMAGE_PATH",
        str(Path(__file__).parent / "assets" / "stub_miner_sample.png"),
    )
)
DEFAULT_HOST = os.getenv("STUB_MINER_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.getenv("STUB_MINER_PORT", "8765"))


def _ensure_image(path: Path) -> Path:
    """Ensure the stub image exists on disk, creating a placeholder if needed."""
    if path.exists():
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(DEFAULT_IMAGE_BYTES)
    logger.warning("Stub image missing; wrote placeholder bytes to %s", path)
    return path


def _guess_media_type(path: Path) -> str:
    """Infer the appropriate content type based on file suffix."""
    suffix = path.suffix.lower()
    if suffix == ".png":
        return "image/png"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    return "application/octet-stream"


IMAGE_PATH = _ensure_image(DEFAULT_IMAGE_PATH)
IMAGE_MEDIA_TYPE = _guess_media_type(IMAGE_PATH)


class InferenceRequest(BaseModel):
    """Subset of fields accepted by the real inference endpoint."""

    model_config = ConfigDict(extra="allow")

    prompt: Optional[str] = None
    job_id: Optional[str] = None
    callback_url: Optional[str] = None
    callback_secret: Optional[str] = None


class EditRequest(BaseModel):
    """Subset of fields accepted by the real edit endpoint."""

    model_config = ConfigDict(extra="allow")

    prompt: str
    image_url: Optional[str] = None
    image_b64: Optional[str] = None
    seed: int
    guidance_scale: float = 2.5
    num_inference_steps: int = 28
    job_id: Optional[str] = None
    callback_url: Optional[str] = None
    callback_secret: Optional[str] = None


app = FastAPI(
    title="Stub Inference Server",
    description="Serves a static image for testing the reverse proxy. Supports both async (callback) and sync (direct) modes.",
    version="0.3.0",
)

_jobs: Dict[str, Dict[str, Any]] = {}
_edit_jobs: Dict[str, Dict[str, Any]] = {}


async def _dispatch_callback(
    job_id: str,
    callback_url: str,
    image_path: Path,
    callback_secret: Optional[str] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """Upload the generated image to the callback URL."""
    logger.info(f"[Job {job_id}] Dispatching callback to URL: {callback_url}")
    attempted_at = datetime.now(timezone.utc)
    attempt_iso = attempted_at.isoformat()

    headers = {}
    if callback_secret:
        headers["X-Callback-Secret"] = callback_secret
        logger.info(f"[Job {job_id}] Using callback secret: {callback_secret[:8]}...{callback_secret[-4:] if len(callback_secret) > 12 else ''}")
    else:
        logger.info(f"[Job {job_id}] No callback secret provided")

    form = aiohttp.FormData()
    form.add_field("job_id", job_id)
    form.add_field("status", "completed")
    form.add_field("completed_at", attempt_iso)

    try:
        image_bytes = await asyncio.to_thread(image_path.read_bytes)
        logger.debug(f"[Job {job_id}] Read {len(image_bytes)} bytes from {image_path}")
    except Exception as exc:
        logger.error(f"[Job {job_id}] Failed to read image: {exc}")
        return {
            "status": "failed",
            "error": f"Unable to read image: {exc}",
            "attempted_at": attempt_iso,
        }

    form.add_field(
        "image",
        image_bytes,
        filename=image_path.name,
        content_type=_guess_media_type(image_path),
    )

    logger.info(f"[Job {job_id}] Sending form data: job_id={job_id}, status=completed, image_size={len(image_bytes)} bytes")

    timeout_config = aiohttp.ClientTimeout(total=timeout)

    try:
        async with aiohttp.ClientSession(timeout=timeout_config) as session:
            logger.debug(f"[Job {job_id}] POST {callback_url} with headers: {list(headers.keys())}")
            async with session.post(callback_url, data=form, headers=headers) as response:
                response_text = await response.text()
                result = {
                    "status": "delivered" if response.status < 400 else "error",
                    "status_code": response.status,
                    "response_preview": response_text[:500],
                    "attempted_at": attempt_iso,
                }
                if response.status < 400:
                    logger.info(f"[Job {job_id}] ✓ Callback delivered successfully (HTTP {response.status})")
                    logger.debug(f"[Job {job_id}] Response: {response_text[:200]}")
                else:
                    logger.warning(
                        f"[Job {job_id}] ✗ Callback returned error status {response.status}"
                    )
                    logger.warning(f"[Job {job_id}] Callback URL was: {callback_url}")
                    logger.warning(f"[Job {job_id}] Response body: {response_text[:500]}")
                return result
    except Exception as exc:
        logger.error(f"[Job {job_id}] ✗ Callback delivery failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
            "attempted_at": attempt_iso,
        }


def _build_job_response(job_id: str) -> Dict[str, Any]:
    """Return a job payload similar to the real miner server."""
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "id": job_id,
        "status": "completed",
        "queued_at": now_iso,
        "completed_at": now_iso,
        "request": {
            "prompt": _jobs[job_id].get("prompt"),
        },
    }


def _build_edit_job_response(job_id: str) -> Dict[str, Any]:
    """Return an edit job payload similar to the real miner server."""
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "id": job_id,
        "status": "completed",
        "queued_at": now_iso,
        "completed_at": now_iso,
        "request": {
            "prompt": _edit_jobs[job_id].get("prompt"),
            "seed": _edit_jobs[job_id].get("seed"),
        },
    }


@app.post("/inference")
async def submit_inference(request: InferenceRequest):
    """Async endpoint: Acknowledge job and dispatch callback."""
    if not IMAGE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Stub image not found at {IMAGE_PATH}",
        )

    job_id = request.job_id or str(uuid7())
    logger.info(f"[Job {job_id}] Received inference request (prompt: {request.prompt[:50] if request.prompt else 'None'}...)")
    
    # Apply callback URL override if configured
    actual_callback_url = request.callback_url
    if OVERRIDE_CALLBACK_URL and request.callback_url:
        logger.warning(f"[Job {job_id}] ⚠️  OVERRIDING callback URL from {request.callback_url} to {OVERRIDE_CALLBACK_URL}")
        actual_callback_url = OVERRIDE_CALLBACK_URL
    elif OVERRIDE_CALLBACK_URL and not request.callback_url:
        logger.warning(f"[Job {job_id}] ⚠️  No callback URL provided but override is set - using {OVERRIDE_CALLBACK_URL}")
        actual_callback_url = OVERRIDE_CALLBACK_URL
    
    if actual_callback_url:
        logger.info(f"[Job {job_id}] Callback URL: {actual_callback_url}")
        if request.callback_secret:
            logger.info(f"[Job {job_id}] Callback secret provided: {request.callback_secret[:8]}...{request.callback_secret[-4:] if len(request.callback_secret) > 12 else ''}")
        else:
            logger.info(f"[Job {job_id}] No callback secret provided")
    else:
        logger.info(f"[Job {job_id}] No callback URL provided")
    
    _jobs[job_id] = {
        "prompt": request.prompt,
        "callback_url": request.callback_url,
        "callback_secret": request.callback_secret,
    }
    job_payload = _build_job_response(job_id)

    # Dispatch callback if callback_url is provided
    callback_result = None
    if actual_callback_url:
        callback_result = await _dispatch_callback(
            job_id=job_id,
            callback_url=actual_callback_url,
            image_path=IMAGE_PATH,
            callback_secret=request.callback_secret,
        )

    response_data = {
        "accepted": True,
        "job_id": job_id,
        "status": "completed",
        "message": "Stubbed inference job completed immediately",
        "status_url": f"/inference/status/{job_id}",
    }

    if callback_result:
        response_data["callback_result"] = callback_result

    return response_data


@app.get("/inference/status/{job_id}")
async def get_inference_status(job_id: str):
    """Mimic polling the status of an inference job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return _build_job_response(job_id)


@app.post("/sync/inference")
async def submit_sync_inference(request: InferenceRequest):
    """Sync endpoint: Return image directly or timeout."""
    if not IMAGE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Stub image not found at {IMAGE_PATH}",
        )

    job_id = request.job_id or str(uuid7())
    logger.info(f"[Sync Job {job_id}] Received sync inference request (prompt: {request.prompt[:50] if request.prompt else 'None'}...)")
    
    # Return the image directly as a file response
    return FileResponse(
        path=IMAGE_PATH,
        media_type=IMAGE_MEDIA_TYPE,
        filename=f"{job_id}{IMAGE_PATH.suffix}",
        headers={
            "X-Job-ID": job_id,
            "X-Status": "completed"
        }
    )


@app.post("/edit")
async def submit_edit(request: EditRequest):
    """Async endpoint: Acknowledge edit job and dispatch callback."""
    if not IMAGE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Stub image not found at {IMAGE_PATH}",
        )

    job_id = request.job_id or f"edit-{str(uuid7())}"
    logger.info(f"[Edit Job {job_id}] Received edit request (prompt: {request.prompt[:50] if request.prompt else 'None'}..., seed: {request.seed})")
    
    # Apply callback URL override if configured
    actual_callback_url = request.callback_url
    if OVERRIDE_CALLBACK_URL and request.callback_url:
        logger.warning(f"[Edit Job {job_id}] ⚠️  OVERRIDING callback URL from {request.callback_url} to {OVERRIDE_CALLBACK_URL}")
        actual_callback_url = OVERRIDE_CALLBACK_URL
    elif OVERRIDE_CALLBACK_URL and not request.callback_url:
        logger.warning(f"[Edit Job {job_id}] ⚠️  No callback URL provided but override is set - using {OVERRIDE_CALLBACK_URL}")
        actual_callback_url = OVERRIDE_CALLBACK_URL
    
    if actual_callback_url:
        logger.info(f"[Edit Job {job_id}] Callback URL: {actual_callback_url}")
        if request.callback_secret:
            logger.info(f"[Edit Job {job_id}] Callback secret provided: {request.callback_secret[:8]}...{request.callback_secret[-4:] if len(request.callback_secret) > 12 else ''}")
        else:
            logger.info(f"[Edit Job {job_id}] No callback secret provided")
    else:
        logger.info(f"[Edit Job {job_id}] No callback URL provided")
    
    _edit_jobs[job_id] = {
        "prompt": request.prompt,
        "seed": request.seed,
        "callback_url": request.callback_url,
        "callback_secret": request.callback_secret,
    }
    job_payload = _build_edit_job_response(job_id)

    # Dispatch callback if callback_url is provided
    callback_result = None
    if actual_callback_url:
        callback_result = await _dispatch_callback(
            job_id=job_id,
            callback_url=actual_callback_url,
            image_path=IMAGE_PATH,
            callback_secret=request.callback_secret,
        )

    response_data = {
        "accepted": True,
        "job_id": job_id,
        "status": "completed",
        "message": "Stubbed edit job completed immediately",
        "status_url": f"/edit/status/{job_id}",
    }

    if callback_result:
        response_data["callback_result"] = callback_result

    return response_data


@app.get("/edit/status/{job_id}")
async def get_edit_status(job_id: str):
    """Mimic polling the status of an edit job."""
    if job_id not in _edit_jobs:
        raise HTTPException(status_code=404, detail=f"Edit job {job_id} not found")

    return _build_edit_job_response(job_id)


@app.post("/sync/edit")
async def submit_sync_edit(request: EditRequest):
    """Sync endpoint: Return edited image directly or timeout."""
    if not IMAGE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Stub image not found at {IMAGE_PATH}",
        )

    job_id = request.job_id or f"edit-{str(uuid7())}"
    logger.info(f"[Sync Edit Job {job_id}] Received sync edit request (prompt: {request.prompt[:50] if request.prompt else 'None'}..., seed: {request.seed})")
    
    # Return the image directly as a file response
    return FileResponse(
        path=IMAGE_PATH,
        media_type=IMAGE_MEDIA_TYPE,
        filename=f"{job_id}{IMAGE_PATH.suffix}",
        headers={
            "X-Job-ID": job_id,
            "X-Status": "completed",
            "X-Seed": str(request.seed)
        }
    )


@app.get("/")
async def root():
    return {"status": "ok", "detail": "Stub inference server is running"}


def main() -> None:
    """Allow running via ``python stub_miner_server.py``."""
    import uvicorn

    uvicorn.run(
        "dev_scaffolding.stub_miner_server:app",
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        reload=False,
    )


if __name__ == "__main__":
    main()
