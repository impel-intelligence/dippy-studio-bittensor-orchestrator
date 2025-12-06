from __future__ import annotations

from typing import Any, Dict, Optional

DEFAULT_NEGATIVE_PROMPT = (
    "bad quality, worst quality, text, signature, watermark, extra limbs, extra legs, extra arms, "
    "child, anime, watercolours, cartoonish, overexposed, nudity, explicit content"
)


def build_img_h100_pcie_payload(
    *,
    prompt: str,
    image_url: str,
    negative_prompt: str = DEFAULT_NEGATIVE_PROMPT,
    width: int = 1024,
    height: int = 1024,
    guidance_scale: float = 4.0,
    num_inference_steps: int = 10,
    seed: Optional[int] = None,
    enable_safety_checker: bool = False,
    webhook_url: str | None = None,
    callback_secret: str | None = None,
) -> Dict[str, Any]:
    """Return a payload ready for img-h100_pcie jobs."""
    payload: Dict[str, Any] = {
        "prompt": str(prompt),
        "negative_prompt": str(negative_prompt),
        "image_url": str(image_url),
        "width": max(1, int(width)),
        "height": max(1, int(height)),
        "guidance_scale": float(guidance_scale),
        "num_inference_steps": max(1, int(num_inference_steps)),
        "enable_safety_checker": bool(enable_safety_checker),
    }

    if seed is not None:
        try:
            numeric_seed = int(seed)
        except (TypeError, ValueError):
            numeric_seed = None
        if numeric_seed is not None and numeric_seed >= 0:
            payload["seed"] = numeric_seed

    if webhook_url:
        payload["webhook_url"] = webhook_url.strip()
    if callback_secret:
        payload["callback_secret"] = callback_secret

    return payload


__all__ = ["build_img_h100_pcie_payload", "DEFAULT_NEGATIVE_PROMPT"]
