from orchestrator.common.payload_templates import (
    DEFAULT_NEGATIVE_PROMPT,
    build_img_h100_pcie_payload,
)


def test_build_img_h100_pcie_payload_defaults() -> None:
    payload = build_img_h100_pcie_payload(
        prompt="Draw a spaceship",
        image_url="https://example.com/image.png",
    )

    assert payload["prompt"] == "Draw a spaceship"
    assert payload["image_url"] == "https://example.com/image.png"
    assert payload["width"] == 1024
    assert payload["height"] == 1024
    assert payload["guidance_scale"] == 4.0
    assert payload["num_inference_steps"] == 10
    assert payload["negative_prompt"] == DEFAULT_NEGATIVE_PROMPT
    assert payload["enable_safety_checker"] is False
    assert "seed" not in payload
    assert "webhook_url" not in payload
    assert "callback_secret" not in payload


def test_build_img_h100_pcie_payload_with_overrides() -> None:
    payload = build_img_h100_pcie_payload(
        prompt="Hi",
        image_url="https://example.com/hero.png",
        seed=1234,
        width=512,
        height=768,
        guidance_scale=7.5,
        num_inference_steps=20,
        enable_safety_checker=True,
        webhook_url=" https://example.com/hook ",
        callback_secret="secret-value",
    )

    assert payload["seed"] == 1234
    assert payload["width"] == 512
    assert payload["height"] == 768
    assert payload["guidance_scale"] == 7.5
    assert payload["num_inference_steps"] == 20
    assert payload["enable_safety_checker"] is True
    assert payload["webhook_url"] == "https://example.com/hook"
    assert payload["callback_secret"] == "secret-value"
