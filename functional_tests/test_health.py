from .client import ApiClient


def test_health_endpoint(api_client: ApiClient) -> None:
    """The service should respond with 200 OK on /health."""

    resp = api_client.health()
    assert resp.status_code == 200, resp.text
