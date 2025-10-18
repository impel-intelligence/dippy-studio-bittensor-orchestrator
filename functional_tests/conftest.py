from __future__ import annotations

import os
from typing import Generator, Optional, cast

import pytest

from .client import ApiClient
def pytest_addoption(parser: pytest.Parser) -> None:  # noqa: D401
    """Register custom command-line options for functional tests."""

    parser.addoption(
        "--base-url",
        action="store",
        default=os.getenv("API_BASE_URL", "http://localhost:8000"),
        help="Base URL for the API under test (default: env API_BASE_URL or http://localhost:8000)",
    )

@pytest.fixture(scope="session")
def base_url(request: pytest.FixtureRequest) -> str:  # noqa: D401
    """Return the base URL for the API under test, resolved from CLI or env."""

    return cast(str, request.config.getoption("--base-url"))


@pytest.fixture(scope="session")
def api_client(base_url: str) -> Generator[ApiClient, None, None]:  # noqa: D401
    """Provide an :class:`~functional_tests.client.ApiClient` instance."""

    client = ApiClient(base_url)
    yield client
