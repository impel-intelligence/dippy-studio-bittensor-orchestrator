#!/usr/bin/env python3

import sys

import pytest

try:
    import pytest_cov  # noqa: F401
except ImportError:  # pragma: no cover
    pytest_cov = None


if __name__ == "__main__":
    args = ["orchestrator/tests", "-v"]

    if pytest_cov is not None:
        args.extend(["--cov=orchestrator", "--cov-report=term-missing"])
    else:
        print("pytest-cov not installed, running without coverage")

    sys.exit(pytest.main(args))
