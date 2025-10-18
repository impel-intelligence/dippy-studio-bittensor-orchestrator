FROM ghcr.io/astral-sh/uv:bookworm-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_CACHE_DIR=/tmp/uv-cache

ENV UV_COMPILE_BYTECODE=1 
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_PREFERENCE=only-managed
ENV UV_PYTHON_INSTALL_DIR=/opt/python

RUN uv python install 3.11

WORKDIR /app

COPY requirements.orchestrator.txt ./
COPY pyproject.toml ./
COPY README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -r requirements.orchestrator.txt

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install \
    fastapi[all] \
    uvicorn[standard] \
    pytest \
    pytest-asyncio \
    black \
    isort \
    mypy \
    ruff

COPY orchestrator/ ./orchestrator/
COPY epistula/ ./epistula/
COPY jobrelay/ ./jobrelay/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -e .

RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /var/lib/orchestrator && chown -R appuser:appuser /var/lib/orchestrator
USER appuser

EXPOSE 42069

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:42069/docs || exit 1

CMD ["python", "-m", "orchestrator.server"]
