FROM ghcr.io/astral-sh/uv:bookworm-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
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

COPY requirements.forwarder.txt ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -r requirements.forwarder.txt

COPY forwarder/ ./forwarder/

ENV PYTHONPATH=/app

RUN useradd --create-home --shell /bin/bash appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /home/appuser/.cache/uv && \
    chown -R appuser:appuser /home/appuser/.cache
USER appuser

ENV UV_CACHE_DIR=/home/appuser/.cache/uv
ENV FORWARDER_PORT=9871

EXPOSE 9871

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD sh -c 'curl -f http://localhost:${FORWARDER_PORT:-9871}/docs || exit 1'

CMD ["python", "-m", "forwarder.server"]
