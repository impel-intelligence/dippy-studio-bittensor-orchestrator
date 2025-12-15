FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY ss58/requirements.ss58.txt ./requirements.ss58.txt
RUN pip install --upgrade pip && pip install -r requirements.ss58.txt

COPY ss58/ ./ss58/

ENV PYTHONPATH=/app
ENV SS58_PORT=8585

EXPOSE 8585

RUN mkdir -p /var/lib/ss58

CMD ["uvicorn", "ss58.app:app", "--host", "0.0.0.0", "--port", "8585"]
