FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY jobrelay/requirements.jobrelay.txt ./requirements.jobrelay.txt
RUN pip install --upgrade pip && pip install -r requirements.jobrelay.txt

COPY jobrelay/ ./jobrelay/

ENV PYTHONPATH=/app

RUN mkdir -p /var/lib/jobrelay
ENV JOBRELAY_DUCKDB_PATH=/var/lib/jobrelay/jobrelay.duckdb

CMD ["uvicorn", "jobrelay.app:app", "--host", "0.0.0.0", "--port", "8181"]
