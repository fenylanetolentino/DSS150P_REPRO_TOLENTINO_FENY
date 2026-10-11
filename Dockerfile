# syntax=docker/dockerfile:1
FROM python:3.12.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=120 --retries 10 -r requirements.txt

COPY config ./config
COPY metadata ./metadata
COPY src ./src
COPY sql ./sql
COPY tests ./tests

CMD ["python", "-m", "src.cli", "--help"]