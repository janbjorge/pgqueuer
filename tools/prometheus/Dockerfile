# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.13-alpine AS builder
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache-dir "pgqueuer[asyncpg]" fastapi uvicorn
COPY tools/prometheus /app/prometheus

FROM python:3.13-alpine
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=builder /usr/local /usr/local
COPY --from=builder /app /app
RUN addgroup -S app && adduser -S app -G app
USER app
EXPOSE 8000
CMD ["uvicorn", "--factory", "prometheus.prometheus:create_app", "--host", "0.0.0.0", "--port", "8000"]
