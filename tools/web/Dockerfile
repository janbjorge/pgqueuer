# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.13-alpine AS builder
ARG PGQUEUER_VERSION=
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache-dir "pgqueuer[web,asyncpg]${PGQUEUER_VERSION:+==${PGQUEUER_VERSION}}"

FROM python:3.13-alpine
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PGQUEUER_WEB_HOST=0.0.0.0 \
    PGQUEUER_WEB_PORT=8080
WORKDIR /app
COPY --from=builder /usr/local /usr/local
RUN addgroup -S app && adduser -S app -G app
USER app
EXPOSE 8080
CMD ["pgq", "web"]
