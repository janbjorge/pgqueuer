# --- Stage 1: build dependencies and the PGQueuer package
FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS builder
WORKDIR /src
COPY . /src
RUN uv pip install --no-cache-dir .

# --- Stage 2: minimal runtime image
FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /usr/local /usr/local
ENTRYPOINT ["pgq", "run"]
