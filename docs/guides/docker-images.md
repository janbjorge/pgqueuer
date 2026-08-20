# Docker Images

Pre-built, multi-arch (`linux/amd64` + `linux/arm64`) images for the web dashboard
and the standalone Prometheus exporter are published to the GitHub Container
Registry on every release, tagged to match the PyPI version:

| Image | Source | Tags |
|---|---|---|
| `ghcr.io/janbjorge/pgqueuer-web` | `tools/web/Dockerfile` | `vX.Y.Z`, `latest` |
| `ghcr.io/janbjorge/pgqueuer-prometheus` | `tools/prometheus/Dockerfile` | `vX.Y.Z`, `latest` |

Pin to a version tag in production; `latest` tracks the newest non-prerelease.

## Web dashboard

```bash
docker run -p 8080:8080 \
  -e PGHOST=your-postgres-host \
  -e PGUSER=your-username \
  -e PGPASSWORD=your-password \
  -e PGDATABASE=your-database \
  -e PGQUEUER_WEB_USER=admin \
  -e PGQUEUER_WEB_PASSWORD=change-me \
  ghcr.io/janbjorge/pgqueuer-web:latest
```

Connection settings follow the same rules as `pgq web`: either `PGQUEUER_DSN`
(or `PGDSN`), or the standard libpq variables (`PGHOST`, `PGUSER`,
`PGPASSWORD`, `PGDATABASE`). `PGQUEUER_WEB_HOST` / `PGQUEUER_WEB_PORT` are
already set in the image (`0.0.0.0:8080`); override only if you need a
different bind.

`PGQUEUER_WEB_USER` / `PGQUEUER_WEB_PASSWORD` are **not** set by the image.
Without them the dashboard runs unauthenticated. It can cancel and requeue
jobs, so never publish it without setting both. See
[Web Dashboard → Authentication](../integrations/web-dashboard.md#authentication).

## Prometheus exporter

```bash
docker run -p 8000:8000 \
  -e PGHOST=your-postgres-host \
  -e PGUSER=your-username \
  -e PGPASSWORD=your-password \
  -e PGDATABASE=your-database \
  ghcr.io/janbjorge/pgqueuer-prometheus:latest
```

Metrics are served at `http://localhost:8000/metrics`.

!!! warning
    This exporter connects with a bare `asyncpg.connect()`: it reads only
    `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGDATABASE`.
    `PGQUEUER_DSN` / `PGDSN` are **not** read here, unlike the web dashboard
    and every `pgq` CLI command. Setting only a DSN for this image will fail
    to connect.

## Docker Compose

```yaml
services:
  db:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: pgqueuer
      POSTGRES_PASSWORD: pgqueuer
      POSTGRES_DB: pgqueuer

  web:
    image: ghcr.io/janbjorge/pgqueuer-web:latest
    ports:
      - "8080:8080"
    environment:
      PGHOST: db
      PGUSER: pgqueuer
      PGPASSWORD: pgqueuer
      PGDATABASE: pgqueuer
      PGQUEUER_WEB_USER: admin
      PGQUEUER_WEB_PASSWORD: change-me
    depends_on:
      - db

  prometheus-exporter:
    image: ghcr.io/janbjorge/pgqueuer-prometheus:latest
    ports:
      - "8000:8000"
    environment:
      PGHOST: db
      PGUSER: pgqueuer
      PGPASSWORD: pgqueuer
      PGDATABASE: pgqueuer
    depends_on:
      - db
```

A working example lives at `tools/web/docker-compose.yml`.

## Building locally

Both images build from the repo without registry access, useful for testing
Dockerfile changes before a release:

```bash
docker build -f tools/web/Dockerfile -t pgqueuer-web .
docker build -f tools/prometheus/Dockerfile -t pgqueuer-prometheus .
```

## Multi-arch builds in CI

Images are built for `linux/amd64` and `linux/arm64` via `docker/build-push-action`
with QEMU emulation in `.github/workflows/release.yml`, triggered on the same
`release: created` event as the PyPI publish step.
