# Docker Images

The repository ships Dockerfiles for the web dashboard and the standalone
Prometheus exporter. Releases no longer publish them, so build them yourself:

| Image | Dockerfile |
|---|---|
| Web dashboard | `tools/web/Dockerfile` |
| Prometheus exporter | `tools/prometheus/Dockerfile` |

The images published up to v1.5.0 stay on the GitHub Container Registry as
`ghcr.io/janbjorge/pgqueuer-web` and `ghcr.io/janbjorge/pgqueuer-prometheus`,
tagged `1.5.0` and `latest`, but get no newer versions.

## Building

From the repository root:

```bash
docker build -f tools/web/Dockerfile -t pgqueuer-web .
docker build -f tools/prometheus/Dockerfile -t pgqueuer-prometheus .
```

Both install `pgqueuer` from PyPI, the newest release unless you pin one with
`--build-arg PGQUEUER_VERSION=X.Y.Z`.

## Web dashboard

```bash
docker run -p 8080:8080 \
  -e PGHOST=your-postgres-host \
  -e PGUSER=your-username \
  -e PGPASSWORD=your-password \
  -e PGDATABASE=your-database \
  -e PGQUEUER_WEB_USER=admin \
  -e PGQUEUER_WEB_PASSWORD=change-me \
  pgqueuer-web
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
  pgqueuer-prometheus
```

Metrics are served at `http://localhost:8000/metrics`.

!!! warning
    This exporter connects with a bare `asyncpg.connect()`: it reads only
    `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGDATABASE`.
    `PGQUEUER_DSN` / `PGDSN` are **not** read here, unlike the web dashboard
    and every `pgq` CLI command. Setting only a DSN for this image will fail
    to connect.

## Docker Compose

With the compose file at the repository root:

```yaml
services:
  db:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: pgqueuer
      POSTGRES_PASSWORD: pgqueuer
      POSTGRES_DB: pgqueuer

  web:
    build:
      context: .
      dockerfile: tools/web/Dockerfile
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
    build:
      context: .
      dockerfile: tools/prometheus/Dockerfile
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
