# Deployment Patterns

PgQueuer requires no message broker, Redis instance, or additional infrastructure — just
PostgreSQL. This page covers how to run workers in production.

## Single Worker Process

The simplest deployment: one process, one `QueueManager`.

```python
# myapp.py
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def main() -> PgQueuer:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("process_order")
    async def process_order(job: Job) -> None:
        ...

    return pgq
```

```bash
pgq run myapp:main
```

This is sufficient for most workloads. A single `QueueManager` handles multiple entrypoints
concurrently via asyncio.

## Horizontal Scaling

Run multiple worker processes against the same PostgreSQL database. PgQueuer uses
`FOR UPDATE SKIP LOCKED` in every dequeue query, so workers never claim the same job:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Worker  1   │     │  Worker  2   │     │  Worker  3   │
│ QueueManager │     │ QueueManager │     │ QueueManager │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                            │
                     ┌──────┴──────┐
                     │ PostgreSQL  │
                     └─────────────┘
```

Each worker:
- Listens on the same `ch_pgqueuer` channel independently
- Claims jobs atomically — no coordinator or leader election needed
- Can run on separate VMs, containers, or processes on the same host

**Starting multiple workers:**

```bash
# Fly.io: scale to 3 machines
fly scale count 3

# Docker Compose: scale service
docker compose up --scale worker=3

# Local: multiple terminals or background processes
pgq run myapp:main &
pgq run myapp:main &
pgq run myapp:main
```

## One Process per CPU Core

For CPU-bound workloads, run one worker per core to avoid GIL contention in Python:

```bash
# Using a shell loop
for i in $(seq 1 $(nproc)); do
    pgq run myapp:main &
done
wait
```

For IO-bound workloads (HTTP calls, database queries), a single asyncio process typically
saturates a database connection before needing additional processes.

## Process Supervision

Use a supervisor to restart workers on crash.

**systemd:**

```ini
# /etc/systemd/system/pgqueuer-worker.service
[Unit]
Description=PgQueuer Worker
After=network.target postgresql.service

[Service]
User=app
WorkingDirectory=/app
ExecStart=pgq run myapp:main --shutdown-on-listener-failure
Restart=always
RestartSec=5
Environment=PGHOST=localhost
Environment=PGUSER=pgqueuer_app
Environment=PGDATABASE=mydb

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable --now pgqueuer-worker
```

**Docker / Docker Compose:**

```yaml
services:
  worker:
    image: myapp:latest
    command: pgq run myapp:main --shutdown-on-listener-failure
    restart: always
    environment:
      PGHOST: db
      PGUSER: pgqueuer_app
      PGDATABASE: mydb
    depends_on:
      db:
        condition: service_healthy
```

**Fly.io:**

Fly machines restart automatically on exit. Add `--shutdown-on-listener-failure` so a broken
LISTEN connection causes a clean restart rather than silent degradation:

```toml
# fly.toml
[processes]
  worker = "pgq run myapp:main --shutdown-on-listener-failure"
```

## Graceful Shutdown

`pgq run` handles `SIGTERM` and `SIGINT`. On receiving a signal:

1. Stop accepting new jobs from the queue.
2. Wait for in-flight jobs to complete (up to a configurable drain period).
3. Exit cleanly.

Container orchestrators (Kubernetes, Fly.io, ECS) send `SIGTERM` before `SIGKILL`, so
in-flight jobs finish provided the `terminationGracePeriodSeconds` is long enough.

!!! tip
    Set `terminationGracePeriodSeconds` to at least the 95th-percentile runtime of your
    longest job plus a 30-second buffer.

## Schema Migrations on Deploy

Run `pgq upgrade` before deploying new application code. It is safe to run against a live
database — migrations are additive and non-destructive:

```bash
# Deploy workflow
pgq upgrade          # apply schema changes
# then roll out new workers
```

`pgq upgrade` is idempotent: running it multiple times produces no side effects.

## Environment Configuration

PgQueuer reads standard PostgreSQL environment variables:

| Variable | Purpose |
|----------|---------|
| `PGHOST` | Database host |
| `PGPORT` | Database port (default `5432`) |
| `PGUSER` | Database user |
| `PGPASSWORD` | Database password |
| `PGDATABASE` | Database name |
| `PGQUEUER_PREFIX` | Prefix for table/channel names (default `pgqueuer`) |

Use `PGQUEUER_PREFIX` to run multiple isolated PgQueuer instances in the same database:

```bash
PGQUEUER_PREFIX=billing pgq install
PGQUEUER_PREFIX=billing pgq run billing_app:main
```

## Deployment Checklist

- [ ] `pgq install` run once against the target database
- [ ] `pgq autovac` applied after installation
- [ ] Application role granted [minimal runtime privileges](../reference/database-permissions.md)
- [ ] Workers started with `--shutdown-on-listener-failure`
- [ ] Supervisor configured to restart workers on exit
- [ ] `pgq upgrade` included in the deploy pipeline before rolling out new workers
- [ ] `terminationGracePeriodSeconds` (or equivalent) set to cover longest expected job runtime
