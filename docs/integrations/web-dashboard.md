# Web Dashboard

PgQueuer ships a built-in web dashboard for queue insights and job management:
backlog age, throughput, execution-duration percentiles, failures with
tracebacks, worker liveness, schedules, and table health — plus requeue and
cancel actions. It is pure Python (FastAPI + [htmx](https://htmx.org), no
JavaScript build step) and reads the tables PgQueuer already maintains.
**No schema changes are required.**

## Installation

```bash
pip install "pgqueuer[web,asyncpg]"
```

## Running standalone

```bash
pgq web --host 0.0.0.0 --port 8080
```

Connection settings follow the same rules as every other `pgq` command:
`--pg-dsn`, `PGDSN`, or the standard libpq env vars (`PGHOST`, `PGUSER`, ...).

Screens:

| Route | Purpose |
| --- | --- |
| `/` | Headline stats: depth, in-flight, held-failed, workers, oldest queued age |
| `/entrypoints` | Per-entrypoint health: backlog age, p50/p95/p99 durations, failure rate, throughput sparkline |
| `/jobs` | Browse/filter queue rows; cancel queued or in-flight jobs |
| `/jobs/{id}` | Job detail: decoded payload, headers, full state-transition history |
| `/failures` | Held `failed` jobs (requeue-able) and the historical exception log |
| `/workers` | Active queue managers and stale picked jobs |
| `/schedules` | Cron schedules with overdue highlighting |
| `/system` | Table sizes, persistence modes, un-aggregated log backlog |
| `/healthz` | Liveness probe |

Live updates ride the `LISTEN/NOTIFY` channel PgQueuer already installs:
queue-table changes push a debounced server-sent event and the affected
screens re-render. Statistics-derived views (throughput, durations) refresh on
a timer instead, since the notify trigger only covers the queue table.

## Authentication

The dashboard is **open by default**, like comparable tools (River UI). It can
cancel and requeue jobs, so never expose it unauthenticated beyond localhost.
Enable HTTP Basic auth via environment variables:

```bash
export PGQUEUER_WEB_USER=admin
export PGQUEUER_WEB_PASSWORD=change-me
pgq web --host 0.0.0.0
```

## Embedding in your own FastAPI app

The router is mountable, so you can wrap it in your own auth middleware or
dependencies. Set `app.state.pgq_queries` in your lifespan:

```python
from contextlib import asynccontextmanager

import asyncpg
from fastapi import Depends, FastAPI

from pgqueuer.db import AsyncpgPoolDriver
from pgqueuer.queries import Queries
from pgqueuer.web import create_web_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(min_size=2)
    app.state.pgq_queries = Queries(AsyncpgPoolDriver(pool))
    yield
    await pool.close()


app = FastAPI(lifespan=lifespan)
app.include_router(
    create_web_router(dependencies=[Depends(my_auth)], include_sse=False),
    prefix="/pgqueuer",
)
```

With `include_sse=False` no broadcaster is required and live regions fall back
to their polling triggers. To keep server-sent events when embedding, start a
`pgqueuer.adapters.web.sse.Broadcaster` in your lifespan and store it as
`app.state.pgq_broadcaster`.

## Docker

A ready-made image definition lives at `tools/web/Dockerfile`, with a compose
example in `tools/web/docker-compose.yml`:

```bash
docker build -f tools/web/Dockerfile -t pgqueuer-dashboard .
docker run -e PGHOST=db -e PGUSER=... -p 8080:8080 pgqueuer-dashboard
```

## Reusing the insights API

The dashboard is one frontend over `pgqueuer.core.insights`, which any code
can consume — the same getters power scripts, exporters, and future UIs:

```python
from pgqueuer.core.insights import InsightsService, QueueManagementService

insights = InsightsService(queries)
snapshot = await insights.overview()
per_entrypoint = await insights.entrypoint_stats()

management = QueueManagementService(queries)
await management.requeue([job_id])
```

## Performance notes

- Duration percentiles are derived on the fly from `pgqueuer_log` state
  transitions. Queries are bounded to a selectable window (1h/6h/24h, capped
  at 24h) and served by the log table's `created` index.
- Jobs picked before the window start lose their pick/complete pair and are
  omitted from percentiles (undercounted, never miscounted).
- NOTIFY storms are debounced (250 ms) before fanning out to SSE clients, and
  the listener uses a dedicated connection so page renders are never starved.
