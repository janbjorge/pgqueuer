# 🚀 PGQueuer – PostgreSQL‑powered job queues for Python

[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain) [![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer) [![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer) [![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

[📚 Documentation](https://pgqueuer.readthedocs.io/en/latest/) · [💻 Source](https://github.com/janbjorge/pgqueuer/) · [💬 Discord](https://discord.gg/C7YMBzcRMQ)

## Overview

PGQueuer turns your PostgreSQL database into a fast, reliable background job processor. Jobs live in the same database as your application data, eliminating the need for external message brokers like Redis or RabbitMQ. Scale your job processing without adding new infrastructure.

Built on PostgreSQL's advanced concurrency features, PGQueuer uses `LISTEN/NOTIFY` for instant job notifications and `FOR UPDATE SKIP LOCKED` for efficient worker coordination. Its clean architecture supports everything from simple background tasks to complex workflows with rate limiting, deferred execution, and job tracking—all backed by your existing database.

## Key Features

### Core Capabilities
- 💡 **Minimal integration**: Single Python package—bring your existing PostgreSQL connection and start enqueueing jobs
- ⚛️ **PostgreSQL-powered concurrency**: Workers coordinate using `FOR UPDATE SKIP LOCKED` without stepping on each other
- 🚧 **Instant notifications**: `LISTEN/NOTIFY` wakes idle workers as soon as jobs arrive (with polling backup for robustness)
- 📦 **Clean architecture**: Built on ports and adapters pattern with support for multiple drivers (asyncpg, psycopg sync/async)

### Performance & Control
- 👨‍🎓 **Batch operations**: Enqueue or acknowledge thousands of jobs per round trip for maximum throughput
- 🎛️ **Rate limiting**: Control requests per second per entrypoint to respect external API limits
- 🔒 **Concurrency control**: Limit parallel execution and enable serialized dispatch for shared resources
- ⏰ **Deferred execution**: Schedule jobs to run at specific times with `execute_after`

### Production Ready
- ⏳ **Built-in scheduling**: Cron-like recurring tasks with no separate scheduler process
- 🛡️ **Graceful shutdown**: Clean worker termination with job completion guarantees
- 📊 **Real-time tracking**: Wait for job completion using `CompletionWatcher` with live status updates
- 🔧 **Observability**: Prometheus metrics, distributed tracing (Logfire, Sentry), and interactive dashboard

## Why PGQueuer?

PGQueuer is designed for teams who value simplicity and want to leverage PostgreSQL as their job queue infrastructure. If you're already running PostgreSQL, PGQueuer lets you add background job processing without introducing new services to deploy, monitor, or coordinate.

**Zero additional infrastructure**: Your jobs live in the same database as your application data, backed by ACID guarantees and familiar PostgreSQL tooling. No separate message broker to provision, scale, or keep in sync with your database.

**Real-time with PostgreSQL primitives**: `LISTEN/NOTIFY` delivers sub-second job latency without polling loops. Workers wake instantly when jobs arrive, and `FOR UPDATE SKIP LOCKED` coordinates parallel workers without contention.

**Built for modern Python**: First-class async/await support with clean shutdown semantics. Rate limiting, concurrency control, and scheduling are built in—not bolted on. Write entrypoints as regular async functions and let PGQueuer handle the orchestration.

**When PGQueuer shines**: Single database stack, microservices that share a database, applications where job data needs transactional consistency with business data, teams who prefer fewer moving parts over distributed systems complexity.

For a detailed comparison with Celery and other approaches, see [docs/celery-comparison.md](docs/celery-comparison.md).

## Installation

PGQueuer targets Python 3.11+ and PostgreSQL 12+. Install the package and initialize the database schema:

```bash
pip install pgqueuer
pgq install        # create tables and functions in your database
```

The CLI reads `PGHOST`, `PGUSER`, `PGDATABASE` and related environment variables. Use `pgq install --dry-run` to preview SQL, or `--prefix myapp_` to namespace tables. Run `pgq uninstall` to remove the schema when done.

## Quick Start

PGQueuer uses **consumers** (workers that process jobs) and **producers** (code that enqueues jobs). Here's how both sides work together.

### 1. Create a consumer

The consumer declares entrypoints (job handlers) and scheduled tasks. Each entrypoint corresponds to a job type that can be enqueued:

```python
from datetime import datetime

import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("fetch")
    async def process(job: Job) -> None:
        print(f"Processed: {job!r}")

    @pgq.schedule("every_minute", "* * * * *")
    async def every_minute(schedule: Schedule) -> None:
        print(f"Ran at {datetime.now():%H:%M:%S}")

    return pgq
```

Run the consumer with the CLI—it will start listening for work:

```bash
pgq run examples.consumer:main
```

### 2. Enqueue jobs

In a separate process (your web app, script, etc.), create a `Queries` object and enqueue jobs by entrypoint name:

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    queries = Queries(driver)

    # Enqueue a job for the "fetch" entrypoint
    await queries.enqueue("fetch", b"hello world")
```

The job arrives instantly via `LISTEN/NOTIFY`, and your consumer's `process` function handles it.

## Common Patterns

### Batch Operations

Enqueue thousands of jobs in a single database round trip:

```python
from pgqueuer.queries import Queries

# Enqueue 1000 jobs at once
await queries.enqueue(
    ["fetch"] * 1000,
    [f"payload_{i}".encode() for i in range(1000)],
    [0] * 1000,  # priorities
)
```

### Rate Limiting & Concurrency Control

Control execution frequency and parallelism per entrypoint:

```python
# Limit to 10 requests per second (useful for external APIs)
@pgq.entrypoint("api_calls", requests_per_second=10)
async def call_external_api(job: Job) -> None:
    await http_client.post("https://api.example.com", data=job.payload)

# Limit to 5 concurrent executions
@pgq.entrypoint("db_writes", concurrency_limit=5)
async def write_to_db(job: Job) -> None:
    await db.execute("INSERT INTO data VALUES (%s)", job.payload)

# Process jobs one at a time (serialized)
@pgq.entrypoint("ordered_processing", serialized_dispatch=True)
async def process_in_order(job: Job) -> None:
    await process_sequentially(job.payload)
```

### Deferred Execution

Schedule jobs to run at a specific time:

```python
from datetime import timedelta

# Execute 1 hour from now
await queries.enqueue(
    "send_reminder",
    payload=b"Meeting in 1 hour",
    execute_after=timedelta(hours=1),
)

# Execute at a specific timestamp
from datetime import datetime, timezone
execute_time = datetime(2024, 12, 25, 9, 0, tzinfo=timezone.utc)
await queries.enqueue(
    "send_greeting",
    payload=b"Merry Christmas!",
    execute_after=execute_time,
)
```

### Job Completion Tracking

Wait for jobs to finish and get their final status:

```python
from pgqueuer.completion import CompletionWatcher

# Enqueue a job
job_ids = await queries.enqueue("process_video", video_data)

# Wait for completion
async with CompletionWatcher(driver) as watcher:
    status = await watcher.wait_for(job_ids[0])
    print(f"Job finished with status: {status}")  # "successful", "exception", etc.

# Track multiple jobs concurrently
from asyncio import gather

image_ids = await queries.enqueue(["render_img"] * 20, [b"..."] * 20, [0] * 20)

async with CompletionWatcher(driver) as watcher:
    statuses = await gather(*[watcher.wait_for(jid) for jid in image_ids])
```

### Shared Resources

Initialize heavyweight objects once and inject them into all jobs:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def create_pgqueuer() -> PgQueuer:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)

    # Initialize shared resources (DB pools, HTTP clients, ML models, etc.)
    resources = {
        "db_pool": await asyncpg.create_pool(),
        "http_client": httpx.AsyncClient(),
        "feature_flags": {"beta_mode": True},
    }

    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("process_user")
    async def process_user(job: Job) -> None:
        ctx = pgq.qm.get_context(job.id)

        # Access shared resources
        pool = ctx.resources["db_pool"]
        http = ctx.resources["http_client"]
        flags = ctx.resources["feature_flags"]

        # Use them without recreating
        user_data = await http.get(f"https://api.example.com/users/{job.payload}")
        await pool.execute("INSERT INTO users VALUES ($1)", user_data)

    return pgq
```

## Web Framework Integration

### FastAPI

Integrate PGQueuer with FastAPI's lifespan context:

```python
from contextlib import asynccontextmanager
import asyncpg
from fastapi import Depends, FastAPI, Request
from pgqueuer.db import AsyncpgPoolDriver
from pgqueuer.queries import Queries

def get_queries(request: Request) -> Queries:
    return request.app.extra["queries"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with asyncpg.create_pool() as pool:
        app.extra["queries"] = Queries(AsyncpgPoolDriver(pool))
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/enqueue")
async def enqueue_job(
    payload: str,
    queries: Queries = Depends(get_queries),
):
    job_ids = await queries.enqueue("process_task", payload.encode())
    return {"job_ids": job_ids}
```

Full example: [examples/fastapi_usage.py](examples/fastapi_usage.py)

### Flask (Synchronous)

Use the synchronous driver for traditional WSGI apps:

```python
from flask import Flask, g, jsonify, request
import psycopg
from pgqueuer.db import SyncPsycopgDriver
from pgqueuer.queries import SyncQueries

app = Flask(__name__)

def get_driver():
    if "driver" not in g:
        conn = psycopg.connect(autocommit=True)
        g.driver = SyncPsycopgDriver(conn)
    return g.driver

@app.teardown_appcontext
def teardown_db(exception):
    driver = g.pop("driver", None)
    if driver:
        driver._connection.close()

@app.route("/enqueue", methods=["POST"])
def enqueue():
    queries = SyncQueries(get_driver())
    data = request.get_json()

    job_ids = queries.enqueue(
        data["entrypoint"],
        data["payload"].encode(),
        data.get("priority", 0),
    )
    return jsonify({"job_ids": job_ids})
```

Full example: [examples/flask_sync_usage.py](examples/flask_sync_usage.py)

## Scheduling

Define recurring tasks with cron-style expressions:

```python
from pgqueuer.models import Schedule

# Run daily at midnight
@pgq.schedule("cleanup", "0 0 * * *")
async def cleanup(schedule: Schedule) -> None:
    await perform_cleanup()
    print(f"Cleanup completed at {schedule.last_execution}")

# Run every 5 minutes
@pgq.schedule("sync_data", "*/5 * * * *")
async def sync_data(schedule: Schedule) -> None:
    await sync_with_external_service()

# Run every weekday at 9 AM
@pgq.schedule("morning_report", "0 9 * * 1-5")
async def morning_report(schedule: Schedule) -> None:
    await generate_and_send_report()
```

Schedules are stored in PostgreSQL and survive restarts. For schedule-only workers (no job processing), use `SchedulerManager` directly—see [examples/scheduler.py](examples/scheduler.py).

## Advanced Features

PGQueuer includes many advanced capabilities for production use:

- **Custom Executors**: Implement retry strategies with exponential backoff → [docs/pgqueuer.md#custom-executors](docs/pgqueuer.md#custom-job-executors)
- **Distributed Tracing**: Integrate with Logfire, Sentry for request tracing → [docs/tracing.md](docs/tracing.md)
- **Prometheus Metrics**: Export queue depth, latency, throughput metrics → [docs/prometheus-metrics-service.md](docs/prometheus-metrics-service.md)
- **Job Cancellation**: Mark jobs for cancellation with PostgreSQL NOTIFY → [docs/pgqueuer.md#job-cancellation](docs/pgqueuer.md#job-cancellation)
- **Heartbeat Monitoring**: Keep long-running jobs alive with periodic updates → [docs/pgqueuer.md#automatic-heartbeat](docs/pgqueuer.md#automatic-heartbeat)

## Drivers

PGQueuer works with multiple PostgreSQL drivers:

**Async drivers** (for workers and enqueueing):
- **AsyncpgDriver** – single `asyncpg` connection
- **AsyncpgPoolDriver** – `asyncpg` connection pool (recommended for high throughput)
- **PsycopgDriver** – psycopg 3 async interface

**Sync driver** (enqueue-only):
- **SyncPsycopgDriver** – blocking psycopg connection for traditional web apps

Example with connection pool:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgPoolDriver

pool = await asyncpg.create_pool()
driver = AsyncpgPoolDriver(pool)
pgq = PgQueuer(driver)
```

See [docs/driver.md](docs/driver.md) for detailed driver documentation.

## CLI Tools

PGQueuer includes a command-line interface for common operations:

```bash
# Setup and migration
pgq install                         # Install schema
pgq install --prefix myapp_         # Install with table prefix
pgq install --dry-run               # Preview SQL without executing
pgq upgrade                         # Migrate schema to latest version
pgq uninstall                       # Remove schema

# Running workers
pgq run examples.consumer:main      # Start worker from Python callable

# Monitoring
pgq dashboard                       # Interactive terminal dashboard
pgq dashboard --interval 10         # Refresh every 10 seconds
pgq dashboard --tail 25             # Show 25 most recent jobs
```

## Monitor Your Queues

Launch the interactive dashboard to watch queue activity in real time:

```bash
pgq dashboard --interval 10 --tail 25 --table-format grid
```

Example output:

```text
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:27+00:00 |  12   |   fetch    |         0:00:03          | queued     |    0     |
| 2024-05-05 16:44:28+00:00 |   3   |  api_call  |         0:00:00          | picked     |    5     |
+---------------------------+-------+------------+--------------------------+------------+----------+
```

The dashboard shows queue depth, processing times, job statuses, and priorities. See [docs/dashboard.md](docs/dashboard.md) for more options.

## Documentation

| Topic | Description |
|-------|-------------|
| [Architecture & Design](docs/architecture.md) | Clean architecture, ports and adapters, design decisions |
| [Core Features Guide](docs/pgqueuer.md) | Shared resources, executors, cancellation, scheduling, tracking |
| [Driver Selection](docs/driver.md) | Choosing and configuring asyncpg, psycopg, sync drivers |
| [Celery Comparison](docs/celery-comparison.md) | Side-by-side examples vs Celery |
| [Distributed Tracing](docs/tracing.md) | Logfire and Sentry integration |
| [Prometheus Metrics](docs/prometheus-metrics-service.md) | Exposing queue metrics for monitoring |
| [Dashboard](docs/dashboard.md) | CLI dashboard options and usage |

## Development and Testing

PGQueuer uses [Testcontainers](https://testcontainers.com/?language=python) to launch an ephemeral PostgreSQL instance automatically for the integration test suite—no manual Docker Compose setup or pre‑provisioned database required. Just ensure Docker (or another supported container runtime) is running locally.

Typical development workflow:

1. Install dependencies (including extras): `uv sync --all-extras`
2. Run lint & type checks: `uv run ruff check .` and `uv run mypy .`
3. Run the test suite (will start/stop a disposable PostgreSQL container automatically): `uv run pytest`
4. (Optional) Aggregate target (if you prefer the Makefile): `make check`

The containerized database lifecycle is fully automatic; tests handle creation, migrations, and teardown. This keeps your local environment clean and ensures consistent, isolated runs.

## License

PGQueuer is MIT licensed. See [LICENSE](LICENSE) for details.

---
Ready to supercharge your workflows? Install PGQueuer today and start processing jobs with the database you already trust.
