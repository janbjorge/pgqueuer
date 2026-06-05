# 🚀 PGQueuer – PostgreSQL‑powered job queues for Python

[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain) [![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer) [![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer) [![stars](https://img.shields.io/github/stars/janbjorge/pgqueuer?style=flat)](https://github.com/janbjorge/pgqueuer/stargazers) [![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

[📚 Docs](https://janbjorge.github.io/pgqueuer/) · [💻 Source](https://github.com/janbjorge/pgqueuer/) · [💬 Discord](https://discord.gg/C7YMBzcRMQ)

**Your PostgreSQL database is already a job queue.**

PGQueuer turns PostgreSQL into a fast, reliable background job processor. Jobs live in the same database as your application data — one stack, full ACID guarantees, and no separate message broker to deploy, scale, or keep in sync.

## Features

- 💡 **Minimal footprint** – one `pip install`; bring your existing PostgreSQL connection and start enqueueing
- 🔁 **Transactional enqueue** – commit a job in the same transaction as your data; no dual-write drift
- ⚛️ **True concurrency** – workers claim jobs with `FOR UPDATE SKIP LOCKED`, never double-processing
- 🚀 **Instant dispatch** – `LISTEN/NOTIFY` wakes workers the moment a job lands (with a polling fallback)
- ⏰ **Scheduling & deferral** – cron-style recurring tasks and `execute_after`, no separate scheduler process
- 🔒 **Concurrency control** – per-entrypoint limits and serialized dispatch for shared resources
- 📊 **Observability** – completion tracking, Prometheus metrics, tracing (Logfire/Sentry), and a live dashboard
- 🧪 **In-memory mode** – run the whole queue without Postgres for tests and prototyping

## Why PostgreSQL?

If you already run PostgreSQL, it can do double duty as your job queue:

- **One fewer service** to provision, monitor, and keep available
- **Transactional enqueuing** – commit a job in the same transaction as your application data
- **Consistent state** – your queue and your data always agree because they share one database
- **Lower latency** – jobs stay local, no round-trip to an external broker

```text
┌──────────┐  enqueue   ┌────────────┐  NOTIFY   ┌──────────┐
│ Your App │───────────▶│            │──────────▶│ Worker 1 │──┐
└──────────┘            │            │           └──────────┘  │
                        │ PostgreSQL │  NOTIFY   ┌──────────┐  │
                        │            │──────────▶│ Worker 2 │──┤
                        │            │           └──────────┘  │
                        │            │  NOTIFY   ┌──────────┐  │
                        │            │──────────▶│ Worker N │──┤
                        └────────────┘           └──────────┘  │
                              ▲  FOR UPDATE SKIP LOCKED         │
                              └─────────────────────────────────┘
```

## Installation

PGQueuer targets Python 3.11+ and PostgreSQL 12+:

```bash
pip install pgqueuer
pgq install        # create tables and functions in your database
```

The CLI reads `PGHOST`, `PGUSER`, `PGDATABASE` and related environment variables. Use `pgq install --dry-run` to preview SQL, `--prefix myapp_` to namespace tables, or `pgq uninstall` to remove the schema.

## Quick Start

PGQueuer pairs **consumers** (workers that process jobs) with **producers** (code that enqueues jobs).

### 1. Define a consumer

Each entrypoint is a job handler. Run it with the CLI: `pgq run examples.consumer:main`.

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def main() -> PgQueuer:
    connection = await asyncpg.connect()
    pgq = PgQueuer(AsyncpgDriver(connection))

    @pgq.entrypoint("fetch")
    async def process(job: Job) -> None:
        print(f"Processed: {job!r}")

    return pgq
```

### 2. Enqueue jobs

From your web app, script, or anywhere else with a database connection:

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main() -> None:
    connection = await asyncpg.connect()
    queries = Queries(AsyncpgDriver(connection))
    await queries.enqueue("fetch", b"hello world")
```

The job arrives instantly via `LISTEN/NOTIFY`, and your consumer's `process` handler picks it up.

### Enqueue inside a transaction

This is what a database-backed queue buys you: the job and your business data commit together, or not at all.

```python
async with connection.transaction():
    await connection.execute(
        "INSERT INTO orders (id, status) VALUES ($1, 'paid')", order_id
    )
    await queries.enqueue("send_receipt", str(order_id).encode())
    # If the transaction rolls back, the job is never enqueued — no orphaned work.
```

## Run without a database

`PgQueuer.in_memory()` is a drop-in replacement that implements the same ports as the real backend, so your handlers stay identical — ideal for unit tests and prototyping.

```python
import asyncio
from pgqueuer import PgQueuer
from pgqueuer.models import Job
from pgqueuer.domain.types import QueueExecutionMode

async def main() -> None:
    pq = PgQueuer.in_memory()

    @pq.entrypoint("send_email")
    async def send_email(job: Job) -> None:
        print(f"Sending: {job.payload!r}")

    await pq.qm.queries.enqueue(["send_email"], [b"alice"], [0])
    await pq.qm.run(mode=QueueExecutionMode.drain)

asyncio.run(main())
```

The in-memory adapter has no durability or multi-process coordination — use the PostgreSQL backend for production. See the [in-memory reference](https://janbjorge.github.io/pgqueuer/reference/in-memory/).

## Documentation

| Topic | What's inside |
|-------|---------------|
| [Core concepts](docs/getting-started/core-concepts.md) | Consumers, producers, entrypoints, the job lifecycle |
| [Scheduling](docs/guides/scheduling.md) | Cron-style recurring tasks and deferred execution |
| [Concurrency control](docs/guides/concurrency-control.md) | Per-entrypoint limits and serialized dispatch |
| [Completion tracking](docs/guides/completion-tracking.md) | Wait for jobs to finish with `CompletionWatcher` |
| [Shared resources](docs/guides/shared-resources.md) | Inject DB pools, HTTP clients, and models into handlers |
| [Custom executors](docs/guides/custom-executors.md) | Retry strategies and exponential backoff |
| [Drivers](docs/reference/drivers.md) | asyncpg, psycopg async/sync — choosing and configuring |
| [Architecture](docs/reference/architecture.md) | Ports & adapters, `SKIP LOCKED`, design decisions |
| [Observability](docs/integrations/prometheus.md) | Prometheus metrics, [tracing](docs/integrations/tracing.md), and the dashboard |
| [Framework integration](examples/) | FastAPI ([example](examples/fastapi_usage.py)) and Flask ([example](examples/flask_sync_usage.py)) |
| [Celery comparison](docs/comparisons/celery-comparison.md) | How PGQueuer differs from broker-based queues |

## Monitor your queues

Launch the interactive dashboard to watch queue activity in real time:

```bash
pgq dashboard --interval 10 --tail 25
```

```text
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:27+00:00 |  12   |   fetch    |         0:00:03          | queued     |    0     |
| 2024-05-05 16:44:28+00:00 |   3   |  api_call  |         0:00:00          | picked     |    5     |
+---------------------------+-------+------------+--------------------------+------------+----------+
```

## Development

PGQueuer uses [Testcontainers](https://testcontainers.com/?language=python) to spin up an ephemeral PostgreSQL instance for the test suite — just have Docker running.

```bash
uv sync --all-extras      # install dependencies
make check                # lint, type-check, and run the test suite
```

## License

PGQueuer is MIT licensed. See [LICENSE](LICENSE) for details.
