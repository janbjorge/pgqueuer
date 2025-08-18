# 🚀 PGQueuer – PostgreSQL‑powered job queues for Python

[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain) [![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer) [![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer) [![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

[📚 Documentation](https://pgqueuer.readthedocs.io/en/latest/) · [💻 Source](https://github.com/janbjorge/pgqueuer/) · [💬 Discord](https://discord.gg/C7YMBzcRMQ)

PGQueuer turns your PostgreSQL database into a fast, reliable background job processor. Jobs live in the same database as your application data, so you scale without adding new infrastructure. PGQueuer uses PostgreSQL features like `LISTEN/NOTIFY` and `FOR UPDATE SKIP LOCKED` to keep workers coordinated and throughput high.

## Features

- 💡 **Minimal integration**: PGQueuer is a single Python package. Bring your existing PostgreSQL connection and start enqueueing jobs—no extra services to deploy.
- ⚛️ **PostgreSQL-powered concurrency**: Workers lease jobs with `FOR UPDATE SKIP LOCKED`, allowing many processes to pull from the same queue without stepping on each other.
- 🚧 **Instant notifications**: `LISTEN/NOTIFY` wakes idle workers as soon as a job arrives. A periodic poll backs it up for robustness.
- 👨‍🎓 **Batch friendly**: Designed for throughput; enqueue or acknowledge thousands of jobs per round trip.
- ⏳ **Scheduling & graceful shutdown**: Register cron-like recurring jobs and let PGQueuer stop workers cleanly when your service exits.

## Installation

PGQueuer targets Python 3.11+ and any PostgreSQL 12+ server. Install the package and initialize the database schema with the CLI:

```bash
pip install pgqueuer
pgq install        # create tables and functions in your database
```

The CLI reads `PGHOST`, `PGUSER`, `PGDATABASE` and friends to know where to install. Use `pgq install --dry-run` to preview SQL or `pgq uninstall` to remove the structure when you're done experimenting.

## Quick Start

The API revolves around **consumers** that process jobs and **producers** that enqueue them. Follow the steps below to see both sides.

### 1. Create a consumer

The consumer declares entrypoints and scheduled tasks. PGQueuer wires these up to the worker process that polls for jobs.

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

Run the consumer with the CLI so it begins listening for work:

```bash
pgq run examples.consumer:main
```

### 2. Enqueue jobs

In a separate process, create a `Queries` object and enqueue jobs targeted at your entrypoint:

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    q = Queries(driver)
    await q.enqueue(["fetch"], [b"hello world"], [0])
```

Run the producer:

```bash
python examples/producer.py
```

## Monitor your queues

```bash
pgq dashboard --interval 10 --tail 25 --table-format grid
```

The dashboard is an interactive terminal UI that refreshes periodically. Use it to watch queue depth, processing times and job statuses in real time.

Example output:

```text
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
...
+---------------------------+-------+------------+--------------------------+------------+----------+
```

## Drivers

PGQueuer works with both async and sync PostgreSQL drivers:

- **AsyncpgDriver** / **AsyncpgPoolDriver** – integrate with [`asyncpg`](https://github.com/MagicStack/asyncpg) connections or pools for asyncio applications.
- **PsycopgDriver** – built on psycopg's async interface for apps already using psycopg 3.
- **SyncPsycopgDriver** – a thin wrapper around psycopg's blocking API so traditional scripts can enqueue work.

See the [driver documentation](docs/driver.md) for details.

## Development and Testing

PGQueuer uses [Testcontainers](https://testcontainers.com/?language=python) to launch an ephemeral PostgreSQL instance automatically for the integration test suite—no manual Docker Compose setup or pre‑provisioned database required. Just ensure Docker (or another supported container runtime) is running locally.

Typical development workflow:

1. Install dependencies (including extras): `uv sync --all-extras`
2. Run lint & type checks: `uv ruff check .` and `uv run mypy .`
3. Run the test suite (will start/stop a disposable PostgreSQL container automatically): `uv run pytest`
4. (Optional) Aggregate target (if you prefer the Makefile): `make check`

The containerized database lifecycle is fully automatic; tests handle creation, migrations, and teardown. This keeps your local environment clean and ensures consistent, isolated runs.

## License

PGQueuer is MIT licensed. See [LICENSE](LICENSE) for details.

---
Ready to supercharge your workflows? Install PGQueuer today and start processing jobs with the database you already trust.
