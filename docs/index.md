# PgQueuer

> High-performance PostgreSQL job queue for Python.
> No Redis. No RabbitMQ. Just PostgreSQL.

---

## Install

=== "asyncpg"

    ```bash
    pip install pgqueuer[asyncpg]
    ```

=== "psycopg"

    ```bash
    pip install pgqueuer[psycopg]
    ```

=== "uv"

    ```bash
    uv add pgqueuer[asyncpg]
    ```

Then set up the database schema:

```bash
pgq install
```

!!! tip "Optional extras"

    | Extra | Purpose |
    |-------|---------|
    | `asyncpg` | asyncpg async driver |
    | `psycopg` | psycopg async + sync driver |
    | `logfire` | Logfire distributed tracing |
    | `sentry` | Sentry distributed tracing |
    | `fastapi` | FastAPI Prometheus metrics router |

---

## Quick Start

### 1. Define a consumer

Create `myapp.py`:

=== "asyncpg"

    ```python
    from datetime import datetime
    import asyncpg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job, Schedule

    async def main() -> PgQueuer:
        connection = await asyncpg.connect()
        pgq = PgQueuer.from_asyncpg_connection(connection)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed: {job!r}")

        @pgq.schedule("cleanup", "0 * * * *")
        async def cleanup(schedule: Schedule) -> None:
            print(f"Hourly cleanup: {datetime.now()!r}")

        return pgq
    ```

=== "asyncpg pool"

    ```python
    from datetime import datetime
    import asyncpg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job, Schedule

    async def main() -> PgQueuer:
        pool = await asyncpg.create_pool()
        pgq = PgQueuer.from_asyncpg_pool(pool)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed: {job!r}")

        @pgq.schedule("cleanup", "0 * * * *")
        async def cleanup(schedule: Schedule) -> None:
            print(f"Hourly cleanup: {datetime.now()!r}")

        return pgq
    ```

=== "psycopg"

    ```python
    from datetime import datetime
    import psycopg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job, Schedule

    async def main() -> PgQueuer:
        connection = await psycopg.AsyncConnection.connect(autocommit=True)
        pgq = PgQueuer.from_psycopg_connection(connection)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed: {job!r}")

        @pgq.schedule("cleanup", "0 * * * *")
        async def cleanup(schedule: Schedule) -> None:
            print(f"Hourly cleanup: {datetime.now()!r}")

        return pgq
    ```

=== "In-Memory (testing)"

    ```python
    from pgqueuer import PgQueuer
    from pgqueuer.domain.models import Job
    from pgqueuer.domain.types import QueueExecutionMode

    async def test_job_handler():
        pq = PgQueuer.in_memory()

        @pq.entrypoint("say_hello")
        async def say_hello(job: Job) -> None:
            print(f"Processing: {job.payload}")

        await pq.qm.queries.enqueue(["say_hello"], [b"hello"], [0])
        await pq.qm.run(mode=QueueExecutionMode.drain)
    ```

!!! note
    psycopg connections must use `autocommit=True`.

### 2. Run the consumer

```bash
pgq run myapp:main
```

This registers signal handlers, processes jobs as they arrive, runs scheduled tasks, and
shuts down gracefully on Ctrl+C.

### 3. Enqueue jobs

From another process or script:

=== "Async"

    ```python
    import asyncpg
    from pgqueuer.queries import Queries

    async def enqueue_jobs():
        conn = await asyncpg.connect()
        queries = Queries(conn)
        job_ids = await queries.enqueue(
            ["fetch"] * 10,
            [b"payload"] * 10,
            [0] * 10,
        )
        print(f"Enqueued {len(job_ids)} jobs")
    ```

=== "Sync"

    ```python
    import psycopg
    from pgqueuer.db import SyncPsycopgDriver
    from pgqueuer.queries import Queries

    conn = psycopg.connect(autocommit=True)
    driver = SyncPsycopgDriver(conn)
    queries = Queries(driver)
    job_ids = queries.enqueue("fetch", b"payload")
    ```

### 4. Monitor

```bash
pgq dashboard --interval 5
```

---

## Features

**Real-time delivery** — PostgreSQL `LISTEN/NOTIFY` pushes jobs to workers instantly. No polling, no external broker.

**Rate limiting** — Per-entrypoint `requests_per_second` and `concurrency_limit` protect downstream services.

**Built-in scheduler** — Cron-style recurring tasks without `celery-beat` or any extra process.

**Custom executors** — Plug in retry-with-backoff, notification dispatch, or any execution strategy.

**Observability** — Prometheus metrics, Logfire and Sentry tracing, live CLI dashboard.

**In-memory adapter** — Drop-in replacement for testing and CI. No Docker, no PostgreSQL required.

**Completion tracking** — `CompletionWatcher` lets callers await job results in real time.

**Deferred execution** — Schedule jobs for a future time with `execute_after`.

---

## Why PgQueuer?

If you're already running PostgreSQL, PgQueuer eliminates the need for Redis, RabbitMQ, or
any other message broker. Your jobs live in the same database as your application data — with
full ACID guarantees, row-level locking via `FOR UPDATE SKIP LOCKED`, and real-time
notifications via `LISTEN/NOTIFY`.

See the [Celery comparison](development/celery-comparison.md) for a detailed side-by-side.

---

## Next Steps

- [Shared Resources](guides/shared-resources.md) — share DB pools, HTTP clients, and ML models across jobs
- [Scheduling](guides/scheduling.md) — cron-style recurring tasks
- [Rate Limiting](guides/rate-limiting.md) — throttle and limit concurrency
- [CLI Reference](reference/cli.md) — full list of `pgq` commands
- [Architecture](reference/architecture.md) — how jobs flow through the system
