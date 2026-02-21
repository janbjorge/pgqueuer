---
hide:
  - navigation
  - toc
---

<div class="pgq-hero" markdown>

# PgQueuer

**High-performance PostgreSQL job queue for Python**

Zero extra infrastructure. Pure PostgreSQL. Production-ready.

[Get Started](getting-started/installation.md){ .pgq-hero-badge }
[GitHub](https://github.com/janbjorge/pgqueuer){ .pgq-hero-badge }
[PyPI](https://pypi.org/project/pgqueuer/){ .pgq-hero-badge }

</div>

## Install

```bash
pip install pgqueuer[asyncpg]
# or
pip install pgqueuer[psycopg]
```

---

<div class="pgq-grid" markdown>

<div class="pgq-card" markdown>

### LISTEN / NOTIFY

Real-time job delivery via PostgreSQL's native pub/sub — no polling, no extra broker.

</div>

<div class="pgq-card" markdown>

### Rate Limiting

Per-entrypoint `requests_per_second` and `concurrency_limit` keep downstream services safe.

</div>

<div class="pgq-card" markdown>

### Built-in Scheduler

Cron-style recurring tasks without `celery-beat` or any extra process.

</div>

<div class="pgq-card" markdown>

### Custom Executors

Plug in retry-with-backoff, notification dispatch, or any execution strategy you need.

</div>

<div class="pgq-card" markdown>

### Observability

Prometheus metrics, Logfire and Sentry tracing, live dashboard — all first-class.

</div>

<div class="pgq-card" markdown>

### In-Memory Adapter

Drop-in replacement for testing and CI — no Docker, no PostgreSQL required.

</div>

</div>

---

## Quick Example

=== "asyncpg"

    ```python
    import asyncpg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job

    async def main() -> PgQueuer:
        connection = await asyncpg.connect()
        pgq = PgQueuer.from_asyncpg_connection(connection)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed: {job!r}")

        return pgq
    ```

    ```bash
    pgq run myapp:main
    ```

=== "psycopg"

    ```python
    import psycopg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job

    async def main() -> PgQueuer:
        connection = await psycopg.AsyncConnection.connect(autocommit=True)
        pgq = PgQueuer.from_psycopg_connection(connection)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed: {job!r}")

        return pgq
    ```

    ```bash
    pgq run myapp:main
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

---

## Why PgQueuer?

If you're already running PostgreSQL, PgQueuer eliminates the need for Redis, RabbitMQ, or any
other message broker. Your jobs live in the same database as your application data — with full
ACID guarantees, row-level locking via `FOR UPDATE SKIP LOCKED`, and real-time notifications
via `LISTEN/NOTIFY`.

See the [Celery Comparison](development/celery-comparison.md) for a detailed breakdown.
