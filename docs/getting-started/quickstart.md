# Quick Start

This guide walks you through creating a job consumer, enqueuing work, and monitoring
the queue -- all in under 5 minutes.

**Prerequisites:** PgQueuer installed and schema created. See [Installation](installation.md)
if you haven't done that yet.

---

## 1. Define a Consumer

Create a file called `myapp.py`. PgQueuer uses a factory pattern: you write an `async`
function that returns a configured `PgQueuer` instance.

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
    from pgqueuer.models import Job
    from pgqueuer.types import QueueExecutionMode

    async def test_job_handler():
        pq = PgQueuer.in_memory()

        @pq.entrypoint("say_hello")
        async def say_hello(job: Job) -> None:
            print(f"Processing: {job.payload}")

        await pq.qm.queries.enqueue(["say_hello"], [b"hello"], [0])
        await pq.qm.run(mode=QueueExecutionMode.drain)
    ```

!!! note
    psycopg connections **must** use `autocommit=True`. PgQueuer relies on
    `LISTEN` and immediate visibility of committed rows, which requires autocommit mode.

**What's happening here:**

- `PgQueuer.from_asyncpg_connection(connection)` wraps the connection in an `AsyncpgDriver`
  and sets up a `QueueManager` and `SchedulerManager`.
- `@pgq.entrypoint("fetch")` registers an async function to handle jobs with entrypoint name `"fetch"`.
- `@pgq.schedule("cleanup", "0 * * * *")` registers a cron task that runs every hour.
- Returning the `PgQueuer` instance tells the CLI how to start your application.

---

## 2. Run the Consumer

```bash
pgq run myapp:main
```

The `run` command:

1. Imports `myapp` and calls `main()` to get the `PgQueuer` instance.
2. Registers signal handlers for graceful shutdown (`SIGTERM`, `SIGINT`).
3. Starts the `QueueManager` (job processing) and `SchedulerManager` (cron tasks) concurrently.
4. Listens on the `ch_pgqueuer` NOTIFY channel for new work.
5. Shuts down cleanly on Ctrl+C, waiting for in-flight jobs to finish.

---

## 3. Enqueue Jobs

From another process or script, push jobs into the queue:

=== "Async"

    ```python
    import asyncpg
    from pgqueuer.queries import Queries

    async def enqueue_jobs():
        conn = await asyncpg.connect()
        queries = Queries(conn)
        job_ids = await queries.enqueue(
            ["fetch"] * 10,        # entrypoint names
            [b"payload"] * 10,     # payloads (bytes)
            [0] * 10,              # priorities (higher = first)
        )
        print(f"Enqueued {len(job_ids)} jobs")
    ```

=== "Sync"

    ```python
    import psycopg
    from pgqueuer.db import SyncPsycopgDriver
    from pgqueuer.queries import SyncQueries

    conn = psycopg.connect(autocommit=True)
    driver = SyncPsycopgDriver(conn)
    queries = SyncQueries(driver)
    job_ids = queries.enqueue("fetch", b"payload")
    ```

=== "CLI"

    ```bash
    pgq queue --entrypoint fetch --payload '{"key": "value"}'
    ```

`Queries.enqueue()` accepts lists for batch enqueuing. Each list element corresponds to one
job. The method returns the IDs of the newly created jobs.

---

## 4. Monitor

Watch your queue in real time:

```bash
pgq dashboard --interval 5
```

This refreshes every 5 seconds and shows jobs per entrypoint, status counts, and throughput.

---

## What's Next?

You now have a working PgQueuer setup. Here's where to go from here:

| Goal | Page |
|------|------|
| Understand the mental model | [Core Concepts](core-concepts.md) |
| Add rate limiting or concurrency caps | [Rate Limiting](../guides/rate-limiting.md) |
| Set up cron-style recurring tasks | [Scheduling](../guides/scheduling.md) |
| Handle transient failures with retries | [Custom Executors](../guides/custom-executors.md) |
| Deploy workers to production | [Deployment](../guides/deployment.md) |
| Test without PostgreSQL | [In-Memory Adapter](../reference/in-memory.md) |
