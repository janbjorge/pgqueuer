# Quick Start

This guide walks you through setting up PgQueuer from scratch — defining a consumer,
running it, and enqueuing your first jobs.

## 1. Set up the database

```bash
pgq install
```

## 2. Define a consumer

Create a file, for example `myapp.py`:

=== "asyncpg connection"

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
            print(f"Processed message: {job!r}")

        @pgq.schedule("scheduled_every_minute", "* * * * *")
        async def scheduled_every_minute(schedule: Schedule) -> None:
            print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

        return pgq
    ```

=== "asyncpg pool"

    ```python
    from datetime import datetime
    import asyncpg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job, Schedule

    async def main() -> PgQueuer:
        pool = await asyncpg.create_pool("postgresql://user:password@localhost/dbname")
        pgq = PgQueuer.from_asyncpg_pool(pool)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed message: {job!r}")

        @pgq.schedule("scheduled_every_minute", "* * * * *")
        async def scheduled_every_minute(schedule: Schedule) -> None:
            print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

        return pgq
    ```

=== "psycopg"

    ```python
    from datetime import datetime
    import psycopg
    from pgqueuer import PgQueuer
    from pgqueuer.models import Job, Schedule

    async def main() -> PgQueuer:
        connection = await psycopg.AsyncConnection.connect(
            "postgresql://user:password@localhost/dbname",
            autocommit=True
        )
        pgq = PgQueuer.from_psycopg_connection(connection)

        @pgq.entrypoint("fetch")
        async def process_message(job: Job) -> None:
            print(f"Processed message: {job!r}")

        @pgq.schedule("scheduled_every_minute", "* * * * *")
        async def scheduled_every_minute(schedule: Schedule) -> None:
            print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

        return pgq
    ```

!!! note
    For psycopg connections, always ensure `autocommit=True` is set.

## 3. Start the consumer

```bash
pgq run myapp:main
```

This command:

- Registers termination signal handlers (SIGINT, SIGTERM)
- Processes jobs continuously as they arrive
- Runs scheduled tasks at their defined intervals
- Shuts down gracefully on Ctrl+C

## 4. Enqueue jobs

From another process or script:

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

Or from a synchronous context (enqueue only):

```python
import psycopg
from pgqueuer.db import SyncPsycopgDriver
from pgqueuer.queries import Queries

conn = psycopg.connect(autocommit=True)
driver = SyncPsycopgDriver(conn)
queries = Queries(driver)
job_ids = queries.enqueue("fetch", b"payload")
```

## 5. Monitor with the dashboard

```bash
pgq dashboard --interval 5
```

## Next steps

- [Shared Resources](../guides/shared-resources.md) — share DB pools, HTTP clients, ML models across jobs
- [Scheduling](../guides/scheduling.md) — configure cron-style recurring tasks
- [Rate Limiting & Concurrency](../guides/rate-limiting.md) — protect downstream services
- [CLI Reference](../reference/cli.md) — full list of `pgq` commands
