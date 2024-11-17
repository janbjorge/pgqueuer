# PgQueuer: Unified Job and Schedule Orchestrator

The `PgQueuer` class orchestrates job queues and schedules recurring tasks using PostgreSQL. It combines the `QueueManager` and `SchedulerManager` to manage periodic tasks and queued tasks.

## Setting Up PgQueuer

To use `PgQueuer`, establish a PostgreSQL connection using asyncpg or psycopg and wrap it in a `Driver` instance. Below is an example:

```python
from __future__ import annotations

from datetime import datetime
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute {schedule!r} {datetime.now()!r}")

    return pgq
```

This example sets up `PgQueuer` with a job entrypoint (`fetch`) and a scheduled task (`scheduled_every_minute`).

## Starting PgQueuer with the CLI Tool

To start `PgQueuer` without additional scripting, use the CLI tool:

```bash
pgq run mypackage.create_pgqueuer
```

This will initialize and run the `PgQueuer` instance, handling both job queues and schedules efficiently with built-in mechanisms for graceful shutdown.
