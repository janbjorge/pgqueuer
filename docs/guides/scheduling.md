# Scheduling

PgQueuer includes a built-in scheduler for managing recurring tasks using cron-like expressions.
No separate process (like `celery-beat`) is required.

## Basic Usage

```python
from pgqueuer.models import Schedule

@pgq.schedule("fetch_db", "* * * * *")
async def fetch_db(schedule: Schedule) -> None:
    await perform_task()
```

## How It Works

- **Registration**: Define tasks using the `@schedule` decorator with a name and a cron expression.
- **Execution**: The scheduler runs tasks at defined intervals and tracks execution state.
- **Database Integration**: Schedules are stored in PostgreSQL, ensuring durability and recovery
  across process restarts.

### Scheduler Flow Diagram

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'fontSize': '14px', 'fontFamily': 'Inter, sans-serif'}}}%%
flowchart LR
    Define[@pgq.schedule]
    Store[(Schedule in DB)]
    Poll[Poll loop]
    Enqueue[Enqueue job]
    Execute[Execute task]

    Define --> Store
    Store --> Poll
    Poll -->|cron ready| Enqueue
    Enqueue --> Execute
    Poll -->|not yet| Poll

    classDef code     fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef database fill:#D0DCF0,stroke:#2E5080,stroke-width:2px,color:#111
    classDef process  fill:#DDEAF7,stroke:#4A6FA5,stroke-width:2px,color:#111
    classDef success  fill:#D5EDE5,stroke:#2D9D78,stroke-width:2px,color:#111

    class Define code
    class Store database
    class Poll process
    class Enqueue,Execute success
```

## Cron Expression Format

PgQueuer uses standard 5-field cron expressions:

```
┌───────────── minute (0–59)
│ ┌───────────── hour (0–23)
│ │ ┌───────────── day of month (1–31)
│ │ │ ┌───────────── month (1–12)
│ │ │ │ ┌───────────── day of week (0–6, Sunday=0)
│ │ │ │ │
* * * * *
```

Examples:

| Expression | Meaning |
|------------|---------|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour |
| `0 9 * * 1` | Every Monday at 9:00 AM |
| `0 0 1 * *` | First day of each month at midnight |

## Cleaning Up Old Schedules

When `clean_old=True`, PgQueuer deletes any **existing** database schedules whose entrypoint
matches this decorator's entrypoint before re-registering it. This is useful when you change
the cron expression for a task and want the old schedule row replaced on startup.

```python
@pgq.schedule("fetch_db", "* * * * *", clean_old=True)
async def fetch_db(schedule: Schedule) -> None:
    await perform_task()
```

By default, `clean_old=False` — old schedules are retained.

## Managing Schedules via CLI

List all schedules:

```bash
pgq schedules
```

Remove a schedule by name:

```bash
pgq schedules --remove fetch_db
```

## Example: Full Setup with Scheduling

```python
from datetime import datetime
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    connection = await asyncpg.connect()
    pgq = PgQueuer.from_asyncpg_connection(connection)

    @pgq.entrypoint("process_job")
    async def process_job(job: Job) -> None:
        print(f"Processing: {job!r}")

    @pgq.schedule("hourly_report", "0 * * * *")
    async def hourly_report(schedule: Schedule) -> None:
        print(f"Generating report at {datetime.now()}")

    @pgq.schedule("daily_cleanup", "0 2 * * *", clean_old=True)
    async def daily_cleanup(schedule: Schedule) -> None:
        print(f"Running cleanup at {datetime.now()}")

    return pgq
```

```bash
pgq run myapp:main
```
