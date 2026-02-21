# PGQueuer vs Celery

PGQueuer aims to be a minimalist alternative to Celery by leveraging PostgreSQL directly.
If you're already using PostgreSQL and want fewer moving parts, PGQueuer can often replace
Celery plus an external broker.

## How PGQueuer Differs from Celery

| Feature | Celery | PGQueuer |
|---------|--------|---------|
| Message broker | Requires Redis, RabbitMQ, etc. | PostgreSQL (no extra service) |
| Recurring tasks | Requires `celery-beat` | Built-in scheduler |
| Architecture | Multi-service | Single database |
| Setup complexity | Higher | Lower |

## Example: Running a Worker

**Celery** needs a broker and a separate worker process:

```python
# celery_app.py
from celery import Celery

celery_app = Celery("tasks", broker="redis://localhost:6379/0")

@celery_app.task
def add(x, y):
    return x + y
```

```bash
celery -A celery_app worker -l info
```

**PgQueuer** connects directly to PostgreSQL:

```python
# pgqueuer_app.py
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def create_pgq():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("add")
    async def add(job: Job):
        x, y = job.payload
        return x + y

    return pgq
```

```bash
pgq run pgqueuer_app:create_pgq
```

## Example: Enqueuing a Task

**Celery** uses `delay()`:

```python
from celery_app import add
result = add.delay(2, 3)
print(result.id)
```

**PgQueuer** enqueues directly into PostgreSQL:

```python
import asyncpg
from pgqueuer.queries import Queries

async def main() -> None:
    conn = await asyncpg.connect()
    job_id = await Queries(conn).enqueue("add", [2, 3])
    print(job_id)
```

## Example: Scheduling a Recurring Task

**Celery** requires the `celery-beat` service:

```python
from celery import Celery
from celery.schedules import crontab

app = Celery("tasks", broker="redis://localhost:6379/0")

@app.task
def cleanup():
    print("Cleaning up...")

app.conf.beat_schedule = {
    "cleanup-every-hour": {
        "task": "cleanup",
        "schedule": crontab(minute=0, hour="*"),
    }
}
```

```bash
celery -A celery_scheduled worker -B -l info
```

**PgQueuer** has the scheduler built in:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Schedule

async def create_pgq():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.schedule("cleanup", "0 * * * *")
    async def cleanup(schedule: Schedule):
        print("Cleaning up...")

    return pgq
```

```bash
pgq run pgqueuer_scheduled:create_pgq
```

## Example: Waiting for Job Completion

**Celery** provides `AsyncResult.get()`:

```python
result = add.delay(2, 3)
print(result.get())
```

**PgQueuer** uses `CompletionWatcher`:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.completion import CompletionWatcher

async def wait_for_job() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    job_id = await pgq.queries.enqueue("add", [2, 3])
    async with CompletionWatcher(driver) as watcher:
        status = await watcher.wait_for(job_id)
        print(status)
```

## Acknowledging Celery's Strengths

Celery is a mature project with a long history. It offers advanced features such as complex
retry policies, task chaining, and integrations outside the Python ecosystem. For very large
scale or highly complex workflows, Celery may offer better performance and flexibility.

## When PGQueuer Is a Good Fit

- You're already using PostgreSQL and want to avoid an extra broker service.
- You value stack simplicity and reduced operational overhead.
- You need lightweight recurring jobs without running `celery-beat`.
- Your workload fits within PostgreSQL's throughput characteristics.

For more complex systems or multi-language environments, Celery's broader feature set might
still be the better choice.
