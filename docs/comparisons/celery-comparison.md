# PgQueuer vs Celery

Celery and PgQueuer solve the same core problem -- reliable background job processing --
with different trade-offs. This page compares them side by side so you can pick the
right tool for your situation.

## Feature Comparison

| Feature | Celery | PgQueuer |
|---------|--------|---------|
| Message broker | Redis, RabbitMQ, SQS, etc. | PostgreSQL |
| Recurring tasks | Separate `celery-beat` process | Built-in `@schedule` decorator |
| Architecture | App + broker + optional beat | App + PostgreSQL |
| Job delivery | Broker-dependent | Real-time via `LISTEN/NOTIFY` |
| Transactional enqueue | Separate from app data | Same PostgreSQL transaction |
| Async model | Sync-first with async support | Built on asyncio |
| Complex workflows | Chains, chords, groups, canvas | Basic job queue |

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
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    job_ids = await Queries(driver).enqueue("add", b'{"x": 2, "y": 3}')
    print(job_ids)
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

**PgQueuer** uses `CompletionWatcher`, which streams status updates via `LISTEN/NOTIFY`:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries
from pgqueuer.completion import CompletionWatcher

async def wait_for_job() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    job_ids = await Queries(driver).enqueue("add", b'{"x": 2, "y": 3}')
    async with CompletionWatcher(driver) as watcher:
        status = await watcher.wait_for(job_ids[0])
        print(status)
```

## When PgQueuer Is a Good Fit

- You're already using PostgreSQL and want to avoid an extra broker service
- You value stack simplicity and reduced operational overhead
- You need lightweight recurring jobs without running `celery-beat`
- Your workload fits within PostgreSQL's throughput characteristics
- You want transactional enqueuing (enqueue a job in the same transaction as your app data)

## When Celery May Be Better

Celery is a mature project with a long history. It may be the better choice when:

- You need complex multi-step workflows (chains, chords, groups)
- You require canvas primitives for task composition
- Your system spans multiple languages or needs a non-Python broker
- You need very high throughput beyond what a single PostgreSQL instance can deliver

For more details on PgQueuer's throughput characteristics, see [Benchmarks](benchmarks.md).
