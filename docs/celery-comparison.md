# PGQueuer vs Celery

PGQueuer aims to be a minimalist alternative to Celery by leveraging PostgreSQL directly. If you're already using PostgreSQL and want fewer moving parts, PGQueuer can often replace Celery plus an external broker. The sections below highlight the main differences and provide side-by-side examples.

## How PGQueuer Differs from Celery

- **PostgreSQL-Native**: PGQueuer integrates straight with PostgreSQL, using features like `LISTEN/NOTIFY` and `FOR UPDATE SKIP LOCKED`. Celery requires an external broker (such as Redis or RabbitMQ), so PGQueuer removes that extra dependency.
- **Simplified Architecture**: If you want fewer moving pieces, PGQueuer avoids the overhead of running additional services.
- **Recurring Task Scheduling**: PGQueuer has built-in cron-like scheduling. With Celery you typically run `celery-beat` or a separate scheduler.
- **Lightweight Design**: PGQueuer focuses on minimalism, making it easy to integrate into existing projects without bringing in a larger framework.

### Example: Running a Worker

A minimal Celery setup needs a broker and a worker process:

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

With PGQueuer you connect directly to PostgreSQL and run a single process:

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

### Example: Enqueuing a Task

Once the worker is running you can dispatch tasks. Celery exposes a
`delay()` helper that sends the task to the broker and returns a
`AsyncResult` instance:

```python
# enqueue_celery.py
from celery_app import add

result = add.delay(2, 3)
print(result.id)
```

With PGQueuer you enqueue jobs directly into PostgreSQL. The `Queries`
interface returns the job ID that can later be tracked:

```python
# enqueue_pgq.py
import asyncpg
from pgqueuer.queries import Queries

async def main() -> None:
    conn = await asyncpg.connect()
    job_id = await Queries(conn).enqueue("add", [2, 3])
    print(job_id)
```


### Example: Scheduling a Recurring Task

Celery typically relies on the `celery-beat` service for recurring jobs:

```python
# celery_scheduled.py
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

PGQueuer has the scheduler built in:

```python
# pgqueuer_scheduled.py
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

### Example: Waiting for Job Completion

Celery provides an `AsyncResult` instance that allows you to wait for a
task to finish and fetch the return value:

```python
result = add.delay(2, 3)
print(result.get())
```

With PGQueuer you can achieve similar behavior using the
`CompletionWatcher` helper:

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

Celery is a mature project with a long history. It offers advanced features such as complex retry policies, task chaining, and integrations outside the Python ecosystem. For very large scale or highly complex workflows, Celery may offer better performance and flexibility.

## When PGQueuer Is a Good Fit

- **You're Already Using PostgreSQL**: PGQueuer works directly with your existing database without an external broker.
- **You Value Stack Simplicity**: Reducing dependencies can make operations easier.
- **Lightweight Recurring Jobs**: PGQueuer's built‑in scheduler handles cron-style tasks without extra tools.

If your workflow prioritizes simplicity and you're already committed to PostgreSQL, PGQueuer can be a great choice. For more complex systems, Celery's broader feature set might still be the best option.
