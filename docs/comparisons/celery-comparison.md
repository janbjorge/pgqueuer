# PgQueuer vs Celery

Celery is the most established task queue in the Python ecosystem:
[BSD-licensed](https://pypi.org/project/celery/), actively maintained
([5.6.3 released 2026-03-26](https://github.com/celery/celery/releases)), with a
broker-based architecture that scales beyond what
any single-database queue can do. PgQueuer solves the same core problem, reliable
background job processing, with a smaller footprint: PostgreSQL is the only moving
part.

Facts about Celery were checked on 2026-08-20 against
[docs.celeryq.dev](https://docs.celeryq.dev/en/stable/) and Celery 5.6.3.

## Architecture at a glance

| | Celery | PgQueuer |
|---|--------|----------|
| Message transport | RabbitMQ, Redis, SQS (stable); Kafka, Zookeeper, GC Pub/Sub (experimental)[^brokers] | PostgreSQL (`LISTEN/NOTIFY` + `FOR UPDATE SKIP LOCKED`) |
| Result storage | 17 supported backends (Redis, RPC, SQLAlchemy, DynamoDB, S3, ...)[^backends] | PostgreSQL (`pgqueuer_log` table) |
| Processes to run | Worker + broker + result backend + optional `beat` | Worker + PostgreSQL |
| Concurrency model | prefork, eventlet, gevent, threads, solo[^concurrency] | asyncio |
| Recurring tasks | Separate `celery beat` process | Built-in `@pgq.schedule` decorator |
| Workflow primitives | Chains, chords, groups, map, chunks ([canvas](https://docs.celeryq.dev/en/stable/userguide/canvas.html)) | None; single jobs only |
| Multi-language | Protocol designed for it (message `lang` header)[^protocol] | Python only |
| Monitoring | [Flower](https://docs.celeryq.dev/en/stable/userguide/monitoring.html) web UI, `celery events`, inspect commands | `pgq dashboard`, [web dashboard](../integrations/web-dashboard.md), [Prometheus](../integrations/prometheus.md), SQL |

[^brokers]: [Broker overview](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html).
[^backends]: [Introduction, "What do I need?"](https://docs.celeryq.dev/en/stable/getting-started/introduction.html).
[^concurrency]: [Introduction, features list](https://docs.celeryq.dev/en/stable/getting-started/introduction.html). Native asyncio task support is not shipped; it is an open design discussion targeted at Celery 6.0 ([#3884](https://github.com/celery/celery/issues/3884), [#7874](https://github.com/celery/celery/issues/7874)).
[^protocol]: [Message protocol v2](https://docs.celeryq.dev/en/stable/internals/protocol.html): "Worker may redirect the message to a worker that supports the language."

## Delivery semantics

This is the most consequential difference, and it cuts both ways.

**Celery acknowledges early by default.** The worker acks a task *before* executing
it, "so that a task invocation that already started is never executed again"
([docs](https://docs.celeryq.dev/en/stable/userguide/tasks.html)). If the worker
crashes mid-task, the task is gone. Opting into
[`acks_late`](https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-acks-late)
plus `task_reject_on_worker_lost` gives redelivery, with the documented requirement
that tasks be idempotent. On Redis, redelivery is governed by a
[visibility timeout](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html)
(default 1 hour), and the docs warn that ETA/countdown tasks exceeding it "will be
executed again, and again in a loop."

**PgQueuer is at-least-once by design.** A picked job holds a row-level lock and
refreshes a heartbeat; if the worker dies, the job becomes eligible for re-pickup
after `heartbeat_timeout` and any worker resumes it (see
[Reliability Model](../guides/reliability.md)). The same idempotency requirement
applies: a recovered job runs again from the start.

If your tasks must never run twice, Celery's early-ack default is the safer behavior.
If your tasks must never be lost, PgQueuer gives you that without configuration.

## Feature comparison

| Feature | Celery | PgQueuer |
|---------|--------|----------|
| Transactional enqueue with app data | No; the broker is separate from your database | Yes; same PostgreSQL transaction |
| Retries | [`autoretry_for`, exponential backoff, `max_retries`](https://docs.celeryq.dev/en/stable/userguide/tasks.html) | [`RetryRequested`, `DatabaseRetryEntrypointExecutor` with backoff](../guides/retry.md) |
| Rate limiting | Per-task, changeable at runtime | No built-in rate limiter |
| Concurrency limits | Worker pool size, [autoscaling](https://docs.celeryq.dev/en/stable/userguide/workers.html) | [Per-entrypoint, enforced globally in SQL](../guides/concurrency-control.md) |
| Task routing | [Glob/regex routes, AMQP exchanges](https://docs.celeryq.dev/en/stable/userguide/routing.html) | Entrypoint name only |
| Priorities | Native on RabbitMQ; limited on Redis[^prio] | Native integer priority column |
| Deduplication | Not built in | [`dedupe_key` unique constraint](../guides/reliability.md#idempotency) |
| Scheduled/recurring | `beat`, a separate single-instance process[^beat] | [Built-in, DB-backed, any worker](../guides/scheduling.md) |
| Job completion tracking | `AsyncResult.get()` via result backend | [`CompletionWatcher` via `LISTEN/NOTIFY`](../guides/completion-tracking.md) |

[^prio]: [Routing guide](https://docs.celeryq.dev/en/stable/userguide/routing.html): Redis priorities "will never be as good as priorities implemented at the broker server level."
[^beat]: [Periodic tasks](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html): "You have to ensure only a single scheduler is running for a schedule at a time, otherwise you'd end up with duplicate tasks." Embedding beat in a worker with `-B` is documented as not recommended for production.

## Example: running a worker

**Celery** needs a broker and a separate worker process (adapted from
[First steps with Celery](https://docs.celeryq.dev/en/stable/getting-started/first-steps-with-celery.html)):

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

**PgQueuer** connects directly to PostgreSQL (see the
[Quick Start](../getting-started/quickstart.md)):

```python
# pgqueuer_app.py
import json
import asyncpg
from contextlib import asynccontextmanager
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

@asynccontextmanager
async def create_pgq():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("add")
    async def add(job: Job):
        data = json.loads(job.payload)
        return data["x"] + data["y"]

    yield pgq
```

```bash
pgq run pgqueuer_app:create_pgq
```

## Example: enqueuing a task

**Celery** uses `delay()`
([First steps, "Calling the task"](https://docs.celeryq.dev/en/stable/getting-started/first-steps-with-celery.html)):

```python
from celery_app import add
result = add.delay(2, 3)
print(result.id)
```

**PgQueuer** enqueues directly into PostgreSQL (see the
[Quick Start](../getting-started/quickstart.md)):

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

## Example: scheduling a recurring task

**Celery** uses the `beat` scheduler (adapted from
[Periodic tasks](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html)):

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

**PgQueuer** has the scheduler built in. State lives in PostgreSQL, so any worker
can run due tasks and schedules survive restarts (see
[Scheduling](../guides/scheduling.md)):

```python
import asyncpg
from contextlib import asynccontextmanager
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Schedule

@asynccontextmanager
async def create_pgq():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.schedule("cleanup", "0 * * * *")
    async def cleanup(schedule: Schedule):
        print("Cleaning up...")

    yield pgq
```

```bash
pgq run pgqueuer_scheduled:create_pgq
```

## Example: waiting for job completion

**Celery** provides `AsyncResult.get()`, which requires a configured result backend
([First steps, "Keeping results"](https://docs.celeryq.dev/en/stable/getting-started/first-steps-with-celery.html)):

```python
result = add.delay(2, 3)
print(result.get())
```

**PgQueuer** uses `CompletionWatcher`, which streams status updates via
`LISTEN/NOTIFY` (see [Completion Tracking](../guides/completion-tracking.md)):

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries
from pgqueuer.core.completion import CompletionWatcher

async def wait_for_job() -> None:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    queries = Queries(driver)
    job_ids = await queries.enqueue("add", b'{"x": 2, "y": 3}')
    async with CompletionWatcher(driver, queries=queries) as watcher:
        status = await watcher.wait_for(job_ids[0])
        print(status)
```

## When Celery is the better choice

- Canvas primitives (chains, chords, groups) let you compose multi-step pipelines
  declaratively. PgQueuer has nothing comparable.
- A dedicated broker like RabbitMQ absorbs message volume that would otherwise
  contend with your application's OLTP load on a shared PostgreSQL.
- Flower's web UI and remote worker control (autoscaling, runtime rate-limit
  changes, revocation, broadcast commands) have years of production use behind them.
- Celery's prefork model fits classic Django/Flask apps without requiring asyncio.
  PgQueuer's handlers must be `async def`.
- The message protocol carries a language header and has implementations outside
  Python, so it can serve polyglot systems.
- The ecosystem is much larger: integrations, documentation, answers, and hiring
  familiarity.

## When PgQueuer is the better choice

- You already run PostgreSQL and want no additional infrastructure to provision,
  monitor, secure, and keep available.
- Committing a job atomically with your application data removes a class of
  dual-write bugs. With a separate broker this requires an outbox pattern.
- Handlers are plain coroutines, so asyncio codebases need no pool bridging.
- Recurring jobs need no separate scheduler process and have no single-instance
  constraint, since schedule state lives in the database.
- Crash recovery via heartbeat re-pickup is the default, not an `acks_late` plus
  visibility-timeout configuration exercise.

For throughput numbers, see [Benchmarks](benchmarks.md).
