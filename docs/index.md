# PgQueuer

<div class="hero" markdown>

**Your PostgreSQL database is already a job queue.**

PgQueuer turns PostgreSQL into a fast, reliable background job processor.
Jobs live in the same database as your application data -- one stack,
full ACID guarantees, zero additional infrastructure.

[Get Started](getting-started/installation.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/janbjorge/pgqueuer){ .md-button }

</div>

---

## Why PostgreSQL?

If you're already running PostgreSQL, it can do double duty as your job queue.
That gives you:

- **One fewer service** to provision, monitor, and keep available
- **Transactional enqueuing** -- commit a job in the same transaction as your application data
- **Consistent state** -- your queue and your data always agree because they share the same database
- **Lower latency** -- jobs stay local, no round-trip to an external broker

## How PgQueuer Works

PgQueuer uses battle-tested PostgreSQL primitives to deliver jobs safely and fast:

- **`FOR UPDATE SKIP LOCKED`** -- workers claim jobs atomically; a job is never handed to two workers
- **`LISTEN/NOTIFY`** -- a trigger on the queue table fires `pg_notify()` on every insert, waking workers instantly
- **ACID transactions** -- jobs are enqueued and processed with the same guarantees as your application data
- **Row-level locking** -- multiple workers scale horizontally against a single database

```
                                  NOTIFY        +----------+  FOR UPDATE
                              +------------>    | Worker 1 | ----------+
                              |                 +----------+           |
               enqueue        |    NOTIFY       +----------+           |  SKIP
  Your App  ---------->  PostgreSQL ----------> | Worker 2 | ----------+  LOCKED
                              |                 +----------+           |
                              |    NOTIFY       +----------+           |
                              +------------>    | Worker N | ----------+
                                                +----------+           |
                              ^                                        |
                              +--- each worker claims its own rows ----+
```

## A Taste of PgQueuer

Define a consumer, register job handlers, and run:

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job

async def main() -> PgQueuer:
    connection = await asyncpg.connect()
    pgq = PgQueuer.from_asyncpg_connection(connection)

    @pgq.entrypoint("send_email")
    async def send_email(job: Job) -> None:
        print(f"Sending email: {job.payload}")

    return pgq
```

```bash
pgq install          # create the schema
pgq run myapp:main   # start processing
```

That's it. Just PostgreSQL and your application code.

[Full Quick Start Guide](getting-started/quickstart.md){ .md-button }

---

## Key Features

<div class="grid cards" markdown>

-   **Real-Time Delivery**

    ---

    PostgreSQL `LISTEN/NOTIFY` pushes jobs to workers instantly.
    A trigger on the queue table fires `pg_notify()` on every insert --
    workers wake up immediately without polling.

-   **Rate Limiting & Concurrency**

    ---

    Per-entrypoint `requests_per_second` and `concurrency_limit` protect
    downstream services. Or use `serialized_dispatch` to process jobs
    strictly one at a time.

-   **Built-In Scheduler**

    ---

    Cron-style recurring tasks via the `@schedule` decorator with 5-field
    (minute-level) or 6-field (second-level) expressions. No separate
    beat process required.

-   **Automatic Retries & Heartbeat**

    ---

    `RetryWithBackoffEntrypointExecutor` handles transient failures with
    exponential backoff. Heartbeat monitoring detects crashed workers,
    and `retry_timer` re-queues stalled jobs.

-   **Observability**

    ---

    Prometheus metrics, Logfire and Sentry distributed tracing, and a
    live CLI dashboard (`pgq dashboard`). See what your workers are
    doing in real time.

-   **In-Memory Testing**

    ---

    `PgQueuer.in_memory()` provides a drop-in replacement for tests
    and CI. No Docker, no PostgreSQL instance needed.

-   **Completion Tracking**

    ---

    `CompletionWatcher` lets callers await job results via
    `LISTEN/NOTIFY` -- no polling required. Wait for one job, many jobs,
    or race them with `asyncio.wait`.

-   **Deferred Execution**

    ---

    Schedule jobs for a future time with `execute_after`. Jobs stay
    queued until their timestamp passes, then enter the normal priority
    queue.

</div>

---

## PgQueuer vs Celery at a Glance

| Concern | PgQueuer | Celery |
|---------|----------|--------|
| Infrastructure | PostgreSQL only | PostgreSQL + Redis/RabbitMQ |
| Transactional enqueue | Same transaction as your data | Separate broker |
| Setup | `pip install` + `pgq install` | Broker install + config + beat process |
| Async model | Built on asyncio | Sync-first with async support |
| Recurring tasks | Built-in `@schedule` decorator | Separate `celery-beat` process |
| Complex workflows | Basic job queue | Chains, chords, groups, canvas |

Celery is a mature, battle-tested project that excels at complex multi-step workflows,
canvas primitives, and multi-broker topologies. PgQueuer is a good fit when your jobs
are backed by PostgreSQL and you want a simpler operational footprint.

[Detailed Comparison](comparisons/celery-comparison.md){ .md-button }

---

## Next Steps

- **[Installation](getting-started/installation.md)** -- install PgQueuer and set up the database schema
- **[Quick Start](getting-started/quickstart.md)** -- build your first consumer and producer in 5 minutes
- **[Core Concepts](getting-started/core-concepts.md)** -- understand jobs, entrypoints, and the status lifecycle
- **[Architecture](reference/architecture.md)** -- how data flows from producer to consumer
