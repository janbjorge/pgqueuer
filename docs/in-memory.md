# In-Memory Adapter

## Overview

The in-memory adapter is a drop-in replacement for the PostgreSQL backend, allowing PgQueuer to run entirely without a database connection. Located in `pgqueuer.adapters.inmemory`, it provides `InMemoryDriver` and `InMemoryQueries` classes that satisfy the same `RepositoryPort` protocol as the production Postgres-backed implementation.

This means that `QueueManager` and `SchedulerManager` work unchanged against the in-memory adapter — you don't need to rewrite your job handlers or business logic.

The simplest way to use it is via the factory method:

```python
from pgqueuer import PgQueuer

pq = PgQueuer.in_memory()
```

Alternatively, you can import the classes directly:

```python
from pgqueuer import InMemoryDriver, InMemoryQueries
```

## Quick Start

Here's a minimal example that enqueues a few jobs and processes them immediately using drain mode:

```python
import asyncio
from pgqueuer import PgQueuer
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import QueueExecutionMode

async def main():
    # Create an in-memory queue
    pq = PgQueuer.in_memory()

    # Define a simple job handler
    @pq.entrypoint("say_hello")
    async def say_hello(job: Job) -> None:
        print(f"Processing job {job.id}: {job.payload}")

    # Enqueue some jobs
    job_ids = await pq.qm.queries.enqueue(
        ["say_hello"] * 5,
        [b"job 1", b"job 2", b"job 3", b"job 4", b"job 5"],
        [0] * 5,
    )
    print(f"Enqueued {len(job_ids)} jobs")

    # Process all jobs immediately (drain mode)
    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    print("All jobs processed!")

asyncio.run(main())
```

When you run this script, it will:
1. Enqueue 5 jobs to the in-memory queue
2. Start processing them immediately without needing a Postgres connection
3. Drain mode ensures all jobs are processed before `run()` returns

## When to Use the In-Memory Adapter

The in-memory adapter is recommended for:

- **Unit and integration tests** — Test your job handlers without spinning up a PostgreSQL instance or Docker container
- **CI/CD pipelines** — Run tests in resource-constrained environments (GitHub Actions, containers with limited memory)
- **Local development** — Develop and debug job handlers locally without a database dependency
- **Short-lived batch containers** — Process a fixed set of jobs in one run and discard them (e.g., data ingestion, ETL, one-time cleanup tasks)
- **Proof-of-concept and experimentation** — Quickly prototype queue logic without infrastructure setup

## When NOT to Use the In-Memory Adapter

The in-memory adapter has significant limitations that make it unsuitable for:

- **Production workloads requiring durability** — Any restart (deploy, crash, OOM kill) loses all queued and in-flight jobs
- **Multi-process workers** — No visibility across processes; jobs enqueued in one worker are invisible to others
- **Multi-node / distributed deployments** — No shared state between machines
- **Long-running services** — If the process must survive restarts or deploys, the in-memory adapter will lose data
- **ACID transaction guarantees** — The adapter has no rollback or transaction semantics; failed jobs cannot be atomically retried
- **Monitoring and observability requirements** — No external store means queue state cannot be inspected after the process exits

## Limitations Reference Table

| Capability | PostgreSQL adapter | In-memory adapter |
|---|---|---|
| **Durability** | Persists across restarts | Lost on process exit |
| **Multi-process workers** | Yes (row-level locking) | No |
| **Multi-node scaling** | Yes | No |
| **ACID transactions** | Yes | No (best-effort only) |
| **LISTEN/NOTIFY** | Real Postgres channels | In-process callbacks only |
| **Schema operations** | DDL executed | No-ops (always return True) |
| **Performance** | Bounded by network I/O | CPU-bound; O(n) dequeue scan |

## Implementation Notes

The in-memory adapter is intentionally minimal and makes some important design choices:

### `driver.fetch()` and `driver.execute()`

Both methods raise `NotImplementedError`. This is by design: `InMemoryQueries` operates directly on in-memory dictionaries and never needs to execute SQL. If you call these methods directly, you'll get a clear error message.

### Event Loop Yielding

The `dequeue()` method includes an explicit `await asyncio.sleep(0)` call:

```python
await asyncio.sleep(0)
```

This is **critical for correctness**. Without it, `QueueManager.fetch_jobs` would loop indefinitely and starve other tasks (signal handlers, timers, concurrent jobs) from running on the event loop. The PostgreSQL adapter naturally yields during real I/O; the in-memory adapter must do so explicitly.

### Schema Management

`install()`, `upgrade()`, `uninstall()`, and schema inspection methods (`has_table`, `table_has_column`, etc.) have minimal or no-op implementations:

- `install()`, `upgrade()` — no-ops (no schema to create)
- `uninstall()` — clears all internal dictionaries, resetting queue state
- Schema inspection methods — return `True` (they assume schema is valid)

### Job State Management

The adapter maintains job state using plain Python dictionaries keyed by job ID:

- `_jobs` — active job records
- `_log` — historical log entries (every status transition)
- `_statistics` — aggregated statistics
- `_schedules` — scheduled jobs (for the scheduler manager)
- `_dedupe_index` — for deduplication by key

All of these are cleared when `uninstall()` is called, or when the process exits.

### Notifications

`InMemoryDriver` stores registered LISTEN callbacks and delivers NOTIFY events via the `deliver()` method:

```python
driver.deliver(channel, payload)
```

This is how the in-memory adapter emulates Postgres LISTEN/NOTIFY channels without a real database. Callbacks are invoked synchronously and immediately.
