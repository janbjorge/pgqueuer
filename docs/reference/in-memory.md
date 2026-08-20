# In-Memory Adapter

The in-memory adapter is a drop-in replacement for the PostgreSQL backend, so PgQueuer
can run entirely without a database connection. Located in `pgqueuer.adapters.inmemory`, it
provides `InMemoryDriver` and `InMemoryQueries` classes that satisfy the same
`RepositoryPort` protocol as the production PostgreSQL-backed implementation.

`QueueManager` and `SchedulerManager` work unchanged against the in-memory adapter. You
don't need to rewrite your job handlers or business logic.

The simplest way to use it is via the factory method:

```python
from pgqueuer import PgQueuer

pq = PgQueuer.in_memory()
```

Or import the classes directly:

```python
from pgqueuer import InMemoryDriver, InMemoryQueries
```

## Quick start

```python
import asyncio
from pgqueuer import PgQueuer
from pgqueuer.models import Job
from pgqueuer.types import QueueExecutionMode

async def main():
    pq = PgQueuer.in_memory()

    @pq.entrypoint("say_hello")
    async def say_hello(job: Job) -> None:
        print(f"Processing job {job.id}: {job.payload}")

    job_ids = await pq.qm.queries.enqueue(
        ["say_hello"] * 5,
        [b"job 1", b"job 2", b"job 3", b"job 4", b"job 5"],
        [0] * 5,
    )
    print(f"Enqueued {len(job_ids)} jobs")

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )
    print("All jobs processed!")

asyncio.run(main())
```

## When to use the in-memory adapter

Reach for it when the queue only has to live as long as one process:

- Unit and integration tests, with no PostgreSQL instance or Docker container to start
- CI pipelines on resource-constrained runners (GitHub Actions, lightweight containers)
- Local development and prototyping, before you set up infrastructure
- Short-lived batch containers that process a fixed job set and exit (ETL, one-time cleanup)

Avoid it when any of the following matter:

- Durability. A restart loses every queued and in-flight job, so nothing survives a crash.
- More than one process or machine. Jobs are invisible outside the process that enqueued them.
- Transactions. There is no rollback and no atomic retry.
- Post-mortem inspection. Once the process exits, the job history is gone.

## Limitations reference

| Capability | PostgreSQL adapter | In-memory adapter |
|---|---|---|
| **Durability** | Persists across restarts | Lost on process exit |
| **Multi-process workers** | Yes (row-level locking) | No |
| **Multi-node scaling** | Yes | No |
| **ACID transactions** | Yes | No (best-effort only) |
| **LISTEN/NOTIFY** | Real Postgres channels | In-process callbacks only |
| **Schema operations** | DDL executed | No-ops (always return True) |
| **Performance** | Bounded by network I/O | CPU-bound; O(n) dequeue scan |

## Implementation notes

### `driver.fetch()` and `driver.execute()`

Both methods raise `NotImplementedError` by design. `InMemoryQueries` operates directly on
in-memory dictionaries and never executes SQL.

### Event loop yielding

The `dequeue()` method includes an explicit `await asyncio.sleep(0)` to yield control to
the event loop. This is critical: without it, `QueueManager.fetch_jobs` would starve signal
handlers, timers, and concurrent jobs. The PostgreSQL adapter naturally yields during real
I/O; the in-memory adapter must do so explicitly.

### Schema management

- `install()`, `upgrade()`: no-ops
- `uninstall()`: clears all internal dictionaries, resetting queue state
- Schema inspection methods always return `True`

### Job state

The adapter maintains job state using plain Python dictionaries:

| Dict | Contents |
|------|----------|
| `_jobs` | Active job records |
| `_log` | Historical log entries (every status transition) |
| `_statistics` | Aggregated statistics |
| `_schedules` | Scheduled job definitions |
| `_dedupe_index` | Deduplication by key |

All are cleared when `uninstall()` is called or when the process exits.

### Notifications

`InMemoryDriver` emulates PostgreSQL LISTEN/NOTIFY via in-process callbacks:

```python
await driver.notify(channel, payload)
```

Registered callbacks are invoked synchronously and immediately.
