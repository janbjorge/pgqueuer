# Upgrading from 0.x to 1.0

PgQueuer 1.0 is the first stable release. From this point on, the project
follows [semantic versioning](https://semver.org/) strictly: patch releases
for bug fixes, minor releases for backward-compatible features, major releases
only when a break is unavoidable.

This guide covers the changes most users will hit when upgrading from 0.x.
The full list of 23 numbered breaking changes lives in
[RELEASE.md](https://github.com/janbjorge/pgqueuer/blob/main/RELEASE.md).

## TL;DR

1. Run `pgq upgrade` (or `pgq install` on a fresh database).
2. Convert every `def handler(job)` to `async def handler(job)`.
3. Wrap factory functions in `@asynccontextmanager` and `yield` the manager.
4. Replace `--pg-host` / `--pg-user` / etc. with standard `PGHOST` / `PGUSER`
   environment variables (or `--pg-dsn` / `PGDSN`).
5. Replace `serialized_dispatch=True` with `concurrency_limit=1` and
   per-entrypoint `retry_timer=` with a single `heartbeat_timeout=` on
   `pgq.run()`.
6. Replace `RetryWithBackoffEntrypointExecutor` with
   `DatabaseRetryEntrypointExecutor`.
7. Construct `QueueManager` / `SchedulerManager` /  `CompletionWatcher` with a
   `Queries` instance rather than a raw driver.
8. Run your test suite; most breakages surface at decoration or startup time.

## 1. Database schema

Two additions are required:

```sql
ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TYPE pgqueuer_status ADD VALUE IF NOT EXISTS 'failed';
```

`pgq upgrade` and `pgq install` apply these automatically. If you own the
schema, apply them before starting upgraded workers.

### `id` columns widened to `BIGINT` (#671)

The `queue`, `statistics`, and `schedules` primary keys were `int4 SERIAL`,
capped at ~2.1 billion. Since the sequence never reuses values, a long-lived
deployment could exhaust it and start failing every `enqueue`. Fresh installs
now use `BIGSERIAL`; `pgq upgrade` widens the existing `int4` columns and their
legacy sequences to `BIGINT` in place. The migration is idempotent (guarded on
the column/sequence type) and safe to re-run.

!!! warning "This migration takes an `ACCESS EXCLUSIVE` lock"
    Widening `int4` → `BIGINT` rewrites the table and rebuilds its indexes.
    Postgres holds an `ACCESS EXCLUSIVE` lock for the whole rewrite, blocking
    enqueues, dequeues, and plain `SELECT`s. The rewrite time scales with row
    count, so on a large or bloated `queue` table this can stall the queue for
    seconds to minutes. **Run `pgq upgrade` during a maintenance window or
    low-traffic period**, or run `pgq upgrade --no-widen-id` to apply every
    other migration while skipping the widen, then widen the table out-of-band.

    For a zero-downtime widen on a large table, see the
    [discussion on #671](https://github.com/janbjorge/pgqueuer/issues/671#issuecomment-4809834464)
    and the postgres.ai runbook
    [How to redefine a PK without downtime](https://github.com/postgres-ai/postgres-howtos/blob/main/0033_how_to_redefine_a_PK_without_downtime.md#the-whole-recipe).

## 2. Async-only job handlers

Synchronous job handlers are no longer accepted. Registering a plain `def`
function raises `TypeError` at decoration time.

```python
# Before
@pgq.entrypoint("resize_image")
def resize_image(job: Job) -> None:
    cpu_bound_resize(job.payload)

# After
@pgq.entrypoint("resize_image")
async def resize_image(job: Job) -> None:
    await asyncio.to_thread(cpu_bound_resize, job.payload)
```

Wrap blocking calls with `asyncio.to_thread`. Drop imports of `SyncEntrypoint`
and `SyncContextEntrypoint`: both are removed.

## 3. Factory functions are async context managers

`pgq run` requires the factory to return an async context manager. Plain async
functions that return a value, and `@contextmanager` factories, are rejected.

```python
# Before: any of these worked
async def factory() -> PgQueuer:
    return PgQueuer(...)

@contextmanager
def factory():
    yield PgQueuer(...)

# After
from contextlib import asynccontextmanager

@asynccontextmanager
async def factory():
    connection = await asyncpg.connect()
    pgq = PgQueuer(AsyncpgDriver(connection))

    @pgq.entrypoint("my_job")
    async def process(job: Job) -> None: ...

    yield pgq
    await connection.close()  # cleanup after yield runs on shutdown
```

Replace any `from pgqueuer.factories import run_factory` with
`validate_factory_result`: the new name validates the type but does not
convert it.

## 4. CLI connection options

The six per-component connection flags (`--pg-host`, `--pg-port`, `--pg-user`,
`--pg-database`, `--pg-password`, `--pg-schema`) are gone. Both asyncpg and
psycopg read standard libpq environment variables natively when no DSN is
provided.

```bash
# Before
pgq --pg-host db.internal --pg-user app --pg-database queue install

# After
PGHOST=db.internal PGUSER=app PGDATABASE=queue pgq install

# Custom schema: use PGOPTIONS
PGOPTIONS="-csearch_path=myschema" pgq install
```

What stays: `--pg-dsn` / `PGDSN` and `--prefix` / `PGQUEUER_PREFIX`.

The `dsn()` helper in `pgqueuer.adapters.drivers` / `pgqueuer.db` is also
removed; call `asyncpg.connect()` with no args and let it read the env vars
directly.

## 5. Concurrency: serialized_dispatch, retry_timer, RPS

Three entrypoint parameters were removed:

| Removed | Replacement |
|---------|-------------|
| `serialized_dispatch=True` | `concurrency_limit=1` |
| `retry_timer=timedelta(...)` (per entrypoint) | `heartbeat_timeout=timedelta(...)` on `pgq.run()` (global) |
| `requests_per_second=N` | `concurrency_limit=N` |

```python
# Before
@pgq.entrypoint("send_email", serialized_dispatch=True,
                retry_timer=timedelta(seconds=60))
async def send_email(job: Job) -> None: ...

await pgq.run(dequeue_timeout=timedelta(seconds=5))

# After
@pgq.entrypoint("send_email", concurrency_limit=1)
async def send_email(job: Job) -> None: ...

await pgq.run(
    dequeue_timeout=timedelta(seconds=5),
    heartbeat_timeout=timedelta(seconds=60),
)
```

Two semantics changed:

- `concurrency_limit` is now enforced **globally** at the database level via
  the dequeue SQL query. `concurrency_limit=5` caps at 5 concurrent jobs
  across the entire fleet, not 5 per worker.
- `heartbeat_timeout` is a single global value applied to all entrypoints
  (default 30 seconds). Heartbeats are sent automatically at half the timeout.
  If you had varying per-entrypoint timers, pick the maximum.

RPS rate limiting was inherently flaky (observed RPS diverged from actual
throughput under load); `concurrency_limit` gives deterministic backpressure
instead. Remove any `observed_requests_per_second()` call sites.

## 6. Retry executor renamed

The in-process retry executor is gone. Use the database-level variant, which
survives worker restarts.

```python
# Before
from pgqueuer.executors import (
    RetryWithBackoffEntrypointExecutor,
    MaxRetriesExceeded,
    MaxTimeExceeded,
)

@pgq.entrypoint(
    "my_task",
    executor_factory=lambda p: RetryWithBackoffEntrypointExecutor(
        parameters=p, max_attempts=5, max_delay=timedelta(seconds=10),
    ),
)

# After
from pgqueuer.executors import DatabaseRetryEntrypointExecutor

@pgq.entrypoint(
    "my_task",
    executor_factory=lambda p: DatabaseRetryEntrypointExecutor(
        parameters=p, max_attempts=5, max_delay=timedelta(minutes=5),
    ),
)
```

The `async-timeout` runtime dependency is dropped along with the old executor.

See the [Database-Level Retry](../guides/retry.md) guide for the new
`RetryRequested` exception and `DatabaseRetryEntrypointExecutor` parameters.

## 7. Manager constructors

`QueueManager` and `SchedulerManager` no longer accept a raw driver. The first
positional argument is now a `RepositoryPort` (typically `Queries`). The
underlying driver is reachable via `queries.driver`.

```python
# Before
qm = QueueManager(driver)
sm = SchedulerManager(driver)
watcher = CompletionWatcher(driver)

# After
from pgqueuer.queries import Queries

queries = Queries(driver)
qm = QueueManager(queries)
sm = SchedulerManager(queries)
watcher = CompletionWatcher(driver, queries=queries)
```

`PgQueuer(driver)` is **unchanged**: it still accepts a driver and constructs
`Queries` internally. Most user code only touches `PgQueuer`.

Replace `qm.connection` / `sm.connection` accesses with `qm.queries.driver`.

## 8. Internal modules removed from the package root

14 internal-only shims were removed from the `pgqueuer/` package root. If you
imported from any of these paths, update to the canonical location:

| Removed | Canonical |
|---------|-----------|
| `pgqueuer.buffers` | `pgqueuer.core.buffers` |
| `pgqueuer.cache` | `pgqueuer.core.cache` |
| `pgqueuer.cli` | `pgqueuer.adapters.cli.cli` |
| `pgqueuer.completion` | `pgqueuer.core.completion` |
| `pgqueuer.heartbeat` | `pgqueuer.core.heartbeat` |
| `pgqueuer.helpers` | Removed entirely; functions moved to natural homes |
| `pgqueuer.listeners` | `pgqueuer.core.listeners` |
| `pgqueuer.logconfig` | `pgqueuer.core.logconfig` |
| `pgqueuer.qb` | `pgqueuer.domain.settings` / `pgqueuer.adapters.persistence.qb` |
| `pgqueuer.query_helpers` | `pgqueuer.adapters.persistence.query_helpers` |
| `pgqueuer.retries` | Removed entirely |
| `pgqueuer.supervisor` | `pgqueuer.adapters.cli.supervisor` |
| `pgqueuer.tm` | `pgqueuer.core.tm` |
| `pgqueuer.tracing` | `pgqueuer.ports.tracing` + `pgqueuer.adapters.tracing.*` |

Public API shims (`pgqueuer.db`, `pgqueuer.queries`, `pgqueuer.qm`,
`pgqueuer.sm`, `pgqueuer.executors`, `pgqueuer.errors`, `pgqueuer.applications`,
`pgqueuer.factories`, `pgqueuer.types`, `pgqueuer.models`) are **unchanged**.

## 9. Tracing singleton moved

```python
# Before
from pgqueuer.adapters.tracing import TRACER, set_tracing_class

# After
from pgqueuer.ports.tracing import TRACER, set_tracing_class
```

The adapter implementations (`LogfireTracing`, `SentryTracing`,
`OpenTelemetryTracing`) still live under `pgqueuer.adapters.tracing.*`.

## 10. Custom drivers must implement `notify()`

If you maintain a custom `Driver`, add a `notify(channel, payload)` coroutine.
`pg_notify` moved out of the queries layer into the driver protocol.

```python
async def notify(self, channel: str, payload: str) -> None:
    await self.execute("SELECT pg_notify($1, $2)", channel, payload)
```

Remove any imports of `build_notify_query` from
`pgqueuer.adapters.persistence.qb`: the helper is gone.

## 11. Other API renames and removals

| Removed / renamed | What to do |
|-------------------|-----------|
| `PGChannel` type alias | Use `Channel` instead |
| `Queries.log_statistics(tail=...)` | Rename keyword to `limit=` |
| `Queries.peak_schedule()` | Renamed to `peek_schedule()` (typo fix) |
| `DBSettings.statistics_table_status_type` | Remove the reference; field is gone |
| `EntrypointStatistics` class | Removed (no replacement; was unused dead code) |
| `RetryManager` | Removed; retry logic now inlined into `TimedOverflowBuffer` |
| `TimedOverflowBuffer(callback=...)` | Override the `flush_items()` method and inject a repository port |
| `EntrypointExecutorParameters(channel=..., connection=..., queries=..., shutdown=...)` | Remove those kwargs from custom executor parameter construction |
| `ScheduleExecutorFactoryParameters(connection=..., queries=..., shutdown=...)` | Same; remove the kwargs |
| `AbstractScheduleExecutor.execute(self, schedule)` | Add a second parameter: `execute(self, schedule, context: ScheduleContext)` |

## New features worth adopting

- `RetryRequested` for transient errors; see [Database-Level Retry](../guides/retry.md).
- `on_failure="hold"` to park failed jobs instead of deleting them; see
  [Holding Failed Jobs](../guides/hold-failed-jobs.md). Pairs with `pgq failed`
  and `pgq requeue`.
- MCP server for read-only queue introspection; see
  [MCP Server](../integrations/mcp-server.md).
- `ScheduleContext` gives scheduled tasks access to the same shared `resources`
  mapping as queue handlers.

## Full breaking-change list

[RELEASE.md](https://github.com/janbjorge/pgqueuer/blob/main/RELEASE.md) has all
23 numbered changes, including niche ones not covered here (e.g.
`TaskManagerPort`, deletion of `pgqueuer/core/helpers.py`).
