# Release Notes

PgQueuer follows semantic versioning from v1.0.0 onward. Schema changes are
applied by `pgq install` / `pgq upgrade`. Run the upgrade **before** starting
workers on the new version. `QueueManager.run()` verifies the schema at
startup and refuses to boot against an old one.

Identifiers below use the default unprefixed names; substitute your own if you
set `--prefix` / `PGQUEUER_PREFIX`.

The top section is the next release. Add to it under the version the change
warrants, and cut the tag from it when it ships. Release dates live in the git
tags, so a section is written once and never revisited.

---

## v1.4.0

### Schema change: capacity slots for `concurrency_limit`

Per-entrypoint `concurrency_limit` is now enforced by capacity slots instead of
a row count, closing an overshoot when several workers dequeued concurrently
(#761, #774, #777). This adds one column and one partial unique index:

```sql
ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS slot BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS pgqueuer_picked_slot_idx
    ON pgqueuer (entrypoint, slot) WHERE (status = 'picked' AND slot IS NOT NULL);
```

> **Migrate before deploying workers.** `verify_structure()` now requires
> both, so workers started against an unmigrated database raise `RuntimeError`
> on boot. The index build holds a `SHARE` lock and blocks writes to the queue
> table for its duration. Run it in a low-traffic window on a large table.

Jobs already `picked` when the migration runs keep `slot IS NULL`. They stay
covered by the count gate and drain out normally.

Workers must agree on an entrypoint's limit. If they disagree, the highest one
wins; nothing validates or rejects the mismatch.

### Fixed

- Dequeue no longer overshoots `concurrency_limit`: candidates are windowed
  before locking, so `SKIP LOCKED` cannot slide down the backlog past the cap
  (#774).
- `--restart-on-failure` now restarts. Each supervisor cycle owns its shutdown
  event, so a manager setting `shutdown` on exit no longer latches the
  process-level event and ends the loop. Workers that previously exited on
  failure now restart in place. Review any external restart or alerting that
  relied on the process dying.

### Changed

- `QueryQueueBuilder.build_dequeue_query()` and `build_log_statistics_query()`
  take keyword-only arguments and return a `ComposedQuery` (`.sql`, `.args`)
  instead of a SQL string. Both are internal but reachable via `Queries.qbq`.
- Dequeue SQL is assembled per active gate by a new `SqlComposer`, so unused
  concurrency gates no longer render (ADR-0024).
- `is_unique_violation()` classifies driver errors by SQLSTATE instead of
  importing asyncpg and psycopg. Drivers that do not expose `.sqlstate` are no
  longer recognised.
- `dequeue()` treats a lost slot race (SQLSTATE `23505` or `40P01`) as an empty
  batch and lets the poll loop retry.

---

## v1.3.2

- Pin published Docker images to the released PgQueuer version (#717).

## v1.3.1

- Publish multi-arch Docker images to GHCR (#716).

## v1.3.0

### Features

- Built-in web dashboard for queue insights and management (#691).
- First-class Postgres schema support. `prefix` is now a `DBSettings` field and
  `add_prefix()` is deprecated (#703, #704).
- `pgq sql` command group; `--dry-run` deprecated (#705).
- Periodic log→statistics aggregation, no cron required (#707).
- `connect_psycopg`, `ConnectionSettings`, and shared pool factories.
  Single-connection factories are now context managers.

### Fixes

- Dequeue caps picks to the remaining per-entrypoint and per-worker slots, and
  drops the batch cap to the tightest entrypoint limit (#696, #697).
- Shutdown dispatches the remaining batch instead of stranding picked rows
  (#693); `run()` tears down lifecycle tasks on failure (#694).
- Drain confirms an empty queue with an uncached count before exiting.
- Dequeue timeout probes eligible queued work before the deferred ETA (#695).
- Aggregation index rebuilt on upgrade (#670); the aggregation tick is skipped
  when the worker did no work (#713).
- In-memory adapter dequeues from priority heaps instead of a full scan.

## v1.2.0

- `enqueue(on_conflict=...)` to control dedupe-key conflicts (#681).
- Dequeue preserves priority when merging queued and stale jobs.

## v1.1.1

**Schema change.** Ships the `BIGINT` `id` widening described under v1.0.0
(#676), lifting the ~2.1B lifetime-enqueue ceiling on the queue, statistics,
and schedules tables. `pgq upgrade` rewrites each table under an
`ACCESS EXCLUSIVE` lock. See the warning in the v1.0.0 notes, or use
`pgq upgrade --no-widen-id` and widen out-of-band.

## v1.1.0

- Context injection is auto-detected from the handler signature (#635).
- `--heartbeat-timeout` exposed on `pgq run` (#675).
- **Schema change.** Dequeue rewritten around per-entrypoint `LATERAL` lookups,
  backed by new entrypoint-leading indexes (#668). Applied by `pgq upgrade`.
- Upgrade migration uses the configured status type rather than the default.
- Tracing: one header per entrypoint when the SDK is absent;
  `SentryTracing.trace_process` re-raises instead of swallowing.

## v1.0.2

- Stale-job recovery no longer blocked once an entrypoint is at its concurrency
  limit (#633).
- Jobs canceled by `SIGTERM` are recorded as `canceled` (#631).

## v1.0.1

- Use the selector event loop on Windows for psycopg (#629).

---

## v1.0.0

The first stable release. It cleans up the API surface, enforces the hexagonal
architecture, and removes deprecated code paths.

If you are upgrading from v0.26.x, expect a one-time migration effort. The
checklist at the end of this section covers every change.

### Breaking Changes

#### 1. Sync entrypoints removed: all job handlers must use `async def`

Synchronous entrypoint functions (plain `def`) are no longer supported. Registering one
raises `TypeError` immediately at decoration time with a message guiding you to the fix.

**Before (v0.26.x):**

```python
@pgq.entrypoint("resize_image")
def resize_image(job: Job) -> None:
    img = cpu_bound_resize(job.payload)
```

**After (v1.0.0):**

```python
import asyncio

@pgq.entrypoint("resize_image")
async def resize_image(job: Job) -> None:
    await asyncio.to_thread(cpu_bound_resize, job.payload)
```

**How to migrate:**

- Change every `def handler(job)` to `async def handler(job)`.
- Wrap blocking or CPU-bound calls with `await asyncio.to_thread(fn, ...)`.
- If you used `anyio.from_thread.run()` to call async code from sync handlers,
  remove it. Handlers now always run in an async context.
- Remove imports of `SyncEntrypoint` and `SyncContextEntrypoint`. Both are deleted.

The `Entrypoint` type alias is now `AsyncEntrypoint | AsyncContextEntrypoint`
(previously a 4-variant union that included the sync types).

#### 2. Factory functions must be async context managers

`pgq run` now requires factory functions to return an async context manager. Plain
`async def` functions that return a value, and sync `@contextmanager` factories, are
rejected with a `TypeError` that includes migration instructions.

**Before (v0.26.x, any of these worked):**

```python
# Plain async function
async def factory() -> PgQueuer:
    return PgQueuer(...)

# Sync context manager
@contextmanager
def factory():
    yield PgQueuer(...)
```

**After (v1.0.0, only this form is accepted):**

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

@asynccontextmanager
async def factory() -> AsyncGenerator[PgQueuer, None]:
    connection = await asyncpg.connect(DSN)
    pgq = PgQueuer(AsyncpgDriver(connection))

    @pgq.entrypoint("my_job")
    async def process(job: Job) -> None: ...

    yield pgq
    await connection.close()  # cleanup runs on shutdown
```

**How to migrate:**

- Add the `@asynccontextmanager` decorator.
- Change `return pgq` to `yield pgq`.
- Move cleanup code after the `yield`. It runs on graceful shutdown.
- If you imported `run_factory`, replace it with `validate_factory_result`
  (new name, new behavior: it validates the type but no longer converts it).

#### 3. Removed public exports

| Removed from `pgqueuer.executors`   | Replacement              |
| ----------------------------------- | ------------------------ |
| `SyncEntrypoint`                    | `AsyncEntrypoint`        |
| `SyncContextEntrypoint`             | `AsyncContextEntrypoint` |

| Removed from `pgqueuer.factories`   | Replacement                |
| ----------------------------------- | -------------------------- |
| `run_factory()`                     | `validate_factory_result()` |

#### 4. Database schema changes: re-run `pgq install` or `pgq upgrade`

Two schema additions are needed for the new retry and hold features:

```sql
ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TYPE pgqueuer_status ADD VALUE IF NOT EXISTS 'failed';
```

Both `pgq install` and `pgq upgrade` apply these automatically. If you manage
schema manually, apply these migrations before starting upgraded workers.
Table and type identifiers reflect the default unprefixed names; if you set
`--prefix` / `PGQUEUER_PREFIX`, substitute your prefixed names accordingly.

#### 5. Removed `requests_per_second` rate limiting

The per-entrypoint `requests_per_second` parameter and the underlying RPS tracking
infrastructure have been removed. The feature was flaky: observed RPS came from
recent samples and diverged from actual throughput under load, so throttling was
unpredictable.

**What was removed:**

- `requests_per_second` parameter from `@pgq.entrypoint()` and `QueueManager.entrypoint()`
- `observed_requests_per_second()` method from `QueueManager`
- `RequestsPerSecondEvent` model and `requests_per_second_event` event type
- `RequestsPerSecondBuffer` from `core.buffers`
- `notify_entrypoint_rps()` from the queries layer
- `samples` field from `EntrypointStatistics`

**How to migrate:**

- Remove any `requests_per_second=...` arguments from `@pgq.entrypoint()` calls.
- Remove any calls to `qm.observed_requests_per_second()`.
- Use `concurrency_limit` to control throughput instead. It gives deterministic
  backpressure without measurement-based estimation.

#### 6. Deprecated executor parameter fields removed

The 4 deprecated sentinel fields (`channel`, `connection`, `queries`, `shutdown`)
on `EntrypointExecutorParameters` and the 3 deprecated fields (`connection`,
`queries`, `shutdown`) on `ScheduleExecutorFactoryParameters` have been removed.

These fields were deprecated with `DeprecationWarning` in a prior release and were
unused by all built-in executors. Custom executors that passed these fields will see
a `TypeError` on construction.

**How to migrate:** Remove these keyword arguments from any custom
`EntrypointExecutorParameters(...)` or `ScheduleExecutorFactoryParameters(...)` calls.

#### 7. `PGChannel` type alias removed

The `PGChannel` alias (which was just `PGChannel = Channel`) has been removed from
`pgqueuer.domain.types`, `pgqueuer.models`, and `pgqueuer.types`.

**How to migrate:** Replace `PGChannel` with `Channel` everywhere.

#### 8. `statistics_table_status_type` removed from `DBSettings`

The `DBSettings.statistics_table_status_type` field (marked `TODO: Remove`) has been
removed. It was only used in the `pgq uninstall` teardown query, which now uses
`add_prefix()` directly.

**How to migrate:** Remove any references to `DBSettings().statistics_table_status_type`.

#### 9. `AbstractScheduleExecutor.execute()` signature changed

The `execute()` method on `AbstractScheduleExecutor` now takes a second parameter:

```python
# Before
async def execute(self, schedule: Schedule) -> None: ...

# After
async def execute(self, schedule: Schedule, context: ScheduleContext) -> None: ...
```

**How to migrate:** Add `context: ScheduleContext` to any custom schedule executor's
`execute()` method. You can ignore the parameter if you don't need shared resources.

#### 10. Tracing singleton moved from adapters to ports

`TracingConfig`, `TRACER`, and `set_tracing_class()` moved from
`pgqueuer.adapters.tracing` to `pgqueuer.ports.tracing`. The adapter module
no longer re-exports them (see breaking change #18). Use
`from pgqueuer.ports.tracing import TracingConfig` instead.

#### 11. Internal shim modules removed from package root

14 backward-compatibility shim modules have been removed from the `pgqueuer/`
package root. These exposed internal implementation details. If you imported from
any of these paths, update to the canonical location:

| Removed module          | Canonical import                                  |
| ----------------------- | ------------------------------------------------- |
| `pgqueuer.buffers`      | `pgqueuer.core.buffers`                           |
| `pgqueuer.cache`        | `pgqueuer.core.cache`                             |
| `pgqueuer.cli`          | `pgqueuer.adapters.cli.cli`                       |
| `pgqueuer.completion`   | `pgqueuer.core.completion`                        |
| `pgqueuer.heartbeat`    | `pgqueuer.core.heartbeat`                         |
| `pgqueuer.helpers`      | Removed entirely (see Other Changes)              |
| `pgqueuer.listeners`    | `pgqueuer.core.listeners`                         |
| `pgqueuer.logconfig`    | `pgqueuer.core.logconfig`                         |
| `pgqueuer.qb`           | `pgqueuer.domain.settings` / `pgqueuer.adapters.persistence.qb` |
| `pgqueuer.query_helpers` | `pgqueuer.adapters.persistence.query_helpers`    |
| `pgqueuer.retries`      | Removed entirely (see breaking change #14)        |
| `pgqueuer.supervisor`   | `pgqueuer.adapters.cli.supervisor`                |
| `pgqueuer.tm`           | `pgqueuer.core.tm`                                |
| `pgqueuer.tracing`      | `pgqueuer.ports.tracing` + `pgqueuer.adapters.tracing.*` |

Public API shims (`pgqueuer.models`, `pgqueuer.queries`, `pgqueuer.executors`,
`pgqueuer.errors`, `pgqueuer.db`, `pgqueuer.qm`, `pgqueuer.sm`,
`pgqueuer.applications`, `pgqueuer.factories`, `pgqueuer.types`) are unchanged.

#### 12. `serialized_dispatch` parameter removed

The `serialized_dispatch` parameter has been removed from `@pgq.entrypoint()`,
`QueueManager.entrypoint()`, `PgQueuer.entrypoint()`, and
`EntrypointExecutorParameters`. Use `concurrency_limit=1` instead, which provides
the same one-at-a-time semantics but enforced at the database level.

```python
# Before
@pgq.entrypoint("my_job", serialized_dispatch=True)

# After
@pgq.entrypoint("my_job", concurrency_limit=1)
```

#### 13. Concurrency limit is now global (database-enforced)

`concurrency_limit` on entrypoints is now enforced globally at the database level
via the dequeue SQL query, not per-worker via in-memory semaphores. This means
the limit applies across all workers, not just within a single process.

The `entrypoint()` decorator API is unchanged; you still pass
`concurrency_limit=N`. Enforcement is stricter: if you set
`concurrency_limit=5`, at most 5 jobs run across your entire fleet, not 5 per
worker.

#### 14. `RetryManager` removed

The `RetryManager` class (`pgqueuer.core.retries`) has been deleted. It was an
internal retry-with-backoff wrapper used by buffers. If you imported it directly,
remove the import. `TimedOverflowBuffer` now handles retry logic inline.

#### 15. Buffer API: callbacks replaced with port injection

`TimedOverflowBuffer` no longer accepts a `callback` parameter. Instead,
subclasses override the `flush_items()` template method and inject a repository
port. This only affects users who subclassed `TimedOverflowBuffer`,
`JobStatusLogBuffer`, or `HeartbeatBuffer` directly.

#### 16. `retry_timer` replaced with global `heartbeat_timeout`

The per-entrypoint `retry_timer` parameter has been removed from `@pgq.entrypoint()`,
`QueueManager.entrypoint()`, `PgQueuer.entrypoint()`, and
`EntrypointExecutorParameters`. It is replaced by a single `heartbeat_timeout`
parameter on `QueueManager.run()` / `PgQueuer.run()` (default: 30 seconds).

Previously, each entrypoint could set its own timer controlling when stale "picked"
jobs became eligible for re-pickup. Now a single global timeout applies to all
entrypoints. Heartbeats are sent automatically at half the timeout interval.

```python
# Before
@pgq.entrypoint("send_email", retry_timer=timedelta(seconds=60))
async def send_email(job: Job) -> None: ...

await pgq.run(dequeue_timeout=timedelta(seconds=5), batch_size=10)

# After
@pgq.entrypoint("send_email")  # retry_timer removed
async def send_email(job: Job) -> None: ...

await pgq.run(
    dequeue_timeout=timedelta(seconds=5),
    batch_size=10,
    heartbeat_timeout=timedelta(seconds=60),  # global, applies to all entrypoints
)
```

**How to migrate:**

- Remove `retry_timer=...` from all `@pgq.entrypoint()` calls.
- Add `heartbeat_timeout=...` to your `pgq.run()` call if the default of 30s is
  not suitable. If you had different retry timers per entrypoint, use the maximum.
- Note: stale job retries are now always enabled (previously `retry_timer=0`
  disabled them).

#### 17. `RetryWithBackoffEntrypointExecutor` removed

The in-process retry executor `RetryWithBackoffEntrypointExecutor` has been removed
along with its associated exceptions `MaxRetriesExceeded` and `MaxTimeExceeded`, and
the `async-timeout` dependency.

**How to migrate:** Use `DatabaseRetryEntrypointExecutor` instead (retries at the
database level, surviving worker restarts):

```python
# Before
from pgqueuer.executors import RetryWithBackoffEntrypointExecutor

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

#### 18. Tracing adapter re-exports removed

`pgqueuer.adapters.tracing` no longer re-exports `TracingConfig`, `TRACER`,
`set_tracing_class()`, or `TracingProtocol`. Import from `pgqueuer.ports.tracing`
instead:

```python
# Before
from pgqueuer.adapters.tracing import TRACER, set_tracing_class

# After
from pgqueuer.ports.tracing import TRACER, set_tracing_class
```

#### 19. `log_statistics()` parameter renamed: `tail` → `limit`

The `tail` parameter on `Queries.log_statistics()` has been renamed to `limit` for
consistency with other methods.

```python
# Before
stats = await queries.log_statistics(tail=100)

# After
stats = await queries.log_statistics(limit=100)
```

Positional calls (`log_statistics(100)`) are unaffected.

#### 20. CLI connection options simplified, `dsn()` helper removed

The 6 individual connection CLI options (`--pg-host`, `--pg-port`, `--pg-user`,
`--pg-database`, `--pg-password`, `--pg-schema`) and the `dsn()` helper function
have been removed. Both asyncpg and psycopg natively read standard libpq
environment variables (`PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGPORT`)
when no DSN is provided.

**What was removed:**

- CLI options: `--pg-host`, `--pg-port`, `--pg-user`, `--pg-database`,
  `--pg-password`, `--pg-schema`
- `dsn()` function from `pgqueuer.adapters.drivers` and `pgqueuer.db`
- `AppConfig` fields: `pg_host`, `pg_port`, `pg_user`, `pg_database`,
  `pg_password`, `pg_schema`

**What stays:**

- `--pg-dsn` / `PGDSN`: pass a full connection string
- `--prefix` / `PGQUEUER_PREFIX`: prefix for PgQueuer database objects

**How to migrate:**

```bash
# Before
pgq --pg-host localhost --pg-user myuser --pg-database mydb install

# After — use standard libpq env vars
PGHOST=localhost PGUSER=myuser PGDATABASE=mydb pgq install

# Before
pgq --pg-schema myschema install

# After — use standard PGOPTIONS env var
PGOPTIONS="-csearch_path=myschema" pgq install

# Before (in code)
from pgqueuer.db import dsn
connection = await asyncpg.connect(dsn())

# After — asyncpg reads env vars natively
connection = await asyncpg.connect()
```

#### 21. `QueueManager` and `SchedulerManager` constructor signature changed

`QueueManager` and `SchedulerManager` no longer accept a `connection` parameter.
The first positional argument is now `queries` (a `RepositoryPort`), and the
database driver is accessed via `queries.driver`. This eliminates redundant
double-passing of both a driver and a queries object wrapping that same driver.

`CompletionWatcher` similarly now requires a `queries` keyword argument instead
of constructing one internally.

**Before (v0.26.x):**

```python
from pgqueuer.db import AsyncpgDriver
from pgqueuer.qm import QueueManager
from pgqueuer.sm import SchedulerManager

driver = AsyncpgDriver(connection)
qm = QueueManager(driver)
sm = SchedulerManager(driver)
```

**After (v1.0.0):**

```python
from pgqueuer.db import AsyncpgDriver
from pgqueuer.qm import QueueManager
from pgqueuer.sm import SchedulerManager
from pgqueuer.queries import Queries

driver = AsyncpgDriver(connection)
queries = Queries(driver)
qm = QueueManager(queries)
sm = SchedulerManager(queries)
```

**How to migrate:**

- Replace `QueueManager(driver)` with `QueueManager(Queries(driver))`.
- Replace `SchedulerManager(driver)` with `SchedulerManager(Queries(driver))`.
- Replace `CompletionWatcher(driver)` with
  `CompletionWatcher(driver, queries=Queries(driver))`.
- If you accessed `qm.connection`, use `qm.queries.driver` instead.
- If you accessed `sm.connection`, use `sm.queries.driver` instead.
- `PgQueuer(driver)` is unchanged. It still accepts a driver and creates
  `Queries` internally.

#### 22. `TaskManagerPort` protocol added to ports layer

The `Driver` protocol's `tm` property now returns `TaskManagerPort` (defined in
`pgqueuer.ports.driver`) instead of the concrete `TaskManager` class. This is
only relevant if you implemented a custom driver and type-annotated the `tm`
property with `TaskManager` explicitly.

**How to migrate:** Change the return type annotation from `TaskManager` to
`TaskManagerPort`, or rely on structural subtyping (no annotation needed).

#### 23. `Driver` protocol requires `notify()` method

`pg_notify` has been moved out of the queries layer and into the `Driver`
protocol. Drivers now send `NOTIFY` directly via a new abstract method instead
of the queries layer building a raw SQL string. The
`pgqueuer.adapters.persistence.qb.build_notify_query()` helper has been removed.

**Why:** `pg_notify` is a transport concern that pairs with `add_listener`,
which already lived on `Driver`. Keeping it in the queries layer violated the
ports/adapters boundary, forced the queries layer to know about NOTIFY
mechanics, and required `build_notify_query()` to construct raw SQL by string
interpolation. Each driver now uses its native notify API with parameterized
arguments, which is safer and easier to maintain. Custom drivers and the
in-memory adapter can also intercept notifications directly instead of parsing
emitted SQL.

This breaks any custom async `Driver` implementation, which must now implement
the new `notify()` coroutine. Built-in async drivers (`AsyncpgDriver`,
`AsyncpgPoolDriver`, `PsycopgDriver`, and the in-memory driver) have been
updated. `SyncPsycopgDriver` is unaffected because it implements the separate
`SyncDriver` protocol, which has no `notify()` method.

**New method signature:**

```python
class Driver(Protocol):
    async def notify(self, channel: str, payload: str) -> None:
        """Send a NOTIFY on *channel* with *payload*."""
        ...
```

**How to migrate:** Add a `notify()` implementation to your custom driver. For
example, with psycopg:

```python
async def notify(self, channel: str, payload: str) -> None:
    async with self.connection.cursor() as cur:
        await cur.execute("SELECT pg_notify(%s, %s)", (channel, payload))
```

Remove any imports of `build_notify_query` from
`pgqueuer.adapters.persistence.qb`.

---

### New Features

#### Database-level job retry via `RetryRequested`

Raise `RetryRequested` from any handler to re-queue a job instead of failing it.
The job keeps its row and ID in the queue table, its `attempts` counter is
incremented, and it becomes eligible for processing again after an optional delay.

```python
from pgqueuer import RetryRequested
from datetime import timedelta

@pgq.entrypoint("call_api")
async def call_api(job: Job) -> None:
    response = await http_client.post(url, data=job.payload)
    if response.status == 429:
        raise RetryRequested(delay=timedelta(seconds=30), reason="rate limited")
```

The new `job.attempts` field (`int`, default `0`) tracks how many retries have
occurred, so handlers can implement custom backoff or give-up logic.

#### Automatic exponential backoff via `DatabaseRetryEntrypointExecutor`

Wraps any handler with automatic retry and exponential backoff. Any unhandled
exception (except `RetryRequested`, which passes through unchanged) is converted
into a `RetryRequested` with a computed delay. After `max_attempts` consecutive
failures the original exception propagates as a terminal failure.

```python
from pgqueuer import DatabaseRetryEntrypointExecutor
from datetime import timedelta

@pgq.entrypoint(
    "flaky_api",
    executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
        parameters=params,
        max_attempts=5,                       # default: 5
        initial_delay=timedelta(seconds=1),   # default: 1s
        max_delay=timedelta(minutes=5),       # default: 5m
        backoff_multiplier=2.0,               # default: 2.0
    ),
)
async def flaky_api(job: Job) -> None:
    await call_unreliable_service(job.payload)
```

#### Hold failed jobs for manual re-queue (`on_failure="hold"`)

Set `on_failure="hold"` on an entrypoint to keep terminally failed jobs in the
queue table with `status='failed'` instead of deleting them. They are skipped by
the dequeue query and can be inspected and re-queued later.

```python
@pgq.entrypoint("process_payment", on_failure="hold")
async def process_payment(job: Job) -> None:
    await payment_gateway.charge(job.payload)
```

This combines with `DatabaseRetryEntrypointExecutor`: jobs are held only after
all retry attempts are exhausted.

Invalid `on_failure` values (e.g. typos like `on_failure="retry"`) raise
`ValueError` immediately at decoration time.

**New CLI commands:**

```bash
pgq failed                  # list up to 25 held jobs
pgq failed -n 100           # list up to 100
pgq requeue 42 43 44        # re-queue specific job IDs
```

**Programmatic access:**

```python
failed = await queries.list_failed_jobs(limit=25)
await queries.requeue_jobs([job.id for job in failed])
```

#### Forward CLI args to factory functions

Pass arguments to your factory function using `--` in the CLI:

```bash
pgq run myapp:factory -- --region us-east-1 --workers 4
```

The factory receives them as a `list[str]`:

```python
@asynccontextmanager
async def factory(args: list[str]) -> AsyncGenerator[PgQueuer, None]:
    # args == ["--region", "us-east-1", "--workers", "4"]
    ...
    yield pgq
```

Factories that don't need extra args continue to work unchanged.

#### `ScheduleContext` for shared resources in scheduled tasks

Scheduled task handlers can now receive shared resources via `ScheduleContext`,
matching the `Context.resources` pattern used by queue job handlers.

```python
from pgqueuer.models import Schedule, ScheduleContext

pgq = PgQueuer(driver, resources={"http": http_client})

@pgq.schedule("refresh_cache", "*/5 * * * *", accepts_context=True)
async def refresh_cache(schedule: Schedule, ctx: ScheduleContext) -> None:
    await ctx.resources["http"].get("https://api.example.com/ping")
```

Handlers registered without `accepts_context` continue to work with just the
schedule argument. Previously, the only way to access resources from scheduled tasks
was via closure over `pgq.resources`.

#### Read-only MCP server for AI agent integration

PgQueuer now ships an optional Model Context Protocol server with 11 read-only
tools for inspecting queue state, worker health, failures, throughput, schedules,
and schema info.

```bash
pip install pgqueuer[mcp]    # adds mcp>=1.0, asyncpg>=0.30.0
python -m pgqueuer.adapters.mcp
```

Available tools: `queue_size`, `queue_table_info`, `queue_stats`,
`throughput_summary`, `failed_jobs`, `queue_log`, `schedules`, `stale_jobs`,
`active_workers`, `queue_age`, `schema_info`.

Connection uses standard libpq environment variables (`PGHOST`, `PGPORT`,
`PGUSER`, `PGPASSWORD`, `PGDATABASE`) or an explicit DSN passed to
`create_mcp_server(dsn="postgresql://...")`.

Compatible with Claude Desktop, Claude Code, Cursor, and any MCP client. See
[MCP Server docs](docs/integrations/mcp-server.md) for configuration examples.

---

### Bug Fixes

- **Deferred jobs no longer delayed by `dequeue_timeout`:** Jobs scheduled with
  `execute_after` now wake up within ~100ms of their eligible time. Previously
  they could wait up to the full `dequeue_timeout` (default 30s). The queue
  manager now queries the ETA of the next deferred job and shortens its wait
  accordingly.

- **TOCTOU race with deferred jobs:** Fixed a race condition where a job becoming
  eligible between the dequeue attempt and the ETA query would be missed, causing
  an unnecessary full-timeout sleep. The manager now falls back to a 100ms poll
  interval when queued work exists but no future-deferred jobs are found.

- **In-memory adapter `dedupe_key` leak for held/failed jobs:** The in-memory
  adapter now correctly releases the `dedupe_key` when a job is held with
  `status='failed'`, and validates the `'failed'` enum value at startup.

- **Typo: `peak_schedule` renamed to `peek_schedule`:** The method
  `Queries.peak_schedule()` and `ScheduleRepositoryPort.peak_schedule()` were
  renamed to `peek_schedule()` to fix the misspelling.

- **`id` columns widened to `BIGINT`
  ([#671](https://github.com/janbjorge/pgqueuer/issues/671)):** The `queue`,
  `statistics`, and `schedules` primary keys were `int4 SERIAL`, capped at
  2,147,483,647. Because the sequence only climbs and never reuses values, a
  long-lived deployment would eventually exhaust it and every `enqueue` would
  fail. Fresh installs now use `BIGSERIAL`; `pgq upgrade` widens existing `int4`
  columns in place, along with their legacy `SERIAL` sequences (created
  `AS INTEGER`, so they would otherwise still cap at 2^31-1).

  > **Migration takes an `ACCESS EXCLUSIVE` lock.** Widening `int4` → `BIGINT`
  > rewrites the whole table and rebuilds its indexes. For the duration of that
  > rewrite Postgres holds an `ACCESS EXCLUSIVE` lock, which blocks **everything**
  > on the table: enqueues, dequeues, even plain `SELECT`. The rewrite time
  > scales with row count, so on a large or bloated `queue` table this can be a
  > multi-second-to-minutes stall. Additionally, while the migration *waits* to
  > acquire the lock, new queries queue up behind it, so a single long-running
  > transaction can freeze the queue for the whole wait. **Run `pgq upgrade`
  > during a maintenance window or low-traffic period**, or `pgq upgrade
  > --no-widen-id` to skip the blocking widen and apply it out-of-band. The
  > migration is idempotent (guarded on the column/sequence type), so it is a
  > no-op once everything is `BIGINT` and safe to re-run.

---

### Other Changes

- Moved `TracingConfig`, `TRACER`, and `set_tracing_class()` from
  `pgqueuer.adapters.tracing` to `pgqueuer.ports.tracing` (resolves core→adapter
  import violation).
- Simplified `TimedOverflowBuffer` internals by removing the exponential backoff
  retry machinery in favor of a simple re-queue on flush failure.
- Concurrency enforcement moved from per-worker semaphores to database-level
  `FOR UPDATE SKIP LOCKED` with row counting, so limits are correct across the
  fleet.
- Consolidated all agent/AI guidance into `AGENTS.md` (previously split between
  `CLAUDE.md` and `AGENTS.md`).
- Replaced PNG logo with SVG PQ monogram in docs.
- Fixed Mermaid diagrams for light/dark mode readability, then replaced them with
  ASCII art to eliminate text overlap.
- Removed `examples/callable_factory/` directory (outdated; see
  `examples/consumer.py` for current factory patterns).
- Reduced test suite from 695 to 599 tests by removing duplicate and
  over-parametrized cases.
- Fixed docs CI workflow runner label.
- Added `OnFailure` type to `pgqueuer.types` re-exports.
- Added OpenTelemetry section to the tracing integration guide.
- Removed `async-timeout` from runtime dependencies (kept as a dev/test
  dependency until tests migrate to `asyncio.timeout`).
- Removed dead internal helpers: `ExponentialBackoff`, `timer()`,
  `retry_timer_buffer_timeout()`.
- Removed dead `EntrypointStatistics` class from `pgqueuer.domain.models`.
- Added `has_function()` and `has_trigger()` to `SchemaManagementPort` for
  schema introspection.
- Deleted `pgqueuer/core/helpers.py`. Its functions moved to their natural
  modules (`listeners.py`, `executors.py`, `query_helpers.py`, etc.).
- Removed `dsn()` helper from `pgqueuer.adapters.drivers` and `pgqueuer.db`. Use
  `asyncpg.connect()` or `psycopg.connect("")`, which read libpq env vars
  natively.
- Consolidated `utc_now()` into a single utility in `pgqueuer.domain.models`.
- Batched scheduler heartbeat updates for reduced DB round-trips.
- Added PR title lint for Conventional Commits enforcement in CI.
- Added `TaskManagerPort` protocol to `pgqueuer.ports.driver`, replacing the
  concrete `TaskManager` import that violated the ports→core boundary.
- `QueueManager` and `SchedulerManager` no longer auto-create `Queries`.
  `PgQueuer.__post_init__` is the sole wiring point that constructs concrete
  adapter instances.
- Import-linter contracts expanded to 4: domain, ports, core, and metrics layers
  are all independently validated. Only `core.applications` (the composition
  root) retains adapter import exceptions.
- Guarded `asyncio.Future` state transitions against race conditions.
- Simplified `listener_healthy` timeout to raise `FailingListenerError` directly.

---

### Migration Checklist

1. **Schema:** Run `pgq install` or `pgq upgrade`. Both add the `attempts`
   column and `'failed'` status enum value automatically. `pgq upgrade` also
   widens the `int4` `id` columns to `BIGINT` (#671). That takes an `ACCESS
   EXCLUSIVE` lock and rewrites each table, so run it in a maintenance window on
   large tables (or `pgq upgrade --no-widen-id` and widen out-of-band).
2. **Sync handlers:** Convert all `def handler(job)` to `async def handler(job)`.
   Wrap blocking calls with `await asyncio.to_thread(...)`.
3. **Factory functions:** Convert to `@asynccontextmanager` with `yield` instead
   of `return`.
4. **RPS removal:** Remove any `requests_per_second=...` arguments from
   `@pgq.entrypoint()` calls and any `observed_requests_per_second()` usage.
   Use `concurrency_limit` instead.
5. **Deprecated fields:** Remove `channel`, `connection`, `queries`, `shutdown`
   kwargs from any custom `EntrypointExecutorParameters` or
   `ScheduleExecutorFactoryParameters` constructor calls.
6. **`PGChannel`:** Replace with `Channel`.
7. **`statistics_table_status_type`:** Remove any references to this `DBSettings` field.
8. **Custom schedule executors:** Add `context: ScheduleContext` parameter to
   your `execute()` method.
9. **`serialized_dispatch`:** Replace `serialized_dispatch=True` with
   `concurrency_limit=1`.
10. **Concurrency semantics:** `concurrency_limit` is now global across all
    workers (database-enforced), not per-process. Review limits if you relied on
    per-worker behavior.
11. **Internal imports:** If you imported from `pgqueuer.buffers`, `pgqueuer.qb`,
    `pgqueuer.helpers`, etc., update to canonical paths (see table above).
12. **`RetryManager`:** Remove any imports of `RetryManager` from
    `pgqueuer.core.retries`. The module is deleted.
13. **Custom buffers:** If you subclassed `TimedOverflowBuffer`, replace
    `callback` parameter with a `flush_items()` method override.
14. **`retry_timer`:** Remove `retry_timer=...` from all `@pgq.entrypoint()` calls.
    Add `heartbeat_timeout=...` to `pgq.run()` if the 30s default doesn't fit. If
    you had varying per-entrypoint timers, use the maximum value.
15. **`RetryWithBackoffEntrypointExecutor`:** Replace with
    `DatabaseRetryEntrypointExecutor`. Remove imports of `MaxRetriesExceeded`
    and `MaxTimeExceeded`.
16. **Tracing imports:** Change `from pgqueuer.adapters.tracing import ...` to
    `from pgqueuer.ports.tracing import ...`.
17. **`log_statistics(tail=...)`:** Rename keyword to `limit=...`.
18. **CLI connection options:** Replace `--pg-host`, `--pg-user`, etc. with
    standard libpq env vars (`PGHOST`, `PGUSER`, etc.). Replace `--pg-schema`
    with `PGOPTIONS="-csearch_path=..."`. Replace `from pgqueuer.db import dsn`
    with `asyncpg.connect()` (no args).
19. **Removed imports:** Delete any imports of `SyncEntrypoint`,
    `SyncContextEntrypoint`, `run_factory`, or `dsn`.
20. **`QueueManager` / `SchedulerManager` constructors:** Replace
    `QueueManager(driver)` with `QueueManager(Queries(driver))`. Replace
    `SchedulerManager(driver)` with `SchedulerManager(Queries(driver))`.
    Replace `qm.connection` / `sm.connection` with `qm.queries.driver` /
    `sm.queries.driver`. `PgQueuer(driver)` is unchanged.
21. **`CompletionWatcher`:** Replace `CompletionWatcher(driver)` with
    `CompletionWatcher(driver, queries=Queries(driver))`.
22. **Custom `Driver` implementations:** If you annotated the `tm` property
    return type as `TaskManager`, change it to `TaskManagerPort` from
    `pgqueuer.ports.driver` (or remove the annotation; structural subtyping
    handles it).
23. **Custom async `Driver` implementations, `notify()`:** Implement the new
    `notify(channel, payload)` coroutine on any custom async driver.
    (`SyncDriver` is unaffected.) Remove imports of `build_notify_query` from
    `pgqueuer.adapters.persistence.qb`.
24. **Test:** Run your test suite. Breaking changes surface at decoration/startup
    time, so problems are immediately visible.
