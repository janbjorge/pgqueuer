# Ports & Adapters for PgQueuer

## Status

Accepted

## Context

PgQueuer's flat package layout (~27 modules) has grown organically. Job lifecycle management, rate limiting, and scheduling logic live inside `QueueManager` and `SchedulerManager`, but these classes directly instantiate `Queries` in their `__post_init__` methods and reach into PostgreSQL-specific notification, heartbeat, and buffering infrastructure. This makes it impossible to unit-test core dispatch logic without a running PostgreSQL instance.

Specific coupling problems:

- `QueueManager.__post_init__` hardcodes `self.queries = queries.Queries(self.connection)`.
- `SchedulerManager.__post_init__` does the same.
- `EntrypointExecutorParameters` bundles `connection`, `queries`, `channel`, and `shutdown` alongside executor config (`requests_per_second`, `retry_timer`), even though no executor implementation reads the infrastructure fields.
- `tracing.TRACER` is a module-level mutable singleton instead of an injected dependency.
- The `Queries` class is simultaneously the interface definition and the PostgreSQL implementation.

The `Driver` protocol in `db.py` and `TracingProtocol` in `tracing.py` already demonstrate that protocol-based boundaries work well in this codebase. This document extends that pattern to the rest of the system.

## Decision

Migrate PgQueuer to a Ports & Adapters structure using Python `Protocol` classes as port definitions and dependency injection to wire adapters. The migration is incremental (strangler fig) and must not break the public API.

### Constraint: Zero Unnecessary Breaking Changes

The following imports must continue to work throughout all phases:

```python
from pgqueuer import (
    PgQueuer, QueueManager, SchedulerManager, Queries,
    Job, JobId, AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver,
)
```

The decorator APIs `.entrypoint()` and `.schedule()` must remain stable. When modules move to new paths, thin re-export shims preserve the old import locations.

### Port Definitions

Ports are `Protocol` classes living in `pgqueuer/ports/`. They capture what the core needs without specifying how.

**QueueRepositoryPort** -- persistence for the job queue:

```python
# pgqueuer/ports/repository.py
class QueueRepositoryPort(Protocol):
    async def dequeue(self, batch_size, entrypoints, queue_manager_id, global_concurrency_limit) -> list[Job]: ...
    async def enqueue(self, entrypoint, payload, priority, ...) -> list[JobId]: ...
    async def log_jobs(self, job_status) -> None: ...
    async def clear_queue(self, entrypoint=None) -> None: ...
    async def queue_size(self) -> list[QueueStatistics]: ...
    async def mark_job_as_cancelled(self, ids) -> None: ...
    async def update_heartbeat(self, job_ids) -> None: ...
    async def queued_work(self, entrypoints) -> int: ...
```

**ScheduleRepositoryPort** -- persistence for cron schedules:

```python
class ScheduleRepositoryPort(Protocol):
    async def insert_schedule(self, schedules) -> None: ...
    async def fetch_schedule(self, entrypoints) -> list[Schedule]: ...
    async def set_schedule_queued(self, ids) -> None: ...
    async def update_schedule_heartbeat(self, ids) -> None: ...
    async def delete_schedule(self, ids, entrypoints) -> None: ...
```

**NotificationPort** -- PostgreSQL NOTIFY abstraction:

```python
class NotificationPort(Protocol):
    async def notify_entrypoint_rps(self, entrypoint_count) -> None: ...
    async def notify_job_cancellation(self, ids) -> None: ...
    async def notify_health_check(self, health_check_event_id) -> None: ...
```

**SchemaManagementPort** -- DDL operations:

```python
class SchemaManagementPort(Protocol):
    async def install(self) -> None: ...
    async def uninstall(self) -> None: ...
    async def upgrade(self) -> None: ...
    async def has_table(self, table) -> bool: ...
    async def table_has_column(self, table, column) -> bool: ...
    async def table_has_index(self, table, index) -> bool: ...
    async def has_user_defined_enum(self, key, enum) -> bool: ...
```

The existing `Queries` class satisfies all four ports via structural subtyping. No inheritance or registration required.

### Existing Ports (Already In Place)

- **`Driver`** (`db.py`) -- database execution abstraction. Adapters: `AsyncpgDriver`, `AsyncpgPoolDriver`, `PsycopgDriver`.
- **`TracingProtocol`** (`tracing.py`) -- distributed tracing. Adapters: `LogfireTracing`, `SentryTracing`.

### Dependency Injection Pattern

`QueueManager` and `SchedulerManager` accept an optional `queries` argument. When omitted, they auto-create `Queries(self.connection)` for backward compatibility:

```python
@dataclasses.dataclass
class QueueManager:
    connection: db.Driver
    queries: queries.Queries | None = dataclasses.field(default=None)

    def __post_init__(self) -> None:
        if self.queries is None:
            self.queries = queries.Queries(self.connection)
```

This preserves `QueueManager(connection)` for all existing callers while enabling mock injection for tests.

### Target Directory Layout

```
pgqueuer/
  __init__.py                 # Public API (unchanged)

  domain/
    models.py                 # Job, Schedule, Event, Context, statistics
    types.py                  # JobId, JOB_STATUS, Channel, etc.
    errors.py                 # Exception hierarchy

  ports/
    repository.py             # QueueRepositoryPort, ScheduleRepositoryPort
    notification.py           # NotificationPort
    schema.py                 # SchemaManagementPort
    driver.py                 # Driver protocol
    tracing.py                # TracingProtocol

  core/
    qm.py                     # QueueManager
    sm.py                     # SchedulerManager
    applications.py           # PgQueuer facade
    executors.py              # AbstractEntrypointExecutor, EntrypointExecutor
    listeners.py              # EventRouter, PGNoticeEventListener
    buffers.py                # TimedOverflowBuffer and typed variants
    heartbeat.py, cache.py, helpers.py, retries.py, tm.py, logconfig.py

  adapters/
    drivers/
      asyncpg.py              # AsyncpgDriver, AsyncpgPoolDriver
      psycopg.py              # PsycopgDriver, SyncPsycopgDriver
    persistence/
      queries.py              # Queries class (implements all repository ports)
      qb.py                   # SQL query builders
      query_helpers.py        # Parameter normalization
    tracing/
      logfire.py, sentry.py
    cli/
      cli.py, supervisor.py, factories.py
```

Old module paths (`pgqueuer/db.py`, `pgqueuer/queries.py`, etc.) become thin re-export shims so existing imports keep working.

### Migration Phases

**Phase 0 -- Deprecate unused infrastructure fields in executor parameters.**
Mark `connection`, `queries`, `channel`, `shutdown` on `EntrypointExecutorParameters` and `ScheduleExecutorFactoryParameters` as deprecated. No removal yet.

**Phase 1 -- Extract port protocols.**
Create `pgqueuer/ports/` with Protocol classes. Purely additive, no existing code changes.

**Phase 2 -- Dependency injection for QueueManager and SchedulerManager.**
Change `queries` field from `init=False` to `default=None` with auto-creation fallback. Depends on Phase 1.

**Phase 3 -- Inject tracing instead of global singleton.**
Add optional `tracer` field to `QueueManager` and `Queries`. Falls back to `TRACER` global when not provided.

**Phase 4 -- Directory restructure.**
Move modules into `domain/`, `ports/`, `core/`, `adapters/`. Old paths become re-export shims. One module per commit, full test suite after each move. Leaf dependencies (`types.py`, `models.py`, `errors.py`) move first.

**Phase 5 -- Enforce boundaries.**
Add `import-linter` rules to CI. Add typing conformance tests verifying `Queries` satisfies all port protocols.

Phase dependency graph:

```
Phase 0 ──┐
Phase 1 ──┼──> Phase 2 ──> Phase 4 ──> Phase 5
Phase 3 ──┘
```

Phases 0, 1, and 3 are independently shippable.

## Consequences

### Positive

- `QueueManager` and `SchedulerManager` become unit-testable without PostgreSQL by injecting mock repositories.
- Port protocols document exactly what the core requires, making the contract explicit.
- Alternative persistence backends (e.g., in-memory for testing) can implement the ports without touching core logic.
- The `Driver` protocol pattern, already proven in the codebase, is extended consistently.
- Re-export shims mean the migration is invisible to downstream users.

### Negative

- Re-export shim files add maintenance overhead until a major version removes them.
- Developers must learn the directory conventions and respect import boundaries.
- Protocol definitions add surface area that must stay in sync with the `Queries` implementation.

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Breaking `from pgqueuer import X` | Every moved module gets a re-export shim at the old path |
| Custom executors using deprecated `parameters.connection` | Deprecation warnings first, removal deferred to next major version |
| Circular imports during restructure | Move leaf modules first; one module per commit with full test suite |
| `tracing.TRACER` global removal | Global kept as fallback; injected value takes precedence |
