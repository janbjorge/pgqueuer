# Release Notes

- PgQueuer follows semantic versioning from v1.0.0 onward.
- Schema changes are applied by `pgq install` / `pgq upgrade`. Run the upgrade
  **before** starting workers on a new version; `QueueManager.run()` verifies
  the schema at startup and refuses to boot against an old one.
- Identifiers below use the default unprefixed names; substitute your own if
  you set `--prefix` / `PGQUEUER_PREFIX`.
- The top section is the next release. Add to it under the version the change
  warrants and cut the tag from it when it ships. Release dates live in the git
  tags, so a section is written once and never revisited.

---

## v1.6.0

### Removed

- PostgreSQL 13 support. It reached end of life in November 2025, and CI now
  tests PostgreSQL 14 to 18. If you run PostgreSQL 13, stay on v1.5.0.
- Releases no longer publish the `ghcr.io/janbjorge/pgqueuer-web` and
  `ghcr.io/janbjorge/pgqueuer-prometheus` Docker images. The tags already
  pushed, `1.5.0` and `latest`, stay pullable but get no newer versions. Build
  from `tools/web/Dockerfile` and `tools/prometheus/Dockerfile` instead; see
  the Docker images guide.

## v1.5.0

### Changed: `pgq upgrade` computes what each database needs

- Reads the installed schema from `pg_catalog`, compares it with the schema
  this release declares, and runs only the difference (ADR-0016). A current
  database runs nothing: `PgQueuer schema is already up to date.` v1.4.0 re-ran
  a fixed script every time, which rewrote `pgqueuer_statistics` under
  `ACCESS EXCLUSIVE` and dropped and rebuilt `pgqueuer_log_not_aggregated`.
- Run `pgq upgrade --plan` first. It prints the statements and the `note:`
  lines (index rebuilds, table rewrites) and applies nothing. A plain
  `pgq upgrade` prints its notes only after it has applied.
- Every database coming from v1.4.0 gets:
  - `ALTER TABLE pgqueuer_statistics ALTER COLUMN created SET DEFAULT
    date_trunc('sec', now())`. Catalog only; stored rows are unchanged. The old
    default stored the wrong instant when an insert left out `created` and the
    session `TimeZone` was not UTC.
  - on PostgreSQL 14+, a one-time rebuild of
    `pgqueuer_statistics_unique_count`, because PostgreSQL stores v1.4.0's key
    as `created AT TIME ZONE 'UTC'` and the declaration says
    `timezone('UTC', created)`. It is preceded by a fold of duplicate
    statistics buckets, a full scan that changes nothing on a v1.4.0 table. The
    build holds a `SHARE` lock that blocks writes to `pgqueuer_statistics` for
    a time that scales with row count.
  - if it ever ran `pgq upgrade`, a drop of the retired
    `pgqueuer_heartbeat_id_id1_idx`, which every `pgq upgrade` up to v1.4.0
    created. Brief `ACCESS EXCLUSIVE` lock on `pgqueuer`.
- Depending on its history, a database can also get:
  - missing labels, tables, columns, indexes, the trigger function and the
    trigger, or everything in an empty database;
  - a rebuild of any other index whose definition differs from the declared
    one (`SHARE` lock, reported as a `note:`). The replacement is built beside
    the old index as `pgq_rebuild_*` and swapped in once the build succeeds, so
    a failed build leaves the old index in place. An upgrade interrupted before
    the swap is finished by the next one;
  - `int4` id columns and sequences widened to `BIGINT` (still gated by
    `--widen-id/--no-widen-id`) and statistics `status` moved off the pre-v0.27
    enum. Both rewrite the table under `ACCESS EXCLUSIVE` (reported as a
    `note:`). With `--no-widen-id`, each id column and sequence left narrow is
    a `note:`; v1.4.0 skipped them silently;
  - `NOT NULL` and `DEFAULT` reset on PgQueuer's own columns, reverting hand
    edits;
  - the retired `pgqueuer_statistics_status` enum dropped, and `NOT NULL`
    relaxed on the retired `time_in_queue` column (reported as a `note:`, never
    dropped).
- Notes also flag a missing primary key, or an id column whose kind (serial,
  identity, plain) differs from the declared one, e.g. after its `nextval`
  default was dropped. Nothing is planned for either, so the summary still
  reads `already up to date`.
- Never changes durability. A mismatch with `PGQUEUER_DURABILITY` is a `note:`
  that offers setting the variable to match, or `pgq durability` (which
  rewrites the table).
- A column type it has no conversion for stops it with exit code `1` and one
  line naming the column; alter it by hand and re-run. v1.4.0 ignored such a
  column.
- Upgrades of one installation serialize on an advisory lock, which
  `pgq upgrade` holds on its single connection. Over `AsyncpgPoolDriver` the
  lock is released as soon as it is taken, so it serializes nothing.
- `pgq sql upgrade` still prints a script for operators who apply DDL
  themselves. It can't see the database, so it re-states every object behind
  `IF NOT EXISTS` and covers less than the connected upgrade. Compared with
  v1.4.0's script:
  - it no longer rewrites `pgqueuer_statistics` on every apply, and drops a
    redefined index only when its definition differs;
  - it drops and recreates the `tg_pgqueuer_changed` trigger on every apply.
    Applied outside one transaction, changes made in between send no
    notification;
  - on PostgreSQL 14+, its first apply to a v1.4.0 database drops
    `pgqueuer_statistics_unique_count` and then rebuilds it, not beside the old
    one. Applied outside one transaction, log aggregation fails until the build
    finishes. Every apply also re-runs the statistics fold, a full scan.

### Added

- `pgq upgrade --plan` prints the statements this database needs, headed by a
  comment, and applies nothing. Notes and the summary go to stderr.
- `pgq stale`: picked jobs whose heartbeat is older than `--threshold` seconds
  (default 300).
- `pgq workers`: queue managers currently holding picked jobs.
- `pgq backlog`: count and age of `queued` jobs per entrypoint.
- `--json` on `pgq failed`, `pgq stale`, `pgq workers` and `pgq backlog`.
  `pgq failed --json` reports payloads by size (`payload_bytes`) only.
- `Queries.plan_upgrade()`, `Queries.apply_upgrade()`,
  `Queries.schema_is_installed()` and `Queries.schema_lock()` (the advisory
  lock above). `Queries.upgrade()` logs its notes as warnings.
- `SchemaDriftError` in `pgqueuer.domain.errors` (not re-exported from
  `pgqueuer.errors`): raised by `Queries.upgrade()`, `plan_upgrade()` and
  `apply_upgrade()` for a column type the planner can't convert.
- Docs: CLI exit codes, and an onboarding guide for coding agents.

### Changed

- `pgq upgrade` reports `Applied N statements.` or `PgQueuer schema is already
  up to date.` instead of `Upgraded PgQueuer schema.` Scripts matching the old
  line need updating.
- `pgq upgrade --durability` / `-d` is deprecated: hidden from `--help`, prints
  a warning, and is still ignored. It will be removed in v2.0; use
  `pgq durability`.
- `pgq install` refuses a database that already has this installation, even a
  partial one, and exits `1` naming the prefix and schema. Previously a raw
  `DuplicateObjectError` traceback. Use `pgq upgrade`, which also installs into
  an empty database.
- `pgq sql install` output starts with a comment line naming the release,
  prefix, schema and durability, so checked-in install SQL changes with every
  release.
- `pgq queue` prints the duplicate `dedupe_key` error on stderr, not stdout.
- `enqueue()` rejects one value next to a list of entrypoints, which v1.4.0
  accepted: `enqueue(["a", "b"], None, [0, 0])` now raises `ValueError` for
  `payload`. Pass one value per job, e.g. `[None, None]`.
- MCP `stale_jobs` and `queue_age` return numeric fields as JSON numbers, not
  strings, on PostgreSQL 14+.
- `InsightsService.stale_jobs(threshold=timedelta(0))` returns every picked
  job. Previously a zero threshold fell back to the 5-minute default.
- Workers flush queued heartbeats every eighth of `heartbeat_timeout`, not
  every quarter; each job still queues one every half. With latency under 0.2
  of the timeout, one failed heartbeat write no longer lets a worker re-pick a
  running job. About twice as many batched heartbeat `UPDATE`s; rows written
  per job unchanged. Reasoning in the heartbeat guide.

### Fixed

- Upgrading a v0.18 database with statistics history now leaves log
  aggregation working. Rows split on `time_in_queue` are folded into one per
  bucket, summing `count`, before `pgqueuer_statistics_unique_count` is
  rebuilt, and `NOT NULL` on `time_in_queue` is relaxed. v1.4.0 left the
  pre-v0.19 index and the `NOT NULL` in place. `pgq sql upgrade` does the same.
- `AsyncpgPoolDriver` accepts a one-connection pool. The two-connection minimum
  is checked when it starts listening (which holds one connection for good),
  not at construction.
- With `PGQUEUER_SCHEMA` set, the dashboard's overview, entrypoints, jobs, job
  detail and system pages read that schema's tables. Previously
  `UndefinedTableError` when the schema was not on `search_path`.
- A worker no longer starts a second copy of a job it is still running when the
  job's heartbeat reaches the database late. Its own next dequeue re-picked the
  job as stale.
- `enqueue()` raises `ValueError` naming the argument when batch lists differ in
  length, and inserts nothing. Previously PostgreSQL padded a short list with
  `NULL` (a short `payload` list queued jobs without a payload), and the
  in-memory adapter inserted the first jobs and then raised `IndexError`.

### Removed

- Internal `TTLCache` (`pgqueuer.core.cache`). Drain shutdown probes
  `queued_work` directly, without the 250ms TTL.
- `Queries.qbe.build_widen_id_column_query()` and
  `build_widen_id_sequence_query()`. `pgq upgrade` plans the widening.
- `QueryBuilderEnvironment.build_upgrade_queries()`. Use
  `Queries.plan_upgrade()` / `apply_upgrade()`, or `pgq sql upgrade` for the
  script.
- MCP server: the `Tail`, `TimePeriod`, `StaleThreshold`, `Limit` and `Offset`
  type aliases, and `PgQueuerDatabase.settings` / `.qbs`. Tool schemas are
  unchanged.

## v1.4.0

### Schema change: capacity slots for `concurrency_limit`

- `concurrency_limit` is enforced by capacity slots instead of a row count,
  closing an overshoot when several workers dequeued concurrently (#761, #774,
  #777).
- Adds one column and one partial unique index:

  ```sql
  ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS slot BIGINT;
  CREATE UNIQUE INDEX IF NOT EXISTS pgqueuer_picked_slot_idx
      ON pgqueuer (entrypoint, slot) WHERE (status = 'picked' AND slot IS NOT NULL);
  ```

- **Migrate before deploying workers.** `verify_structure()` requires both, so
  workers on an unmigrated database raise `RuntimeError` on boot.
- The index build holds a `SHARE` lock that blocks writes to the queue table;
  on a large table run it in a low-traffic window.
- Jobs already `picked` during the migration keep `slot IS NULL`, stay under
  the count gate, and drain out normally.
- Workers must agree on an entrypoint's limit. If they disagree the highest
  wins; nothing validates or rejects the mismatch.

### Fixed

- Dequeue no longer overshoots `concurrency_limit`: candidates are windowed
  before locking, so `SKIP LOCKED` can't slide down the backlog past the cap
  (#774).
- `--restart-on-failure` restarts. Each supervisor cycle owns its shutdown
  event, so a manager setting `shutdown` on exit no longer ends the loop.
  Workers that used to exit on failure now restart in place; review external
  restart or alerting that relied on the process dying.

### Added

- `Job.slot`: the capacity seat a picked job holds under `concurrency_limit`;
  `None` for unlimited entrypoints and unpicked rows. Typed as the new `Slot`;
  a plain `int` at runtime.
- Identity types, plain values at runtime:
  - `QueueEntrypoint` (`str`): entrypoint name on `Job`, `Log`, the statistics
    models and `QueueManager.entrypoint_registry`. Schedules keep
    `CronEntrypoint`.
  - `QueueManagerId` (`uuid.UUID`): worker id on `Job`, `StaleJob`,
    `ActiveWorker` and `QueueManager.queue_manager_id`.
  - `HealthCheckId` (`uuid.UUID`): `HealthCheckEvent.id`, distinct from
    `QueueManagerId` at type level.
- All four live in `pgqueuer.domain.types`, re-exported from `pgqueuer.types`
  and `pgqueuer.models`.

### Changed

- Type annotations only, no runtime change. Direct callers need the wrapper to
  pass mypy:
  - `dequeue()`, `queued_work()`, `eligible_queued_work()`,
    `next_deferred_eta()` and `QueryQueueBuilder.build_dequeue_query()` take
    `QueueEntrypoint` (`QueueEntrypoint("name")`), and
    `QueueManager.entrypoint_registry` is keyed by it. `enqueue()`,
    `clear_queue()` and `@entrypoint` still take `str`.
  - `dequeue()` and `build_dequeue_query()` take `queue_manager_id:
    QueueManagerId` (`QueueManagerId(uuid.uuid4())`).
  - `notify_health_check()` takes `HealthCheckId`.
- mypy runs in `strict` mode with `Any` banned in `pgqueuer/`. Tightened public
  signatures:
  - `Driver.fetch()` / `execute()` take `*args: object`; rows are
    `list[dict[str, object]]`;
  - `Job.headers` and `TracebackRecord.additional_context` are
    `dict[str, object]`;
  - `Context.resources` / `ScheduleContext.resources` are
    `MutableMapping[str, object]`, so readers narrow with `isinstance`;
  - `TracingProtocol.trace_publish()` yields `dict[str, object]`;
  - `load_factory()` returns a `Factory` protocol;
  - `EventRouter` takes three typed handlers instead of a `register()`
    decorator;
  - `ScheduleExecutorFactoryParameters` uses `CronEntrypoint` /
    `CronExpression`.
- `Job.headers` also accepts an already-parsed dict, not only JSON text.
- `load_factory()` raises `TypeError` for a non-callable `module:attr`, and
  `pgq` commands raise `TypeError` when a factory yields something other than
  `Queries`. Previously a later `AttributeError`.
- `QueryQueueBuilder.build_dequeue_query()` and `build_log_statistics_query()`
  are keyword-only and return a `ComposedQuery` (`.sql`, `.args`), not a SQL
  string. Internal, but reachable via `Queries.qbq`.
- Dequeue SQL is assembled per active gate by `SqlComposer`; unused concurrency
  gates no longer render (ADR-0024).
- `is_unique_violation()` classifies driver errors by SQLSTATE instead of
  importing asyncpg and psycopg. Drivers without `.sqlstate` are no longer
  recognised.
- `dequeue()` treats a lost slot race (SQLSTATE `23505` or `40P01`) as an empty
  batch; the poll loop retries.

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

- **Schema change.** Ships the `BIGINT` `id` widening described under v1.0.0
  (#676), lifting the ~2.1B lifetime-enqueue ceiling on the queue, statistics
  and schedules tables.
- `pgq upgrade` rewrites each table under an `ACCESS EXCLUSIVE` lock. See the
  v1.0.0 warning, or run `pgq upgrade --no-widen-id` and widen out-of-band.

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

- First stable release: a smaller API surface, an enforced hexagonal
  architecture, and deprecated code paths removed.
- Upgrading from v0.26.x is a one-time migration. The
  [upgrade guide](docs/getting-started/upgrading.md) has before/after code for
  the common cases.
- Most breaks raise at decoration or startup, so a test run surfaces them.

### Breaking changes

1. **Handlers must be `async def`.** A plain `def` entrypoint raises
   `TypeError` at decoration. Wrap blocking or CPU-bound calls in
   `await asyncio.to_thread(fn, ...)`. Handlers always run in an async context,
   so sync handlers that used `anyio.from_thread.run()` to call async code can
   drop it.
   `SyncEntrypoint` and `SyncContextEntrypoint` are deleted; `Entrypoint` is
   `AsyncEntrypoint | AsyncContextEntrypoint`.
2. **Factories must be async context managers.** `pgq run` rejects plain
   `async def` factories and sync `@contextmanager` ones with a `TypeError`.
   Add `@asynccontextmanager`, `yield pgq` instead of `return`, and put cleanup
   after the `yield` (runs on graceful shutdown). `run_factory` is replaced by
   `validate_factory_result`, which validates but no longer converts.
3. **Removed exports.**

   | Removed                                    | Replacement                 |
   | ------------------------------------------ | --------------------------- |
   | `pgqueuer.executors.SyncEntrypoint`        | `AsyncEntrypoint`           |
   | `pgqueuer.executors.SyncContextEntrypoint` | `AsyncContextEntrypoint`    |
   | `pgqueuer.factories.run_factory()`         | `validate_factory_result()` |

4. **Schema.** Retry and hold need two additions, applied by `pgq install` and
   `pgq upgrade`. If you manage DDL yourself, apply them before starting
   upgraded workers:

   ```sql
   ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
   ALTER TYPE pgqueuer_status ADD VALUE IF NOT EXISTS 'failed';
   ```

5. **`requests_per_second` removed.** Observed RPS diverged from real
   throughput under load, so throttling was unpredictable. Gone: the parameter
   on `@pgq.entrypoint()` / `QueueManager.entrypoint()`,
   `QueueManager.observed_requests_per_second()`, `RequestsPerSecondEvent` and
   the `requests_per_second_event` type, `RequestsPerSecondBuffer`,
   `notify_entrypoint_rps()`, and `EntrypointStatistics.samples`. Use
   `concurrency_limit`.
6. **Deprecated executor fields removed.** `channel`, `connection`, `queries`,
   `shutdown` on `EntrypointExecutorParameters`; `connection`, `queries`,
   `shutdown` on `ScheduleExecutorFactoryParameters`. Built-in executors never
   used them; passing them now raises `TypeError`.
7. **`PGChannel` removed** (it was `= Channel`) from `pgqueuer.domain.types`,
   `pgqueuer.models` and `pgqueuer.types`. Use `Channel`.
8. **`DBSettings.statistics_table_status_type` removed.** Only the
   `pgq uninstall` teardown used it; that now calls `add_prefix()`.
9. **`AbstractScheduleExecutor.execute()` takes
   `context: ScheduleContext`** as a second parameter. Ignore it if you don't
   need shared resources.
10. **Tracing singleton moved.** `TracingConfig`, `TRACER` and
    `set_tracing_class()` moved from `pgqueuer.adapters.tracing` to
    `pgqueuer.ports.tracing` (see 18).
11. **Package-root shim modules removed.** Import from the canonical location:

    | Removed module           | Canonical import                                                |
    | ------------------------ | --------------------------------------------------------------- |
    | `pgqueuer.buffers`       | `pgqueuer.core.buffers`                                         |
    | `pgqueuer.cache`         | `pgqueuer.core.cache`                                           |
    | `pgqueuer.cli`           | `pgqueuer.adapters.cli.cli`                                     |
    | `pgqueuer.completion`    | `pgqueuer.core.completion`                                      |
    | `pgqueuer.heartbeat`     | `pgqueuer.core.heartbeat`                                       |
    | `pgqueuer.helpers`       | Removed (see Other changes)                                     |
    | `pgqueuer.listeners`     | `pgqueuer.core.listeners`                                       |
    | `pgqueuer.logconfig`     | `pgqueuer.core.logconfig`                                       |
    | `pgqueuer.qb`            | `pgqueuer.domain.settings` / `pgqueuer.adapters.persistence.qb` |
    | `pgqueuer.query_helpers` | `pgqueuer.adapters.persistence.query_helpers`                   |
    | `pgqueuer.retries`       | Removed (see 14)                                                |
    | `pgqueuer.supervisor`    | `pgqueuer.adapters.cli.supervisor`                              |
    | `pgqueuer.tm`            | `pgqueuer.core.tm`                                              |
    | `pgqueuer.tracing`       | `pgqueuer.ports.tracing` + `pgqueuer.adapters.tracing.*`        |

    Public API shims are unchanged: `pgqueuer.models`, `pgqueuer.queries`,
    `pgqueuer.executors`, `pgqueuer.errors`, `pgqueuer.db`, `pgqueuer.qm`,
    `pgqueuer.sm`, `pgqueuer.applications`, `pgqueuer.factories`,
    `pgqueuer.types`.
12. **`serialized_dispatch` removed** from `@pgq.entrypoint()`,
    `QueueManager.entrypoint()`, `PgQueuer.entrypoint()` and
    `EntrypointExecutorParameters`. Use `concurrency_limit=1`, enforced by the
    database.
13. **`concurrency_limit` is global.** The dequeue query enforces it across
    all workers instead of per-process semaphores: `concurrency_limit=5` means
    5 jobs fleet-wide, not 5 per worker.
14. **`RetryManager` removed** (`pgqueuer.core.retries`, an internal buffer
    helper). `TimedOverflowBuffer` re-queues on flush failure itself.
15. **Buffer callbacks replaced by port injection.** `TimedOverflowBuffer` no
    longer takes `callback`; subclasses override `flush_items()` and inject a
    repository port. Only affects subclasses of `TimedOverflowBuffer`,
    `JobStatusLogBuffer` or `HeartbeatBuffer`.
16. **`retry_timer` replaced by a global `heartbeat_timeout`.** Removed from
    `@pgq.entrypoint()`, `QueueManager.entrypoint()`, `PgQueuer.entrypoint()`
    and `EntrypointExecutorParameters`. Pass `heartbeat_timeout` to
    `QueueManager.run()` / `PgQueuer.run()` (default 30 seconds); with
    different per-entrypoint timers, use the maximum. Heartbeats go out at half
    the timeout. Stale-job retry is always on; `retry_timer=0` used to disable
    it.
17. **`RetryWithBackoffEntrypointExecutor` removed**, with
    `MaxRetriesExceeded`, `MaxTimeExceeded` and the `async-timeout` dependency.
    Use `DatabaseRetryEntrypointExecutor`, which retries in the database and
    survives worker restarts.
18. **Tracing adapter re-exports removed.** Import `TracingConfig`, `TRACER`,
    `set_tracing_class()` and `TracingProtocol` from `pgqueuer.ports.tracing`,
    not `pgqueuer.adapters.tracing`.
19. **`log_statistics(tail=...)` renamed to `limit=...`.** Positional calls are
    unaffected.
20. **CLI connection options and `dsn()` removed.** Gone: `--pg-host`,
    `--pg-port`, `--pg-user`, `--pg-database`, `--pg-password`, `--pg-schema`,
    the matching `AppConfig` fields, and `dsn()` from
    `pgqueuer.adapters.drivers` and `pgqueuer.db`. asyncpg and psycopg read the
    libpq variables (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`,
    `PGDATABASE`) themselves; set a schema with
    `PGOPTIONS="-csearch_path=myschema"`, and in code call `asyncpg.connect()`
    or `psycopg.connect("")` without a DSN. Kept: `--pg-dsn` / `PGDSN` and
    `--prefix` /
    `PGQUEUER_PREFIX`.
21. **`QueueManager` and `SchedulerManager` take `queries`, not a
    connection.** Use `QueueManager(Queries(driver))`,
    `SchedulerManager(Queries(driver))` and
    `CompletionWatcher(driver, queries=Queries(driver))`. Replace
    `qm.connection` / `sm.connection` with `qm.queries.driver` /
    `sm.queries.driver`. `PgQueuer(driver)` is unchanged.
22. **`Driver.tm` returns `TaskManagerPort`** (`pgqueuer.ports.driver`), not
    `TaskManager`. Only matters if a custom driver annotated `tm` explicitly;
    change the annotation or drop it.
23. **Async `Driver`s must implement `notify(channel, payload)`.** `NOTIFY`
    moved from the queries layer to the drivers, which use their native,
    parameterized APIs; `pgqueuer.adapters.persistence.qb.build_notify_query()`
    is removed. Built-in async drivers are updated; `SyncPsycopgDriver` is
    unaffected. A psycopg implementation:

    ```python
    async def notify(self, channel: str, payload: str) -> None:
        async with self.connection.cursor() as cur:
            await cur.execute("SELECT pg_notify(%s, %s)", (channel, payload))
    ```

### New features

- **`RetryRequested`** (`from pgqueuer import RetryRequested`). Raise it from a
  handler to re-queue the job instead of failing it, e.g.
  `raise RetryRequested(delay=timedelta(seconds=30), reason="rate limited")`.
  The job keeps its row and id, `attempts` goes up by one, and it is eligible
  again after the optional delay. `job.attempts` (default `0`) lets handlers
  implement their own backoff or give-up logic.
- **`DatabaseRetryEntrypointExecutor`** (exported from `pgqueuer`).
  Automatic exponential backoff for any handler, attached with
  `executor_factory=lambda p: DatabaseRetryEntrypointExecutor(parameters=p, ...)`
  on the entrypoint. An unhandled exception becomes a `RetryRequested` with a
  computed delay (`RetryRequested` itself passes through), and after
  `max_attempts` consecutive failures the original exception is terminal.
  Defaults: `max_attempts=5`, `initial_delay=timedelta(seconds=1)`,
  `max_delay=timedelta(minutes=5)`, `backoff_multiplier=2.0`.
- **`on_failure="hold"`** on an entrypoint keeps terminally failed jobs in the
  queue table as `status='failed'` instead of deleting them; dequeue skips
  them. With the retry executor, a job is held only after its retries run out.
  Invalid values raise `ValueError` at decoration. Inspect and re-queue with
  `pgq failed` (25 by default, `-n 100` for more), `pgq requeue 42 43 44`, or
  `queries.list_failed_jobs(limit=25)` and `queries.requeue_jobs(ids)`.
- **Factory arguments.** `pgq run myapp:factory -- --region us-east-1` passes
  everything after `--` to the factory as `list[str]`. Factories without
  arguments still work.
- **`ScheduleContext`** (`pgqueuer.models`). A scheduled task registered with
  `accepts_context=True` is called as `(schedule, ctx: ScheduleContext)` and
  gets `ctx.resources`, like `Context.resources` for job handlers. Previously
  the only way was a closure over `pgq.resources`. Handlers without
  `accepts_context` still take just the schedule.
- **Read-only MCP server.** `pip install pgqueuer[mcp]`, then
  `python -m pgqueuer.adapters.mcp`. Eleven tools: `queue_size`,
  `queue_table_info`, `queue_stats`, `throughput_summary`, `failed_jobs`,
  `queue_log`, `schedules`, `stale_jobs`, `active_workers`, `queue_age`,
  `schema_info`. Connects with the libpq variables or
  `create_mcp_server(dsn="postgresql://...")`. Works with Claude Desktop,
  Claude Code, Cursor and other MCP clients; see the
  [MCP Server docs](docs/integrations/mcp-server.md).

### Bug fixes

- Deferred (`execute_after`) jobs wake up within ~100ms of becoming eligible
  instead of waiting up to `dequeue_timeout` (default 30s); the manager
  shortens its wait to the next deferred job's ETA.
- A job becoming eligible between the dequeue attempt and the ETA query no
  longer causes a full-timeout sleep. With queued work but no future deferred
  jobs, the manager polls every 100ms.
- The in-memory adapter releases the `dedupe_key` of a job held as `failed`,
  and validates the `'failed'` enum value at startup.
- `Queries.peak_schedule()` and `ScheduleRepositoryPort.peak_schedule()` are
  renamed to `peek_schedule()`, fixing the misspelling.
- **`id` columns widened to `BIGINT`
  ([#671](https://github.com/janbjorge/pgqueuer/issues/671)).** The `int4
  SERIAL` keys on the queue, statistics and schedules tables cap at
  2,147,483,647, after which every `enqueue` fails. Fresh installs use
  `BIGSERIAL`; `pgq upgrade` widens existing columns and their legacy
  `AS INTEGER` sequences in place.
  - **This takes an `ACCESS EXCLUSIVE` lock.** Rewriting the table and
    rebuilding its indexes blocks **everything** on it, plain `SELECT`
    included; on a large or bloated table that is seconds to minutes. While the
    migration waits for the lock, new queries queue behind it, so one
    long-running transaction can freeze the queue for the whole wait. Run
    `pgq upgrade` in a maintenance window, or use
    `pgq upgrade --no-widen-id` and widen out-of-band. It is idempotent and a
    no-op once everything is `BIGINT`.

### Other changes

- `async-timeout` is no longer a runtime dependency (still a dev/test
  dependency until the tests move to `asyncio.timeout`).
- `pgqueuer/core/helpers.py` deleted; its functions moved to their modules
  (`listeners.py`, `executors.py`, `query_helpers.py`, etc.).
- Dead internals removed: `ExponentialBackoff`, `timer()`,
  `retry_timer_buffer_timeout()`, and `EntrypointStatistics` in
  `pgqueuer.domain.models`.
- Added `has_function()` and `has_trigger()` to `SchemaManagementPort`, and
  `OnFailure` to the `pgqueuer.types` re-exports.
- `utc_now()` lives in one place, `pgqueuer.domain.models`.
- Scheduler heartbeat updates are batched.
- `asyncio.Future` state transitions are guarded against races, and the
  `listener_healthy` timeout raises `FailingListenerError` directly.
- `PgQueuer.__post_init__` is the only place concrete adapters are built;
  `QueueManager` and `SchedulerManager` no longer create `Queries`.
- Import-linter checks the domain, ports, core and metrics layers separately.
  Within core, only `core.applications` (the composition root) keeps adapter
  import exceptions.
- Docs: OpenTelemetry section in the tracing guide; `examples/callable_factory/`
  removed (see `examples/consumer.py`).
- Repo: PR titles linted for Conventional Commits, agent guidance consolidated
  in `AGENTS.md`, test suite trimmed from 695 to 599 tests, SVG logo, ASCII
  diagrams instead of Mermaid, docs CI runner label fixed.
