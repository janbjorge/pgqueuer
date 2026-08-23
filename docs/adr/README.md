# Architecture Decision Records

Index and backlog for PgQueuer's ADRs. Each record captures exactly one
decision: a fork where a real alternative existed and reversing the choice
would cost something. Mechanisms, tool picks, and tunable values belong in
reference documentation or in a record's consequences section; they do not
get records of their own.

Conventions:

- One decision per record, filed as `ADR-NNNN-<slug>.md` in this directory.
- Format: MADR. Every record includes an **Alternatives considered** section
  (if none can be named, it is not a decision and the record should be
  deleted) and a **Not covered by this ADR** section listing the mechanisms
  it deliberately leaves free to change.
- Existing behavior is recorded with status `Accepted (retroactive)`.
- Records cite only merged ADRs. A decision still in the backlog is named
  in prose; add the number once its record merges.

## Index

- [ADR-0001: Job state lives in PostgreSQL](ADR-0001-job-state-lives-in-postgresql.md)
- [ADR-0002: Workers claim jobs by row-level lock contention, not assignment](ADR-0002-workers-claim-by-lock-contention.md)
- [ADR-0003: Notifications signal that something changed, never carry job data](ADR-0003-notifications-signal-not-carry.md)
- [ADR-0004: Delivery is at-least-once](ADR-0004-delivery-is-at-least-once.md)
- [ADR-0005: Worker liveness is an application-level signal, not DB session state](ADR-0005-worker-liveness-is-application-signal.md)

## Backlog

Each entry carries enough context to draft the record without re-deriving
it from the codebase: the single decision, the fork it resolved, the
visible consequences, code pointers, and what the record deliberately
leaves open.

### Core model

- [x] **ADR-0001: Job state lives in PostgreSQL**
  - Decision: the queue's source of truth is a PostgreSQL database the user
    already operates; PgQueuer introduces no broker process.
  - Fork: dedicated broker (Redis/RabbitMQ/SQS) vs. the relational DB
    already in the stack.
  - Consequences: transactional enqueue with business data; ops burden is
    "your existing Postgres"; throughput ceiling is Postgres itself.
    Everything downstream (0002 through 0011) inherits this.
  - Not covered: table layout, SQL, driver choice.

- [x] **ADR-0002: Workers claim jobs by row-level lock contention, not assignment**
  - Decision: any worker may claim any eligible job by winning a row lock.
    There is no coordinator and no assignment of partitions or shards to
    particular workers.
  - Fork: contention (`FOR UPDATE SKIP LOCKED` family) vs. a scheduler that
    assigns work to named workers.
  - Consequences: workers are homogeneous and stateless; adding a worker
    needs no registration; no fairness guarantee between workers.
  - Pointers: claim query built in `pgqueuer/adapters/persistence/qb.py`.
  - Not covered: the claim query's shape (single-statement CTE is detail).

- [x] **ADR-0003: Notifications signal that something changed, never carry job data**
  - Decision: LISTEN/NOTIFY wakes workers up; the payload identifies the
    event kind only. Workers always re-query for actual work.
  - Fork: push job payloads through NOTIFY vs. wake-and-poll.
  - Consequences: lost or duplicated notifications are harmless because the
    poll safety net covers gaps; NOTIFY's 8k payload limit is irrelevant;
    extra round-trip per wake-up.
  - Pointers: event routing in `pgqueuer/core/listeners.py`; completion
    watching in `pgqueuer/core/completion.py` builds on this.
  - Not covered: channel naming, debounce/poll intervals, event JSON shape.

- [x] **ADR-0004: Delivery is at-least-once**
  - Decision: a job may run more than once; exactly-once is not attempted.
    User contract: entrypoints must be idempotent.
  - Fork: at-least-once vs. exactly-once machinery (fencing tokens,
    transactional outbox on the consumer side).
  - Consequences: a crash between claim and completion re-delivers; docs
    must state the idempotency requirement loudly.
  - Not covered: how a crashed worker is detected (ADR-0005), what happens
    to failed jobs (the `on_failure` disposition is a consequence, not a
    decision).

- [x] **ADR-0005: Worker liveness is an application-level signal, not DB session state**
  - Decision: workers periodically prove liveness in data (heartbeat
    timestamps); job ownership does not die with the DB connection.
  - Fork: application heartbeat vs. session-scoped locks or connection
    state that release on disconnect.
  - Consequences: survives connection churn and poolers; a job is "stale"
    when its heartbeat is older than the threshold; recovery latency is
    bounded by that threshold rather than being instant.
  - Pointers: heartbeat timeout plumbed through
    `pgqueuer/adapters/cli/supervisor.py` (`runit(..., heartbeat_timeout)`);
    stale re-pick lives in the claim query (`qb.py`).
  - Not covered: intervals, thresholds, batching of heartbeat updates.

- [ ] **ADR-0006: Concurrency limits are enforced globally at claim time, not per process**
  - Decision: per-entrypoint and per-worker concurrency caps are evaluated
    against cluster-wide state when a job is claimed.
  - Fork: DB-side gate visible to all workers vs. in-process semaphores
    (each process honest only about itself).
  - Consequences: limits hold across the whole fleet regardless of worker
    count; costs extra bookkeeping in the claim path.
  - Not covered: how the gate is computed (CTE mechanics), specific columns.

### Job semantics

- [ ] **ADR-0007: Retries re-queue the job with persisted attempt state**
  - Decision: canonical retry means the job returns to the queue with its
    attempt count stored in the database, visible to operators.
  - Fork: durable DB-level retry vs. in-process retry loops invisible to
    the queue (the in-process helper exists but is a convenience of the
    executor layer, not the retry model).
  - Consequences: retries survive worker death; backoff is schedulable;
    `attempts` is queryable for alerting.
  - Pointers: `DatabaseRetryEntrypointExecutor` in
    `pgqueuer/core/executors.py`; `Job.attempts` in domain models.
  - Not covered: backoff constants, max-attempt defaults, executor classes.

- [ ] **ADR-0008: Job outcomes are recorded as append-only events, aggregated later**
  - Decision: completions and failures append to an event log; statistics
    are derived by periodic aggregation instead of being maintained as
    inline counters.
  - Fork: event log plus rollup vs. incrementing shared counter rows on
    every completion, which contends across the fleet.
  - Consequences: the hot path stays insert-only; stats are eventually
    consistent; log table growth needs the aggregation to run.
  - Pointers: aggregation query in `pgqueuer/adapters/persistence/qb.py`
    (`build_aggregate_log_data_to_statistics_query`).
  - Not covered: advisory-lock serialization of the aggregation, schedules.

- [ ] **ADR-0009: Payload is opaque bytes; the library ships no serializer**
  - Decision: job payloads are `bytes` end to end; encoding and decoding
    are the application's job.
  - Fork: own the serialization format (pickle/JSON/msgpack) vs. stay out.
  - Consequences: PgQueuer owns no deserialization CVEs; polyglot producers
    are possible; users write their own (de)serialization glue.
  - Not covered: the `headers` side-channel used by tracing integrations
    (consequence of ADR-0018).

- [ ] **ADR-0010: Durability is an operator-selected level, per install**
  - Decision: users choose a named durability level at install time,
    trading crash-safety for throughput.
  - Fork: fixed full durability for everyone vs. an explicit knob.
  - Consequences: benchmark-friendly and ephemeral workloads are served;
    "volatile" data loss on crash is opt-in and documented.
  - Pointers: `Durability` levels in `pgqueuer/domain/settings.py`
    (volatile / balanced / durable map to per-table LOGGED/UNLOGGED
    policies).
  - Not covered: which tables map to which policy per level (detail that
    may shift as tables are added).

- [ ] **ADR-0011: Deduplication is enforced by the database, scoped to in-flight jobs**
  - Decision: duplicate suppression is a DB constraint, and "duplicate"
    means an in-flight (queued or running) job with the same key. The key
    becomes reusable once the job reaches a terminal state.
  - Fork: DB constraint vs. application-side check (racy between
    producers); permanent key uniqueness vs. in-flight window.
  - Consequences: dedupe survives producer races; recurring jobs can reuse
    keys; "already ran yesterday" dedupe is out of scope.
  - Pointers: partial unique index built in
    `pgqueuer/adapters/persistence/qb.py`; `OnConflict` in domain types.
  - Not covered: the index definition itself.

- [ ] **ADR-0012: Entrypoints must be async**
  - Decision: registering a non-`async def` entrypoint is rejected at
    registration time; there is one calling convention.
  - Fork: support both sync and async (thread-pool wrapping) vs. async
    only. This was a breaking change, so the record should note the
    migration story (wrap sync work explicitly).
  - Consequences: no hidden thread pools; blocking work is the user's
    explicit choice; sync-only codebases need a wrapper.
  - Pointers: enforcement in `pgqueuer/core/executors.py`
    (`is_async_callable` check raising `TypeError`).
  - Not covered: which executor runs the coroutine, task-group mechanics.

### Architecture

- [ ] **ADR-0013: Domain and business logic depend on interfaces, never on infrastructure**
  - Decision: ports-and-adapters layering; core logic sees protocols, not
    concrete drivers or SQL.
  - Fork: layered architecture vs. direct coupling to one client library.
  - Consequences: adapters are testable and replaceable; a composition root
    is the single sanctioned exception; indirection cost on every boundary.
  - Pointers: `pgqueuer/ports/driver.py` (`Driver` protocol); layering
    contracts in `pyproject.toml` (enforcement tooling is detail).
  - Not covered: which linter enforces it, directory names, module layout.

- [ ] **ADR-0014: Multiple Postgres client libraries are supported as first-class citizens**
  - Decision: users bring their preferred client library (async or sync);
    PgQueuer does not bless a single driver.
  - Fork: one blessed driver (simpler, richer API) vs. a driver port with
    several adapters.
  - Consequences: the driver port is a lowest common denominator of fetch,
    execute, notify, and listen (`pgqueuer/ports/driver.py`);
    driver-specific features such as pipelining and prepared statements are
    unusable; each adapter needs its own test coverage.
  - Cross-ref: the mechanism (the port) comes from ADR-0013; this record is
    about the user-facing commitment.
  - Not covered: the adapter list (asyncpg/psycopg/in-memory are detail).

- [ ] **ADR-0015: The library owns its own schema lifecycle**
  - Decision: PgQueuer ships install/upgrade/verify of its DB objects;
    users are not handed raw DDL to manage in their own migration tool.
  - Fork: built-in schema management vs. "here's the SQL, use Alembic".
  - Consequences: upgrades are a documented library operation; an escape
    hatch (print the SQL for DBA review) is still required; the library
    must never touch non-PgQueuer objects.
  - Not covered: CLI command names, migration style (ADR-0016).

- [ ] **ADR-0016: Schema migrations are idempotent and forward-only**
  - Decision: the upgrade path is a replayable stream of idempotent steps;
    there are no down-migrations.
  - Fork: idempotent replay vs. versioned up/down migrations with a
    recorded schema version.
  - Consequences (this is where "no version table" lives): no version
    bookkeeping, re-running upgrade is always safe, no downgrade path, the
    step list is append-only forever, and each step must guard its own
    applicability.
  - Pointers: `build_upgrade_queries()` in
    `pgqueuer/adapters/persistence/qb.py`; opt-out knob for the one
    table-rewriting step (`widen_id` in `pgqueuer/domain/settings.py`).
  - Not covered: individual step contents, DO-block guard patterns.

- [ ] **ADR-0017: Multiple independent installs may share one database**
  - Decision: all DB object names are namespaced (prefix and/or schema) so
    several PgQueuer instances coexist in one database.
  - Fork: namespacing vs. one-install-per-database.
  - Consequences: every object reference must go through the naming layer;
    NOTIFY channels are global to the database and need their own
    namespacing.
  - Pointers: `DBSettings` prefix/schema machinery in
    `pgqueuer/domain/settings.py`.
  - Not covered: env-var spellings, validation rules, default names.

- [ ] **ADR-0018: Optional integrations ship in-repo behind extras**
  - Decision: tracing, dashboard, metrics, and agent-facing integrations
    live in the main repo, activated by install extras.
  - Fork: in-repo extras vs. separate companion packages vs. no
    integrations at all.
  - Consequences: one repo, CI, and release train; the core install keeps
    its dependencies small; integration modules must degrade gracefully on
    import when the extra is absent (a consequence, not a decision).
  - Not covered: the integration list, header namespacing for trace
    context, degradation mechanics.

### Runtime & process

- [ ] **ADR-0019: The scaling unit is the process; the runner supervises one manager**
  - Decision: horizontal scale means running more processes; PgQueuer ships
    no forking or prefork worker pool.
  - Fork: built-in multi-process pool (celery-style) vs. delegating process
    multiplication to the platform (systemd, k8s, docker compose).
  - Consequences: simple lifecycle (one event loop per process); the
    platform owns restarts and placement; single-machine users must start N
    processes themselves.
  - Pointers: supervisor loop in `pgqueuer/adapters/cli/supervisor.py`.
  - Not covered: signal handling, restart-on-failure, cycle mechanics.

- [ ] **ADR-0022: Recurring schedules are stored in the database and run by the worker runtime**
  - Decision: cron-style schedules are rows in a PgQueuer table, dispatched
    by the same runtime that runs jobs, so no external scheduler is
    required.
  - Fork: built-in DB-backed scheduler vs. "use cron/celery-beat and just
    enqueue".
  - Consequences: schedules share the DB's durability and namespacing
    story; multiple runners must coordinate over schedule ownership;
    scheduling precision is bounded by the poll cadence.
  - Pointers: `SchedulerManager` in `pgqueuer/core/sm.py` (due-schedule
    poll loop, schedule heartbeats).
  - Not covered: poll cadence, heartbeat mechanics, cron parsing.

### Project policy

- [ ] **ADR-0020: Strict SemVer; the public surface is stable within a major**
  - Decision: breaking changes to the public API happen only on major
    versions.
  - Fork: strict SemVer vs. pragmatic breakage in minors.
  - Consequences: compat shims and deprecation cycles exist at all (the
    mechanisms are detail); what counts as "public surface" must be defined
    in the record.
  - Not covered: shim modules, deprecation sentinel pattern.

- [ ] **ADR-0021: Automated tests run against a real PostgreSQL instance**
  - Decision: queue semantics are verified against real Postgres, not
    mocks or fakes of the DB.
  - Fork: real DB in CI vs. mocked driver responses.
  - Consequences: locking, notify, and constraint behavior is exercised
    for real; contributors need Docker; the suite is slower. The in-memory
    adapter exists for users' tests, not as PgQueuer's own verification
    target.
  - Not covered: testcontainers, per-test database templating, xdist.

- [ ] **ADR-0023: Version support policy** (pending decision, not retroactive)
  - Decision to make: propose "support all non-EOL upstream versions" of
    Python and PostgreSQL. Matches today's CI matrix and gives a rule for
    when floors move instead of ad-hoc bumps.
  - Fork: EOL-tracking policy vs. fixed floors vs. chase-latest.
  - Consequences: floor bumps follow upstream EOL dates instead of being
    debated case by case; concrete versions live in README/CI, not in the
    record.
  - Note: this is the one record that makes a decision rather than
    documenting an existing one; it needs maintainer sign-off on the policy
    itself.

## Explicitly not ADRs

Implementation detail; documented in AGENTS.md or reference docs and free to
change without touching a record:

- The single-statement claim query shape, index definitions, query-plan tests
- Advisory locking in statistics aggregation; batching, jitter, debounce
- Tool picks: import-linter, testcontainers, pydantic-settings, htmx
- The in-memory adapter (consequence of 0014 + 0021)
- Graceful degradation when an optional package is missing (consequence of 0018)
- Deprecation sentinel pattern and shim modules (consequence of 0020)
- Header namespacing for trace context (consequence of 0018)
- Signal handling and restart-on-failure (consequence of 0019)
- Completion watching / awaiting job results (consequence of 0003)
- `on_failure` hold/delete disposition (consequence of 0004 + 0008)
- Scheduler heartbeat mechanics (implementation under 0022)
- Concrete timeouts, batch sizes, and other defaults
