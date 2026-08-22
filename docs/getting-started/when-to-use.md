# When to use PgQueuer

PgQueuer is a good fit when your application already uses PostgreSQL and needs
background work without another broker service. It is an async-first job queue,
not a general workflow engine.

## Good fits

### Your application already depends on PostgreSQL

Jobs, schedules, and execution logs live in PostgreSQL. You do not need Redis or
RabbitMQ alongside it. Workers use `LISTEN/NOTIFY` for prompt wakeups and
`FOR UPDATE SKIP LOCKED` to claim jobs safely.

This design is useful when the queue should follow the same backup, access, and
monitoring practices as the rest of your application database.

### A database change and its job must commit together

PgQueuer accepts connections created by your application. A producer can enqueue
a job inside the transaction that updates application data, so both changes commit
or roll back together.

### Your handlers are asynchronous

Entrypoint handlers use `async def`, which suits jobs that spend most of their
time on database, HTTP, or other network I/O. Synchronous applications can still
enqueue jobs through `SyncPsycopgDriver`, but workers and handlers remain
asynchronous. See [Drivers](../reference/drivers.md).

### You need queue controls without a separate scheduler

PgQueuer includes:

- priority and delayed execution
- cron schedules with minute or second precision
- database-wide concurrency limits per entrypoint
- deduplication for queued and running jobs
- cancellation and completion notifications
- durable retries and manual re-queueing
- heartbeat-based recovery after a worker crash

The [Core Concepts](core-concepts.md) and [Reliability Model](../guides/reliability.md)
describe how these controls interact.

### Operators need to inspect the queue

PgQueuer exposes queue state through PostgreSQL tables, CLI commands, Prometheus
metrics, distributed tracing, and an optional web dashboard. The in-memory adapter
provides the same queue API for tests that do not need PostgreSQL behavior.

## Cases that need a different design

### Handlers must be synchronous

PgQueuer does not run plain `def` handlers. Blocking libraries can be called from
an async handler with `asyncio.to_thread()`, but applications built mainly around
synchronous task functions may prefer a queue with native sync workers.

### You need chains, groups, chords, or DAG orchestration

PgQueuer schedules and processes independent jobs. It does not provide workflow
primitives for expressing dependencies between jobs. Application code can enqueue
follow-up work, and `CompletionWatcher` can wait for several jobs, but these are not
a durable workflow graph.

### Workers are written in several languages

The queue tables are visible to any PostgreSQL client, but PgQueuer's worker,
registration, retry, cancellation, and tracing APIs are Python APIs. A system that
needs workers implemented in several languages should use a language-neutral
protocol or broker.

### The primary database should not carry queue traffic

Queue writes, claims, heartbeats, and logs consume PostgreSQL resources. This is
usually the intended tradeoff, but it may not suit systems that isolate background
traffic from the application database. Test the expected workload and read
[Performance Tuning](../guides/performance-tuning.md) before committing to the
architecture.

### Tasks need hard process isolation

PgQueuer runs async handlers in the worker process. It does not provide a built-in
process pool for CPU-heavy or untrusted code. Run such work in a separate service
or use a task system designed around process isolation.

## Requirements and boundaries

| Concern | PgQueuer behavior |
|---------|--------------------|
| Python | 3.10 or newer |
| PostgreSQL | 12 or newer |
| Worker model | Asyncio; entrypoints must use `async def` |
| Sync applications | Can enqueue through psycopg |
| Delivery | At least once after worker-crash recovery; handlers should be idempotent |
| Payload | Application-owned `bytes` payload |
| Recurring work | Built-in 5-field or 6-field cron schedules |
| Workflow graphs | Not provided |
| Other languages | No worker SDK or language-neutral task protocol |

## Next steps

- [Install PgQueuer](installation.md)
- [Build a producer and consumer](quickstart.md)
- [Review the reliability model](../guides/reliability.md)
- [Plan a production deployment](../guides/deployment.md)
