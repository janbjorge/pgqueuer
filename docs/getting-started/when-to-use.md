# When to use PgQueuer

PgQueuer is an async Python job queue that keeps jobs in the PostgreSQL database
you already run. This page covers where that design stops being a good idea.

## A good fit

The case for it is short: if the application already runs PostgreSQL, the queue
can share the database's backups, monitoring, and access rules, and a producer can
enqueue a job in the same transaction that writes the row the job is about.
[Why PostgreSQL?](../index.md#why-postgresql) makes that argument in full, and
[PgQueuer vs Celery](../comparisons/celery-comparison.md) puts it next to the
usual alternative.

Two requirements are worth checking before you go further. Handlers run as
`async def` in a Python worker process, and a job can run more than once, because
a worker that dies mid-job leaves its job to be retried.

## Use a different design when

No configuration flag removes the limits below. They follow from the design.

| Requirement | Where PgQueuer stops |
|-------------|----------------------|
| Native synchronous handlers | Handlers must be `async def`. Sync code can enqueue through psycopg, and blocking calls can go through `asyncio.to_thread()`, but the worker is asyncio |
| Chains, groups, chords, or DAGs | PgQueuer processes independent jobs; it does not persist dependencies between them. Application code can enqueue follow-up work, but nothing resumes a half-finished graph after a crash |
| Reading a job's return value | Nothing stores what a handler returns. `CompletionWatcher` reports the final status, so results have to go somewhere the caller can read them |
| Passing typed arguments | Payloads are `bytes`. Encoding and decoding are application code, with no argument signature to validate against |
| Workers in several languages | The queue tables are readable by any PostgreSQL client, but worker registration, retries, cancellation, and tracing are Python APIs |
| Queue traffic isolated from the primary database | Claims, heartbeats, and logs all consume PostgreSQL resources on the same instance as your application queries |
| Hard isolation for CPU-heavy or untrusted tasks | Handlers run in the worker process. There is no built-in process pool, so a runaway job affects everything sharing that worker |

Database load is measurable, so measure it: run your expected workload against a
test instance before you commit to the architecture.
[Performance Tuning](../guides/performance-tuning.md) covers the knobs.

## Next steps

- [Build a producer and consumer](quickstart.md)
- [Plan a production deployment](../guides/deployment.md)
