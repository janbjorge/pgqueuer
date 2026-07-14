# Reliability Model

This page explains how PgQueuer handles failures, ensures jobs are not lost, supports
idempotent enqueuing, and provides an audit trail of every completed job.

## Failure Handling

When a job raises an unhandled exception, PgQueuer:

1. Marks the job status as `exception`.
2. Captures the full traceback, exception type, and message into a **TracebackRecord**.
3. Moves the record from the active queue (`pgqueuer`) to the log table (`pgqueuer_log`).

The job is **not** automatically retried at this point — it is considered definitively failed
for this execution attempt. The traceback is persisted in `pgqueuer_log` for inspection
via a direct query:

```sql
SELECT job_id, entrypoint,
       traceback->>'exception_type' AS exception_type,
       traceback->>'exception_message' AS exception_message,
       traceback->>'traceback' AS traceback_text
FROM pgqueuer_log
WHERE status = 'exception'
ORDER BY created DESC
LIMIT 20;
```

## Retry Strategies

PgQueuer provides three complementary retry mechanisms.

### Database-level retry: durable re-queuing

Raise `RetryRequested` from your handler to re-queue the job in the database. The job row is
updated in-place — the `id`, `payload`, and all metadata are preserved. Any worker can pick
up the retried job.

```python
from datetime import timedelta
from pgqueuer.errors import RetryRequested

@pgq.entrypoint("call_api")
async def call_api(job: Job) -> None:
    response = await http_client.post(API_URL, data=job.payload)
    if response.status == 429:
        raise RetryRequested(delay=timedelta(seconds=30), reason="rate limited")
```

Use `DatabaseRetryEntrypointExecutor` to automatically convert any exception into a
database-level retry with exponential backoff:

```python
from pgqueuer.executors import DatabaseRetryEntrypointExecutor

@pgq.entrypoint(
    "flaky_api",
    executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
        parameters=params,
        max_attempts=5,
        initial_delay=timedelta(seconds=1),
    ),
)
async def flaky_api(job: Job) -> None:
    await call_unreliable_service(job.payload)
```

See [Database-Level Retry](retry.md) for full details, backoff configuration, and
traceability queries.

### Holding terminal failures for manual re-queue

Set `on_failure="hold"` on an entrypoint to keep the job in the queue table with
`status='failed'` instead of deleting it. The payload, headers, and attempt count are preserved
for inspection. Use `pgq failed` to list held jobs and `pgq requeue <id>` to send them back.

See [Holding Failed Jobs](hold-failed-jobs.md) for the full guide with use cases and examples.

### Worker-crash recovery: stalled jobs

If a worker process crashes mid-job, the job remains in `picked` state with a stale heartbeat.
The global `heartbeat_timeout` on `pgq.run()` controls when stale jobs become eligible for
re-pickup by another worker:

```python
from datetime import timedelta

await pgq.run(
    dequeue_timeout=timedelta(seconds=5),
    batch_size=10,
    heartbeat_timeout=timedelta(minutes=5),
)
```

Any worker can then claim and re-run the stalled job. See
[Heartbeat Monitoring](heartbeat.md) for stall detection queries.

!!! warning "Design for re-execution"
    A recovered job **will run again from the start**. Ensure your job functions are
    idempotent, or checkpoint progress externally so a restart is safe.

## Idempotency

Pass a `dedupe_key` to prevent duplicate jobs from entering the queue:

```python
job_ids = await queries.enqueue(
    "send_invoice",
    payload=b'{"order_id": 42}',
    dedupe_key="invoice-order-42",
)
```

PgQueuer enforces a database-level unique constraint:

```sql
UNIQUE (dedupe_key)
WHERE status IN ('queued', 'picked') AND dedupe_key IS NOT NULL
```

If a job with the same `dedupe_key` already exists in `queued` or `picked` state, a
`DuplicateJobError` is raised. The constraint only covers `queued` and `picked`, so once the
job leaves those states — `successful`, `exception`, `canceled`, `deleted`, or `failed`
(held) — the key is released and the same key can be used again.

**Choosing a dedupe key:** Use a stable, business-meaningful identifier — for example,
`f"invoice-{order_id}"` or `f"report-{date}-{user_id}"`. This turns enqueue into an
idempotent operation: calling it twice with the same key and payload is safe.

### Skipping duplicates instead of raising

By default a duplicate fails the whole enqueue call — in a batch, nothing is inserted.
Pass `on_conflict="skip"` to insert the non-duplicate jobs and skip the rest:

```python
job_ids = await queries.enqueue(
    ["send_invoice", "send_invoice", "send_invoice"],
    [b"1", b"2", b"3"],
    [0, 0, 0],
    dedupe_key=["invoice-1", "invoice-2", "invoice-3"],
    on_conflict="skip",
)
# One entry per input: JobId for inserted jobs, None for skipped duplicates,
# e.g. [JobId(11), None, JobId(12)] if "invoice-2" was already active.
```

The result keeps its 1:1 positional mapping with the inputs, so callers always know which
jobs were inserted and which were skipped. Skipped jobs are not created at all: they get no
job id and no `queued` entry in the log table. The same flag is available on the CLI via
`pgq queue --dedupe-key ... --on-conflict skip`.

**Duplicates within a single batch:** if the same `dedupe_key` appears more than once in one
`enqueue` call — even with different payloads — the **first occurrence by input order** is
enqueued and every later occurrence is treated as a conflict. Under `on_conflict="skip"` the
later positions come back as `None`; under the default they fail the call. Order your inputs
so the payload you want kept comes first.

## Poison Jobs

A "poison job" is one that consistently causes worker crashes or hangs without updating its
heartbeat. PgQueuer does not include a built-in dead-letter queue, but you can detect and
handle poison jobs with a query:

```sql
-- Jobs stuck in 'picked' with a long-stale heartbeat (indicative of repeated crashes/hangs)
-- Adjust the interval based on your heartbeat_timeout and expected job runtime
SELECT id, entrypoint, status, heartbeat, updated
FROM pgqueuer
WHERE status = 'picked'
  AND heartbeat < NOW() - INTERVAL '10 minutes'
ORDER BY heartbeat ASC;
```

**Recommended pattern:** route these to a separate monitoring alert or move them to a
dedicated "quarantine" entrypoint by updating their entrypoint column and re-queuing.

## Audit Trail

Every job that leaves the active queue is written to `pgqueuer_log`:

| Event | Status in log |
|-------|--------------|
| Job completes without error | `successful` |
| Job raises an exception (`on_failure="delete"`, the default) | `exception` (with traceback) |
| Job raises an exception (`on_failure="hold"`) | `failed` (with traceback) |
| Job is canceled | `canceled` |
| Job is deleted without running | `deleted` |

The log is **append-only** and serves as a permanent audit record. You can query it directly
or use `pgq dashboard` from the CLI.

!!! note "Log table retention"
    PgQueuer does not automatically prune `pgqueuer_log`. Add a periodic `DELETE` job or
    PostgreSQL table partition policy to manage log growth in high-throughput systems.

## Summary

| Concern | Mechanism |
|---------|-----------|
| Durable retry across workers | `RetryRequested` / `DatabaseRetryEntrypointExecutor` |
| Worker crash recovery | `heartbeat_timeout` |
| Terminal failure parking | `on_failure="hold"` |
| Duplicate enqueue prevention | `dedupe_key` unique constraint |
| Graceful duplicate handling in batches | `enqueue(..., on_conflict="skip")` |
| Failure inspection | `pgqueuer_log` with traceback |
| Audit trail | `pgqueuer_log` for all terminal states |
