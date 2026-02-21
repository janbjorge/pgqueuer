# Reliability Model

This page explains how PgQueuer handles failures, ensures jobs are not lost, supports
idempotent enqueuing, and provides an audit trail of every completed job.

## Failure Handling

When a job raises an unhandled exception, PgQueuer:

1. Marks the job status as `exception`.
2. Captures the full traceback, exception type, and message into a **TracebackRecord**.
3. Moves the record from the active queue (`pgqueuer`) to the log table (`pgqueuer_log`).

The job is **not** automatically retried at this point — it is considered definitively failed
for this execution attempt. The traceback is persisted for inspection via `pgq logs` or a
direct query:

```sql
SELECT job_id, entrypoint, exception_type, exception_message, traceback
FROM pgqueuer_log
WHERE status = 'exception'
ORDER BY created DESC
LIMIT 20;
```

## Retry Strategies

PgQueuer provides two complementary retry mechanisms.

### Per-attempt retry: transient errors

Use `RetryWithBackoffEntrypointExecutor` to automatically retry a job function when it raises
an exception, before the job is marked as failed. This is suitable for transient errors such
as network timeouts or rate-limited APIs.

```python
from datetime import timedelta
from pgqueuer.executors import RetryWithBackoffEntrypointExecutor

@pgq.entrypoint(
    "send_email",
    executor_factory=lambda parameters: RetryWithBackoffEntrypointExecutor(
        parameters=parameters,
        max_attempts=5,
        max_delay=timedelta(seconds=30),
        max_time=timedelta(minutes=2),
    ),
)
async def send_email(job: Job) -> None:
    await smtp_client.send(job.payload)
```

See [Custom Executors](custom-executors.md#retry-with-backoff-executor) for full parameter
details.

### Worker-crash recovery: stalled jobs

If a worker process crashes mid-job, the job remains in `picked` state with a stale heartbeat.
Configure `retry_timer` on `QueueManager` to automatically re-queue jobs whose heartbeat has
not been updated within the specified window:

```python
from datetime import timedelta
pgq = PgQueuer(driver, retry_timer=timedelta(minutes=5))
```

Any worker can then claim and re-run the stalled job. See
[Heartbeat Monitoring](heartbeat.md) for stall detection queries.

!!! warning "Design for re-execution"
    A job recovered via `retry_timer` **will run again from the start**. Ensure your job
    functions are idempotent, or checkpoint progress externally so a restart is safe.

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
`DuplicateJobError` is raised. Once the job reaches a terminal state (`successful`,
`exception`, `canceled`, `deleted`), the key is released and the same key can be used again.

**Choosing a dedupe key:** Use a stable, business-meaningful identifier — for example,
`f"invoice-{order_id}"` or `f"report-{date}-{user_id}"`. This turns enqueue into an
idempotent operation: calling it twice with the same key and payload is safe.

## Poison Jobs

A "poison job" is one that consistently causes worker crashes or hangs without updating its
heartbeat. PgQueuer does not include a built-in dead-letter queue, but you can detect and
handle poison jobs with a query:

```sql
-- Jobs that have been re-picked more than 3 times (indicative of repeated failure)
-- Adjust threshold based on your retry_timer and expected job runtime
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
| Job raises an exception | `exception` (with traceback) |
| Job is canceled | `canceled` |
| Job is deleted without running | `deleted` |

The log is **append-only** and serves as a permanent audit record. You can query it directly
or use `pgq logs` from the CLI.

!!! note "Log table retention"
    PgQueuer does not automatically prune `pgqueuer_log`. Add a periodic `DELETE` job or
    PostgreSQL table partition policy to manage log growth in high-throughput systems.

## Summary

| Concern | Mechanism |
|---------|-----------|
| Transient errors | `RetryWithBackoffEntrypointExecutor` |
| Worker crash recovery | `retry_timer` + heartbeat |
| Duplicate enqueue prevention | `dedupe_key` unique constraint |
| Failure inspection | `pgqueuer_log` with traceback |
| Audit trail | `pgqueuer_log` for all terminal states |
