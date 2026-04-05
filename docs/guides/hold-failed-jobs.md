# Holding Failed Jobs

By default, when a job raises an unhandled exception it is **deleted** from the queue table and
logged with status `exception`. The payload is gone. This is fine for many workloads, but some
scenarios require keeping the failed job around so a human (or external system) can inspect it
and decide whether to retry.

Setting `on_failure="hold"` on an entrypoint changes this behavior: instead of deleting, the
job is updated to `status='failed'` and stays in the queue table with its payload, headers, and
all metadata intact. The dequeue query skips `failed` jobs, so they won't be picked up by
workers until explicitly re-queued.

## How It Works

1. A job handler raises an unhandled exception.
2. PgQueuer checks the entrypoint's `on_failure` setting.
3. If `"hold"`: the job is updated to `status='failed'` in-place and a log entry is written.
4. If `"delete"` (default): the job is deleted and logged with `exception` (existing behavior).
5. Failed jobs sit in the queue table until someone re-queues or deletes them.

The hold operation flows through the same batched buffer as normal job completions — there is
no performance penalty in the common (non-failure) path.

## Use Cases

### One-shot jobs with manual review

You want each job to run exactly once. If it fails, a human inspects the error and decides
whether to retry:

```python
@pgq.entrypoint("process_order", on_failure="hold")
async def process_order(job: Job) -> None:
    await payment_gateway.charge(job.payload)
```

If the payment gateway returns an error, the job is parked. An operator can inspect the payload
via `pgq failed`, fix the upstream issue, and then `pgq requeue <id>`.

### Auto-retry with a safety net

Combine `DatabaseRetryEntrypointExecutor` with `on_failure="hold"` to get automatic retries
**and** a safety net. The executor retries N times with exponential backoff; if all attempts
fail, the job is held instead of lost:

```python
from datetime import timedelta
from pgqueuer.executors import DatabaseRetryEntrypointExecutor

@pgq.entrypoint(
    "sync_inventory",
    on_failure="hold",
    executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
        parameters=params,
        max_attempts=5,
        initial_delay=timedelta(seconds=2),
        max_delay=timedelta(minutes=10),
    ),
)
async def sync_inventory(job: Job) -> None:
    await inventory_api.sync(job.payload)
```

After 5 failed attempts the job lands in `status='failed'` with the full retry history in the
log table.

### External API with human-in-the-loop

A webhook delivery service where transient failures retry automatically, but persistent
failures (4xx responses, invalid payloads) need a human to fix the payload and re-send:

```python
@pgq.entrypoint("deliver_webhook", on_failure="hold")
async def deliver_webhook(job: Job) -> None:
    response = await http_client.post(url, data=job.payload)
    if response.status == 429:
        raise RetryRequested(delay=timedelta(seconds=60), reason="rate limited")
    response.raise_for_status()
```

Rate limits trigger an automatic database-level retry. Other HTTP errors (400, 500, etc.) raise
an exception and the job is held for inspection.

## Inspecting Failed Jobs

### CLI

```bash
pgq failed              # list up to 25 held jobs
pgq failed -n 100       # list up to 100
```

Output includes job ID, entrypoint, attempt count, creation time, and payload size.

### SQL

```sql
SELECT id, entrypoint, attempts, created,
       octet_length(payload) AS payload_bytes
FROM pgqueuer
WHERE status = 'failed'
ORDER BY created DESC
LIMIT 50;
```

For the traceback of a specific failed job:

```sql
SELECT traceback->>'exception_type' AS type,
       traceback->>'exception_message' AS message,
       traceback->>'traceback' AS tb
FROM pgqueuer_log
WHERE job_id = 42 AND status = 'failed'
ORDER BY created DESC
LIMIT 1;
```

## Re-queuing Failed Jobs

### CLI

```bash
pgq requeue 42 43 44    # re-queue specific jobs by ID
```

### Programmatic

```python
await queries.requeue_jobs([JobId(42), JobId(43)])
```

### What happens on re-queue

- Status changes from `failed` to `queued`.
- `execute_after` is set to `NOW()` — the job is immediately eligible.
- `attempts` is reset to `0` — a fresh start.
- A log entry with `status='queued'` is written for auditability.
- Any worker can pick up the re-queued job.
- If `DatabaseRetryEntrypointExecutor` is configured, the job gets a full new set of retry
  attempts.

## Behavior Summary

| Scenario | `on_failure="delete"` (default) | `on_failure="hold"` |
|----------|--------------------------------|---------------------|
| Handler raises exception | Job deleted, logged as `exception` | Job kept with `status='failed'` |
| Handler raises `RetryRequested` | Re-queued with delay (both modes) | Re-queued with delay (both modes) |
| Handler completes | Job deleted, logged as `successful` | Job deleted, logged as `successful` |
| `DatabaseRetryEntrypointExecutor` exhausted | Job deleted | Job held with `status='failed'` |
