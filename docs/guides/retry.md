# Database-Level Retry

PgQueuer supports **database-level retry** — when a job handler raises `RetryRequested`, the
job stays in the queue table and is re-queued for another attempt. The job row is updated
in-place (not deleted and re-inserted), so the `id`, `payload`, `headers`, and all metadata
are preserved across retries.

## How It Works

1. Your handler raises `RetryRequested` with an optional delay and reason.
2. PgQueuer catches the exception and atomically:
    - Resets the job status to `queued`.
    - Bumps `execute_after` by the requested delay.
    - Increments the `attempts` counter.
    - Writes a log entry to `pgqueuer_log` with full traceability context.
3. The job becomes eligible for dequeue again after the delay expires.
4. On the next execution, `job.attempts` reflects how many previous attempts occurred.

Because the job row is **updated** (not deleted), the same `job.id` is stable across all
retry attempts. This makes it straightforward to trace the full retry graph by querying the
log table.

## Basic Usage

Raise `RetryRequested` from your handler when you detect a transient failure:

```python
from datetime import timedelta
from pgqueuer import PgQueuer, Job
from pgqueuer.errors import RetryRequested

pgq = PgQueuer(driver)

@pgq.entrypoint("call_api")
async def call_api(job: Job) -> None:
    response = await http_client.post(API_URL, data=job.payload)
    if response.status == 429:
        raise RetryRequested(
            delay=timedelta(seconds=30),
            reason="rate limited",
        )
    response.raise_for_status()
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `delay` | `timedelta` | `timedelta(0)` | Time to wait before the next attempt |
| `reason` | `str \| None` | `None` | Human-readable explanation (stored in the log) |

## Reading the Attempt Counter

The `job.attempts` field tells you how many previous attempts have been made. On the first
execution it is `0`, after one retry it is `1`, and so on:

```python
@pgq.entrypoint("resilient_task")
async def resilient_task(job: Job) -> None:
    if job.attempts > 5:
        # Give up after 5 retries — let it fail terminally
        raise RuntimeError("Too many retries, giving up")

    try:
        await do_work(job.payload)
    except TransientError:
        raise RetryRequested(
            delay=timedelta(seconds=2 ** job.attempts),
            reason=f"transient error on attempt {job.attempts}",
        )
```

## Automatic Retry with DatabaseRetryEntrypointExecutor

For cases where you want **any** unhandled exception to trigger a retry (not just explicit
`RetryRequested` raises), use `DatabaseRetryEntrypointExecutor`. It wraps your handler and
converts exceptions into `RetryRequested` with exponential backoff:

```python
from datetime import timedelta
from pgqueuer import PgQueuer, Job
from pgqueuer.executors import DatabaseRetryEntrypointExecutor

pgq = PgQueuer(driver)

@pgq.entrypoint(
    "flaky_api",
    executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
        parameters=params,
        max_attempts=5,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(minutes=5),
        backoff_multiplier=2.0,
    ),
)
async def flaky_api(job: Job) -> None:
    await call_unreliable_service(job.payload)
```

After `max_attempts` consecutive failures, the original exception propagates as a terminal
failure (the job is deleted and logged with status `exception`).

If your handler raises `RetryRequested` directly, it passes through the executor unchanged —
the executor only converts non-retry exceptions.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_attempts` | `int` | `5` | Maximum retries before the exception becomes terminal |
| `initial_delay` | `timedelta` | `1s` | Delay before the first retry |
| `max_delay` | `timedelta` | `5m` | Cap on exponential backoff |
| `backoff_multiplier` | `float` | `2.0` | Multiplier applied to delay after each attempt |

### Backoff Schedule Example

With `initial_delay=1s`, `backoff_multiplier=2.0`, `max_delay=60s`:

| Attempt | Delay |
|---------|-------|
| 0 | 1s |
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5 | 32s |
| 6+ | 60s (capped) |

## Retry vs. Other Retry Mechanisms

PgQueuer has three retry mechanisms, each suited to a different failure mode:

| Mechanism | Scope | When to use |
|-----------|-------|-------------|
| `RetryRequested` | Database-level | Transient failures where you want the job to survive across worker restarts and be visible to any worker |
| `RetryWithBackoffEntrypointExecutor` | In-process | Transient failures where retrying immediately in the same worker is sufficient |
| `retry_timer` | Worker-crash recovery | Stalled jobs whose worker has crashed without updating the heartbeat |

**Key difference:** `RetryRequested` re-queues the job in the database, so any worker can
pick it up. `RetryWithBackoffEntrypointExecutor` retries within the same process, so if the
worker crashes mid-retry the attempts are lost. Use database-level retry when durability
across restarts matters.

## Traceability

Every retry writes a log entry to `pgqueuer_log` with:

- `status = 'queued'` (the job was re-queued)
- `traceback` containing a `TracebackRecord` with the exception details
- `additional_context` with retry metadata:

```json
{
    "entrypoint": "call_api",
    "attempt": 0,
    "retry_delay": "0:00:30",
    "reason": "rate limited"
}
```

Query the retry history for a specific job:

```sql
SELECT job_id, status, created,
       traceback->'additional_context'->>'attempt' AS attempt,
       traceback->'additional_context'->>'reason' AS reason,
       traceback->'additional_context'->>'retry_delay' AS delay
FROM pgqueuer_log
WHERE job_id = 42
ORDER BY created;
```

## Behavior Summary

| Scenario | What happens |
|----------|-------------|
| Handler raises `RetryRequested` | Job updated to `queued`, attempts incremented, log entry written |
| Handler raises any other exception | Job deleted, logged as `exception` (existing behavior) |
| Handler completes normally | Job deleted, logged as `successful` (existing behavior) |
| `DatabaseRetryEntrypointExecutor` + exception + attempts < max | Converted to `RetryRequested` with backoff |
| `DatabaseRetryEntrypointExecutor` + exception + attempts >= max | Exception propagates as terminal failure |
