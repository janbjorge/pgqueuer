# Custom Executors

Executors define how jobs are processed once dequeued. PgQueuer provides a default executor,
but you can create custom executors to introduce specialized behavior such as advanced logging,
conditional execution, or retry logic.

## What Are Executors?

Custom executors let you:

- **Implement custom logic**: Interact with external APIs, add specialized error handling, or
  build complex workflows.
- **Modularize job processing**: Keep execution logic separate from application code.
- **Enhance flexibility**: Define concurrency limits, dynamic resource allocation, or multi-step
  dispatch patterns.

## Creating a Custom Executor

Subclass `AbstractEntrypointExecutor` and implement the `execute` method:

```python
from pgqueuer.executors import AbstractEntrypointExecutor
from pgqueuer.models import Job, Context

class NotificationExecutor(AbstractEntrypointExecutor):
    async def execute(self, job: Job, context: Context) -> None:
        type_, message = job.payload.decode().split("|")
        if type_ == "email":
            await self.send_email(message)
        elif type_ == "sms":
            await self.send_sms(message)

    async def send_email(self, message: str) -> None:
        print(f"Sending Email: {message}")

    async def send_sms(self, message: str) -> None:
        print(f"Sending SMS: {message}")
```

## Registering a Custom Executor

Pass the executor class via `executor_factory`:

```python
@pgq.entrypoint("user_notification", executor_factory=NotificationExecutor)
async def notification_task(job: Job) -> None:
    pass
```

## Database Retry Executor

`DatabaseRetryEntrypointExecutor` converts unhandled exceptions into database-level retries
via `RetryRequested`. The job is re-queued in the database so any worker can pick it up after
the delay — retries survive worker restarts.

### When to Use It

- Failures that may take minutes to resolve (e.g., downstream service outages)
- Jobs that must survive worker restarts between retry attempts
- Scenarios where you want the retry to be visible in the queue and log tables

### Example

```python
from datetime import timedelta
from pgqueuer import PgQueuer, Job
from pgqueuer.executors import DatabaseRetryEntrypointExecutor

pgq = PgQueuer(driver)

@pgq.entrypoint(
    "sync_inventory",
    executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
        parameters=params,
        max_attempts=5,
        initial_delay=timedelta(seconds=2),
        max_delay=timedelta(minutes=10),
        backoff_multiplier=3.0,
    ),
)
async def sync_inventory(job: Job) -> None:
    await inventory_api.sync(job.payload)
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_attempts` | `5` | Maximum retries before the exception becomes terminal |
| `initial_delay` | `1s` | Delay before the first retry |
| `max_delay` | `5m` | Cap on exponential backoff |
| `backoff_multiplier` | `2.0` | Multiplier applied to delay after each attempt |

If the handler raises `RetryRequested` directly, it passes through unchanged — the executor
only converts non-retry exceptions. See [Database-Level Retry](retry.md) for the full guide.

!!! tip "Combine with `on_failure=\"hold\"`"
    After `max_attempts` is exhausted, the exception propagates as a terminal failure. Add
    `on_failure="hold"` to park the job instead of deleting it, giving you a chance to inspect
    and manually re-queue. See [Holding Failed Jobs](hold-failed-jobs.md).
