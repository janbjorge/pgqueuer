# Custom Executors

Executors define how jobs are processed once dequeued. The default executor calls your
handler and lets exceptions propagate. A custom executor sits in the same place but can
wrap that call: log around it, dispatch on the payload, retry it, or refuse to run it.

Because the executor is a class, that logic lives outside your handlers, which keeps
handler bodies about the work itself.

## Creating a custom executor

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

## Registering a custom executor

Pass the executor class via `executor_factory`:

```python
@pgq.entrypoint("user_notification", executor_factory=NotificationExecutor)
async def notification_task(job: Job) -> None:
    pass
```

## Database retry executor

`DatabaseRetryEntrypointExecutor` converts unhandled exceptions into database-level retries
via `RetryRequested`. The job is re-queued in the database so any worker can pick it up after
the delay; retries survive worker restarts.

### When to use it

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

If the handler raises `RetryRequested` directly, it passes through unchanged; the executor
only converts non-retry exceptions. See [Database-Level Retry](retry.md) for the full guide.

!!! tip "Combine with `on_failure=\"hold\"`"
    After `max_attempts` is exhausted, the exception propagates as a terminal failure. Add
    `on_failure="hold"` to park the job instead of deleting it, giving you a chance to inspect
    and manually re-queue. See [Holding Failed Jobs](hold-failed-jobs.md).
