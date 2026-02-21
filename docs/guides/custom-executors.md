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

## Retry with Backoff Executor

`RetryWithBackoffEntrypointExecutor` handles transient failures with exponential backoff and
jitter.

### Features

1. **Automatic retries** on failure — reduces manual intervention.
2. **Exponential backoff** — progressively longer delays between attempts.
3. **Jitter** — randomised delays to avoid thundering herd on retries.
4. **Configurable limits** — max attempts, backoff cap, and total retry time.

### When to Use It

- Calling rate-limited or occasionally unavailable external APIs
- Handling transient network timeouts or disconnections
- Retrying database operations during brief contention or outages

### Example

```python
import asyncpg
from datetime import timedelta
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.executors import RetryWithBackoffEntrypointExecutor
from pgqueuer.models import Job

async def create_pgqueuer() -> PgQueuer:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    @pgq.entrypoint(
        "retry_with_backoff",
        executor_factory=lambda parameters: RetryWithBackoffEntrypointExecutor(
            parameters=parameters,
            max_attempts=5,
            max_delay=timedelta(seconds=0.5),
            max_time=timedelta(seconds=1),
        ),
    )
    async def retry_with_backoff(job: Job) -> None:
        print(f"Processing job: {job!r}")

    return pgq
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `max_attempts` | Maximum number of retry attempts |
| `max_delay` | Cap on exponential backoff delay between retries |
| `max_time` | Maximum total time allowed for all retry attempts combined |
