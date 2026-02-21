# Deferred Execution

The `execute_after` attribute enables deferred job execution, allowing you to control when
a job becomes eligible for processing.

## How It Works

`execute_after` specifies the **earliest** time a job can be picked for execution. If not
provided, the job is eligible immediately (`NOW()`).

This is useful for:

- Delaying execution to allow external dependencies to settle
- Implementing retry-after-cooldown patterns
- Scheduling one-off future tasks without a full cron setup

## Example

Enqueue a job that should execute one minute from now:

```python
from datetime import timedelta
from pgqueuer.queries import Queries

await Queries(driver).enqueue(
    "my_task",
    payload=None,
    priority=0,
    execute_after=timedelta(minutes=1),
)
```

Jobs remain in the queue with `queued` status until their `execute_after` timestamp is
surpassed. The `QueueManager` uses `FOR UPDATE SKIP LOCKED` with a timestamp filter, so
deferred jobs are never picked early.

!!! note
    `execute_after` only accepts a `timedelta` offset from the current time (or `None` for
    immediate execution). Absolute `datetime` values are **not** supported.

## Combining with Priority

Deferred jobs participate in the normal priority queue once their `execute_after` time passes:

```python
# High priority but deferred — will jump the queue once eligible
await queries.enqueue("urgent_task", b"data", priority=10, execute_after=timedelta(hours=1))
```
