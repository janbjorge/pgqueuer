# PgQueuer: Unified Job and Schedule Orchestrator

## Shared Resources (`Context.resources`)

PgQueuer lets you provide a single shared resources container that is injected into every job execution context. This makes it easy to initialize heavyweight or shared components (database pools, HTTP clients, caches, ML models, etc.) once at process startup and reuse them across all jobs.

### Why Use Shared Resources?

- Avoid re-initializing expensive objects per job (e.g. HTTP session pools, model weights)
- Centralize lifecycle (create at startup, optionally close at shutdown)
- Enable coordinated state (e.g. in‑memory counters, feature flags)
- Provide a structured place for integrations (tracing, metrics, external APIs)

### Providing Resources

You pass a mutable mapping when constructing `PgQueuer` (or `QueueManager` directly):

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def build_pgqueuer() -> PgQueuer:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)

    resources = {
        "http_client": build_http_client(),   # e.g. httpx.AsyncClient()
        "vector_index": load_vector_index(),  # custom object
        "feature_flags": {"beta_mode": True},
    }

    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("process_user")
    async def process_user(job: Job) -> None:
        ctx = pgq.qm.get_context(job.id)
        http = ctx.resources["http_client"]
        flags = ctx.resources["feature_flags"]
        # Use shared objects without recreating them
        ...

    return pgq
```

Internally this mapping is passed into each `Context` as `context.resources`. All jobs receive the SAME object (it is not copied), so mutations are visible across jobs.

### Access Inside Custom Executors

If you implement a custom executor (`AbstractEntrypointExecutor`), the `execute(self, job, context)` method receives the `Context`:

```python
from pgqueuer.executors import AbstractEntrypointExecutor
from pgqueuer.models import Job, Context

class LoggingExecutor(AbstractEntrypointExecutor):
    async def execute(self, job: Job, context: Context) -> None:
        logger = context.resources.get("logger")
        if logger:
            logger.info("Processing job %s", job.id)
        # Call wrapped function (if delegating) or implement logic directly
```

### Mutating Resources

Because `resources` is a shared mutable mapping:

```python
context.resources.setdefault("metrics", {}).setdefault("processed", 0)
context.resources["metrics"]["processed"] += 1
```

If you need stricter control (immutability, lifecycle hooks), you can later replace the mapping with a custom registry class; the public contract is simply "object with mapping semantics."

### Scheduled (Cron) Tasks

Currently, scheduled tasks do NOT automatically receive `resources` as a second argument. You can still access them via closure:

```python
pgq = PgQueuer(driver, resources={"http": http_client})

@pgq.schedule("refresh_cache", "*/5 * * * *")
async def refresh_cache(schedule):
    http = pgq.resources["http"]
    await http.get("https://api.example.com/ping")
```

A future enhancement (see project TODO) may allow optional arity-based injection or a `ScheduleContext`.

### Enabling Context Injection

Entry points only receive the runtime `Context` when registered with `accepts_context=True`. This opt-in flag applies whether the callable is synchronous, asynchronous, or a callable object.

```python
@pgq.entrypoint("process_with_context", accepts_context=True)
async def process_with_context(job: Job, ctx: Context) -> None:
    logger = ctx.resources.get("logger")
    ...
```

Entry points registered without the flag are invoked with the job only:

```python
@pgq.entrypoint("process_without_context")
async def process_without_context(job: Job) -> None:
    ...
```

### Testing With Resources

In tests you can assert propagation:

```python
qm = QueueManager(driver, resources={"flag": "test"})

@qm.entrypoint("demo")
async def demo(job: Job) -> None:
    assert qm.get_context(job.id).resources["flag"] == "test"
```

### Summary

| Aspect        | Behavior |
|---------------|----------|
| Initialization | Passed at construction: `PgQueuer(..., resources=...)` |
| Scope          | Shared across all jobs in the same process |
| Mutation       | Visible to subsequent jobs |
| Scheduled jobs | Use closure access (for now) |
| Custom executors | Receive via `context.resources` |

### Using Async Resources in Sync Entrypoints

Sync entrypoints (declared with `def`, not `async def`) are executed in a worker thread via `anyio.to_thread.run_sync`. Async / event‑loop bound resources (e.g. asyncpg pools, `httpx.AsyncClient`, async factories) must NOT be called directly inside these sync functions. Doing so just returns an un‑awaited coroutine object (or may raise loop/thread errors), and the I/O never actually runs.

Recommended approaches:
1. Prefer making I/O entrypoints async:
   ```python
   @pgq.entrypoint("process_user")
   async def process_user(job: Job) -> None:
       pool = pgq.qm.get_context(job.id).resources["pg_pool"]
        await pool.execute("SELECT 1")
   ```
2. If you must stay sync (e.g. CPU work) but still need async calls, bridge them back to the loop:
   ```python
   @pgq.entrypoint("resize_then_store")
   def resize_then_store(job: Job) -> None:
       ctx = pgq.qm.get_context(job.id)
       img = cpu_resize(job.payload)
       # Bridge async store using anyio.from_thread.run
       anyio.from_thread.run(store_image, img, ctx.resources["pg_pool"])

   async def store_image(data: bytes, pool: asyncpg.Pool) -> None:
        await pool.execute("INSERT INTO images(data) VALUES($1)", data)
   ```
3. Wrap async clients in a small synchronous facade that internally uses `anyio.from_thread.run` for each call.

Misuse example (DO NOT DO):
```python
@pgq.entrypoint("bad")
def bad(job: Job) -> None:
    ctx = pgq.qm.get_context(job.id)
    async_func = ctx.resources["async_func"]          # async def ...
    coro = async_func("value")                        # Returns coroutine, not result
    # Never awaited -> lost work / warnings
```

Correct bridging:
```python
@pgq.entrypoint("good")
def good(job: Job) -> None:
    ctx = pgq.qm.get_context(job.id)
    async_func = ctx.resources["async_func"]
    result = anyio.from_thread.run(async_func, "value")  # Properly awaited
```

Testing reference:
See `test_sync_async_resources.py` for:
- Misuse detection (coroutine leakage)
- Proper bridging with `anyio.from_thread.run`
- Shared mutation behavior when bridging

Guidance:
- Mark entrypoints async whenever they perform network / DB / other async I/O.
- Limit sync entrypoints to CPU or pure transformations plus optional bridged calls.
- Consider adding your own wrappers or guards if your team wants to forbid direct async resource access in sync entrypoints.


PgQueuer is a comprehensive library designed to manage job queues and recurring tasks efficiently using PostgreSQL. By integrating `QueueManager` and `SchedulerManager`, it offers a unified solution for handling both queued jobs and periodic tasks seamlessly.

---

## **Getting Started**

### Setting Up PgQueuer

To start using PgQueuer, establish a connection to PostgreSQL with `asyncpg` or `psycopg`. You can then create a PgQueuer instance using one of the convenient classmethods. Below is a complete example:

#### Using asyncpg connection

```python
from datetime import datetime
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    # Establish database connection
    connection = await asyncpg.connect()

    # Initialize PgQueuer instance using the classmethod
    pgq = PgQueuer.from_asyncpg_connection(connection)

    # Define a job entrypoint
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    # Define a scheduled task
    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

    return pgq
```

#### Using asyncpg pool

```python
from datetime import datetime
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    # Create a connection pool
    pool = await asyncpg.create_pool("postgresql://user:password@localhost/dbname")

    # Initialize PgQueuer instance from pool
    pgq = PgQueuer.from_asyncpg_pool(pool)

    # Define a job entrypoint
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    # Define a scheduled task
    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

    return pgq
```

#### Using psycopg async connection

```python
from datetime import datetime
import psycopg
from pgqueuer import PgQueuer
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    # Establish database connection with autocommit enabled
    connection = await psycopg.AsyncConnection.connect(
        "postgresql://user:password@localhost/dbname",
        autocommit=True
    )

    # Initialize PgQueuer instance using the classmethod
    pgq = PgQueuer.from_psycopg_connection(connection)

    # Define a job entrypoint
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    # Define a scheduled task
    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute: {schedule!r}, {datetime.now()!r}")

    return pgq
```

These examples demonstrate how to define both a job entrypoint (`fetch`) and a recurring task (`scheduled_every_minute`) with cron-like scheduling. The PgQueuer classmethods simplify the setup by handling driver initialization automatically.

**Note:** For psycopg connections, always ensure `autocommit=True` is set when creating the connection.

### Starting PgQueuer with CLI

PgQueuer includes a CLI tool for easy startup without additional scripting. To launch it:

```bash
pgq run mypackage:create_pgqueuer
```

This command initializes the PgQueuer instance, manages job queues, schedules tasks, and ensures graceful shutdown.

---

## **Key Features**

### Execute After

The `execute_after` attribute enables deferred job execution, allowing you to control when jobs become eligible for processing.

#### How It Works

The `execute_after` attribute specifies the earliest time a job can be picked for execution. If not provided, the job is eligible immediately (`NOW()`). This feature is particularly useful for delaying execution to accommodate external dependencies or resource availability.

#### Example Usage

To enqueue a job that should execute one minute from now:

```python
from datetime import timedelta
await Queries(driver).enqueue(
    "my_task", payload=None, priority=0, execute_after=timedelta(minutes=1)
)
```

Jobs remain in the queue until their `execute_after` timestamp is surpassed.

---

### Job Cancellation

PgQueuer supports canceling queued or in-progress jobs programmatically, leveraging PostgreSQL's NOTIFY system.

#### Overview

PgQueuer provides a mechanism for canceling jobs by their unique IDs. This allows for selective termination of tasks either in the queue or already being processed. Cancellations are "best-effort" and may not halt a job already underway.

#### Example Usage

**Enqueueing Jobs:**

```python
from pgqueuer.queries import Queries
queries = Queries(db_driver)
job_ids = await queries.enqueue("task_entrypoint", b"Job data", priority=5)
```

To enqueue jobs from synchronous code:

```python
from pgqueuer.queries import SyncQueries
queries = Queries(sync_db_driver)
job_ids = queries.enqueue("task_entrypoint", b"Job data", priority=5)
```

**Cancelling Jobs:**

```python
await queries.mark_job_as_cancelled(job_ids)
```

#### Handling Cancellations in Code

**Asynchronous Job Processing:**

```python
@entrypoint("task_entrypoint")
async def process_job(job: Job):
    with qm.get_context(job.id).cancellation:
        await perform_task(job.data)
```

**Synchronous Job Processing:**

```python
@entrypoint("sync_entrypoint")
def process_job(job: Job):
    cancel_scope = qm.get_context(job.id).cancellation
    for step in job_steps:
        if cancel_scope.cancel_called:
            return
        perform_task(step)
```

---

### Configuring PgQueuer

PgQueuer provides configurable parameters to optimize job processing based on specific requirements:

- **`dequeue_timeout`**: Maximum time to wait for jobs (default: 30 seconds).
- **`retry_timer`**: Interval to retry unprocessed jobs.

---

### Custom Job Executors

Executors in PgQueuer handle the execution of dequeued jobs. By default, PgQueuer provides a generic executor, but custom executors can be created to introduce specialized behavior such as integration with external services, advanced logging, or conditional execution logic.

#### What Are Executors?

Executors are responsible for defining how jobs are processed once dequeued. They allow developers to:

- **Implement Custom Logic**: Tailor job execution for unique requirements, such as interacting with APIs, handling retries differently, or adding specialized error handling mechanisms.
- **Modularize Job Processing**: Keep the job processing logic separate from the application, simplifying maintenance and improving testability.
- **Enhance Flexibility**: Define behavior like concurrency limits, dynamic resource allocation, or complex workflows.

#### Example: NotificationExecutor

The following example demonstrates how to create a custom executor that processes jobs for sending notifications via email or SMS.

```python
from pgqueuer.executors import AbstractEntrypointExecutor
from pgqueuer.models import Job

class NotificationExecutor(AbstractEntrypointExecutor):
    async def execute(self, job: Job, context):
        # Parse job data to determine notification type and message
        type, message = job.data.decode().split('|')
        if type == 'email':
            await self.send_email(message)
        elif type == 'sms':
            await self.send_sms(message)

    async def send_email(self, message: str):
        print(f"Sending Email: {message}")

    async def send_sms(self, message: str):
        print(f"Sending SMS: {message}")
```

#### Registering and Using Custom Executors

Custom executors can be registered to specific entrypoints using the `executor_factory` parameter. For example:

```python
@pgq.entrypoint("user_notification", executor_factory=NotificationExecutor)
async def notification_task(job: Job):
    pass
```

When jobs are enqueued for the `user_notification` entrypoint, the `NotificationExecutor` will handle their processing.

---

### Retry with Backoff Executor

The `RetryWithBackoffEntrypointExecutor` is a specialized custom executor designed for handling transient job failures. It extends the base executor interface and introduces retry logic with exponential backoff and jitter.

#### How Does It Work?

This executor automatically retries jobs that fail during processing. It calculates delays between retries using exponential backoff, where the delay increases with each attempt, and adds jitter to avoid contention when multiple jobs are retried simultaneously. The retry process ensures that transient issues (e.g., temporary API unavailability or network glitches) do not cause jobs to fail permanently.

#### Features of the RetryWithBackoffEntrypointExecutor

1. **Retry Mechanism**: Automatically attempts to reprocess failed jobs, reducing manual intervention and increasing fault tolerance.
2. **Exponential Backoff**: Ensures retries are spaced progressively further apart, minimizing strain on external systems while allowing time for transient issues to resolve.
3. **Jitter**: Introduces randomness to retry delays, preventing job collisions and reducing the risk of contention in high-concurrency scenarios.
4. **Customizable Limits**: Configure the maximum number of retry attempts, the cap on exponential backoff delay, and the total allowed retry time.

#### Example Use Case

The `RetryWithBackoffEntrypointExecutor` is ideal for scenarios like:

- Interacting with unreliable external APIs prone to rate limiting or downtime.
- Handling transient network failures, such as timeouts or temporary disconnections.
- Retrying database operations during temporary outages or deadlock scenarios.

By using this executor, you can enhance system resilience and maintain smooth operations without overloading resources.

#### Example Implementation

Here’s an example of how to use the `RetryWithBackoffEntrypointExecutor` in a PGQueuer setup:

```python
import asyncpg
from datetime import timedelta
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.executors import RetryWithBackoffEntrypointExecutor
from pgqueuer.models import Job

async def create_pgqueuer() -> PgQueuer:
    # Connect to the PostgreSQL database
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    # Define an entrypoint with retry and exponential backoff logic
    @pgq.entrypoint(
        "retry_with_backoff",
        executor_factory=lambda parameters: RetryWithBackoffEntrypointExecutor(
            parameters=parameters,
            max_attempts=5,  # Retry the job up to 5 times
            max_delay=timedelta(seconds=0.5),  # Cap exponential backoff at 0.5 seconds
            max_time=timedelta(seconds=1),  # Ensure the entire retry process finishes within 1 second
        ),
    )
    async def retry_with_backoff(job: Job) -> None:
        # Simulate a transient failure scenario
        print(f"Processing job with retry logic: {job!r}")

    return pgq
```

#### Explanation of the Example

1. **Executor Configuration**:
   - `max_attempts`: Limits the retries to 5 attempts.
   - `max_delay`: Caps the exponential backoff delay at 0.5 seconds to prevent excessively long waits.
   - `max_time`: Ensures the entire retry process, including all attempts, completes within 1 second to avoid prolonged processing.

2. **Why Use It?**:
   - The retry logic handles failures gracefully, especially in scenarios where a brief wait or retry can resolve the issue.
   - Exponential backoff with jitter reduces the likelihood of resource contention, ensuring system stability even during high load.

By integrating `RetryWithBackoffEntrypointExecutor`, you can build robust workflows that recover automatically from transient issues, reducing the need for manual intervention.

---

### Scheduler

Manage recurring tasks with cron-like expressions.

#### Example Usage

```python
@pgq.schedule("fetch_db", "* * * * *")
async def fetch_db(schedule: Schedule):
    await perform_task()
```

#### How It Works

- **Registration**: Define tasks using the `@schedule` decorator.
- **Execution**: The scheduler runs tasks at defined intervals and tracks execution state.
- **Database Integration**: Schedules are stored in PostgreSQL, ensuring durability and recovery.

#### New Feature: `clean_old` Flag

The `clean_old` flag is a new addition to the scheduler decorator. When set to `True`, it will remove any old schedules that are not in the current registry. This is useful for cleaning up schedules that are no longer needed.

##### Example Usage

To use the `clean_old` flag, simply set it to `True` in the `@schedule` decorator:

```python
@pgq.schedule("fetch_db", "* * * * *", clean_old=True)
async def fetch_db(schedule: Schedule):
    await perform_task()
```

By default, the `clean_old` flag is set to `False`, so it will not remove any old schedules unless explicitly set to `True`.

---

### Throttling and Concurrency Control

PgQueuer provides fine-grained control over job execution frequency and concurrency.

#### Rate Limiting

Define maximum requests per second for specific job types:

```python
@entrypoint("data_processing", requests_per_second=10)
def process_data(job: Job):
    pass
```

#### Concurrency Limiting

Limit concurrent job processing:

```python
@entrypoint("data_processing", concurrency_limit=4)
async def process_data(job: Job):
    pass
```

#### Serialized Dispatch

Ensure jobs of the same type are processed one at a time:

```python
@entrypoint("shared_resource", serialized_dispatch=True)
async def process_shared_resource(job):
    pass
```

---

### Automatic Heartbeat

The automatic heartbeat mechanism ensures active jobs are monitored:

- **Periodic Updates**: Updates a `heartbeat` timestamp to signal job activity.
- **Stall Detection**: Identifies stalled jobs for retries or alerts.
- **Resource Management**: Prevents unresponsive jobs from locking system resources.

### Wait-for-Completion (close to real-time job tracking)

`CompletionWatcher` lets you **await** the final status of any job, live-streamed via PostgreSQL `LISTEN/NOTIFY`, with zero manual polling.

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `refresh_interval` | `timedelta \| None` | **5 s** | Safety-net: a lightweight query every *n* seconds in case a `NOTIFY` was lost. |
| `debounce` | `timedelta` | **50 ms** | Coalesces bursts of `NOTIFY`s so the expensive status query runs at most once per window. |

#### Basic usage

```python
from pgqueuer.completion import CompletionWatcher

async with CompletionWatcher(driver) as watcher:      # uses defaults
    status = await watcher.wait_for(job_id)           # "successful", "exception", …
````

#### Tracking many jobs at once

```python
from asyncio import gather
from pgqueuer.completion import CompletionWatcher

image_ids   = await qm.queries.enqueue(["render_img"]   * 20, [b"..."] * 20, [0] * 20)
report_ids  = await qm.queries.enqueue(["generate_pdf"] * 10, [b"..."] * 10, [0] * 10)
cleanup_ids = await qm.queries.enqueue(["cleanup"]      *  5, [b"..."] *  5, [0] *  5)

async with CompletionWatcher(driver) as w:
    img_waiters   = [w.wait_for(j) for j in image_ids]
    pdf_waiters   = [w.wait_for(j) for j in report_ids]
    clean_waiters = [w.wait_for(j) for j in cleanup_ids]

    img_statuses, pdf_statuses, clean_statuses = await gather(
        gather(*img_waiters), gather(*pdf_waiters), gather(*clean_waiters)
    )
```

Recognised terminal states: **`canceled`**, **`deleted`**, **`exception`**, **`successful`**.

#### Helper functions

For one-off scripts and test suites you can avoid the context-manager boilerplate by using two tiny wrappers that ship with PgQueuer.
Their full source is shown here so you can copy/paste or consult the doc-strings any time.

```python
import asyncio
from datetime import timedelta

from pgqueuer import db, models
from pgqueuer.completion import CompletionWatcher


async def wait_for_all(
    driver: db.Driver,
    job_ids: list[models.JobId],
    refresh_interval: timedelta = timedelta(seconds=5),
    debounce: timedelta = timedelta(milliseconds=50),
) -> list[models.JOB_STATUS]:
    """
    Block until **every** supplied job finishes and return their statuses in
    the same order the IDs were passed.

    Extra keyword arguments are forwarded to ``CompletionWatcher``.
    """
    async with CompletionWatcher(
        driver,
        refresh_interval=refresh_interval,
        debounce=debounce,
    ) as watcher:
        waiters = [watcher.wait_for(jid) for jid in job_ids]
        return await asyncio.gather(*waiters)


async def wait_for_first(
    driver: db.Driver,
    job_ids: list[models.JobId],
    refresh_interval: timedelta = timedelta(seconds=5),
    debounce: timedelta = timedelta(milliseconds=50),
) -> models.JOB_STATUS:
    """
    Return as soon as **any** job hits a terminal state; pending waiters are
    cancelled and the watcher shuts down cleanly.
    """
    async with CompletionWatcher(
        driver,
        refresh_interval=refresh_interval,
        debounce=debounce,
    ) as watcher:
        waiters = [watcher.wait_for(jid) for jid in job_ids]
        done, pending = await asyncio.wait(
            waiters, return_when=asyncio.FIRST_COMPLETED
        )
        for fut in pending:
            fut.cancel()

    return next(iter(done)).result()
```

### Notification Reliability

To maximize reliability without relying on heavy polling:

1. **Listener Health Check** – The `QueueManager` performs periodic
   health checks on the `LISTEN/NOTIFY` channel. Enable
   `--shutdown-on-listener-failure` so the manager stops if the
   listener becomes unhealthy. An external supervisor can then restart
   it to recover.

2. **Disable the refresh poll** – `CompletionWatcher` issues a safety
   query every few seconds by default. Set `refresh_interval=None` to
   rely solely on notifications when your channel is stable.

   ```python
   async with CompletionWatcher(driver, refresh_interval=None) as w:
       status = await w.wait_for(job_id)
   ```
