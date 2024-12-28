# PgQueuer: Unified Job and Schedule Orchestrator

PgQueuer is a comprehensive library designed to manage job queues and recurring tasks efficiently using PostgreSQL. By integrating `QueueManager` and `SchedulerManager`, it offers a unified solution for handling both queued jobs and periodic tasks seamlessly.

---

## **Getting Started**

### Setting Up PgQueuer

To start using PgQueuer, establish a connection to PostgreSQL with `asyncpg` or `psycopg`. Wrap the connection in a `Driver` instance and initialize PgQueuer. Below is a complete example:

```python
from datetime import datetime
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule

async def main() -> PgQueuer:
    # Establish database connection
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)

    # Initialize PgQueuer instance
    pgq = PgQueuer(driver)

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

This example demonstrates how to define both a job entrypoint (`fetch`) and a recurring task (`scheduled_every_minute`) with cron-like scheduling.

### Starting PgQueuer with CLI

PgQueuer includes a CLI tool for easy startup without additional scripting. To launch it:

```bash
pgq run mypackage.create_pgqueuer
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

This executor automatically retries jobs that fail during processing. It calculates delays between retries using exponential backoff, where the delay increases with each attempt, and adds jitter to avoid contention when multiple jobs are retried simultaneously.

#### Features of the RetryWithBackoffEntrypointExecutor

1. **Retry Mechanism**: Automatically attempts to reprocess failed jobs, reducing manual intervention.
2. **Exponential Backoff**: Ensures retries are spaced progressively further apart, minimizing strain on external systems.
3. **Jitter**: Introduces randomness to retry delays, preventing job collisions in high-concurrency scenarios.

#### Example Use Case

The `RetryWithBackoffEntrypointExecutor` is ideal for scenarios like:

- Interacting with unreliable external APIs.
- Handling transient network failures.
- Retrying database operations during temporary outages.

By using this executor, you can enhance system resilience and maintain smooth operations without overloading resources.

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
