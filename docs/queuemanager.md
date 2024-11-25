# QueueManager

The `QueueManager` in pgqueuer facilitates the management and processing of jobs within the job queue.

## Setting Up the QueueManager

1. **Create a Connection**: First, establish a connection with PostgreSQL using asyncpg or psycopg (v3.2+). This connection will be utilized by the `QueueManager` to interact with the database.

2. **Instantiate the QueueManager**: Wrap the connection in a `Driver` and pass it to `QueueManager` upon instantiation.

3. **Define Entrypoints**: Entrypoints are functions that the `QueueManager` will call to process jobs. Each entrypoint corresponds to a job type.

### Example Code

Here's a simple example to demonstrate the setup and basic usage of `QueueManager`:

```python
import asyncio
import signal

import asyncpg

from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


async def main() -> None:
    # Establish a database connection; asyncpg and psycopg are supported.
    connection = await asyncpg.connect()
    # Initialize a database driver
    driver = AsyncpgDriver(connection)
    # Create a QueueManager which orchestrates entrypoints based on registered names.
    qm = QueueManager(driver)

    # Ensures a clean and graceful shutdown of the job processing system.
    def handle_signal(signum: object, frame: object) -> None:
        qm.shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)

    # Register an entrypoint with the Queue Manager.
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    # Enqueue jobs; typically managed by a separate process. Handled sequentially here for simplicity.
    N = 1_000
    await qm.queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(N)],
        [0] * N,
    )

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
```

## Job Cancellation

The job cancellation feature in `pgqueuer` allows for the programmatic termination of jobs that are queued or currently under processing. This feature is designed to be used in scenarios where it is necessary to halt specific operations due to changing conditions, errors, or other operational controls.

### Overview

Job cancellations are managed through PostgreSQL's NOTIFY system. The `QueueManager` listens for cancellation events and attempts to halt the execution of targeted jobs accordingly. It’s crucial to note that this cancellation process is "best effort," meaning that there are scenarios where a job may not be cancelled immediately or may even complete despite a cancellation request due to race conditions.

#### Enqueue Jobs

When enqueueing jobs, a unique identifiers is assigned to each job, which will be used later to reference and potentially cancel the job:

```python
from pgqueuer.queries import Queries

# Setup your QueueManager and database driver
queries = Queries(db_driver)
job_ids = await queries.enqueue("task_entrypoint", b"Job data", priority=5)
```

#### Cancel Jobs

To cancel jobs, use the IDs obtained during the enqueueing:

This function sends a cancellation event that the QueueManager processes. Where a cancellation is attempted immediately, it means that the affected jobs are removed from the queue table and promptly added to the log table with a status of "canceled". However, due to the asynchronous nature of the operations and potential database delays, this process is inherently racy.

```python
await queries.mark_job_as_cancelled(job_ids)
```

### Execute After

#### Overview

The `execute_after` attribute in `pgqueuer` provides a mechanism for scheduling deferred job execution within the queue. This attribute allows jobs to be queued but held back from execution until a specified point in time has passed, adding flexibility for scenarios where control over job timing is needed.

#### Attribute Description

- **`execute_after`**: Specifies the earliest point in time that the job can be picked for execution. This is particularly useful for delaying execution without holding system resources.

The attribute is stored as a timestamp and, if not explicitly specified during job creation, it defaults to the current time (`NOW()`). This means that, unless otherwise specified, jobs are immediately available for execution.

#### Execution Timing

The addition of the `execute_after` attribute directly influences the timing of job processing. The job will be considered for execution only if the current timestamp exceeds the `execute_after` timestamp. In practical terms:

- If a job is enqueued with an `execute_after` time set in the future, it will remain in the queue until that time has passed.

- In the worst-case scenario, the actual time a job is eligible for execution is given by: **`execute_after + dequeue_timeout_interval`**

#### Usage

The `execute_after` attribute can be provided when enqueueing jobs via the `enqueue` method. For instance, to enqueue a job that should not be executed until 1 minute from now:

```python
from datetime import timedelta
await Queries(apgdriver).enqueue("my_task", payload=None, priority=0, execute_after=timedelta(minutes=1))
```

This job will only become eligible for execution once the specified minute has passed.

### Handling Cancellations in Job Logic

Jobs should include logic to handle potential cancellations gracefully. Below are examples of how to integrate cancellation checks into both asynchronous and synchronous job processing logic.

#### Asynchronous Job Logic
In asynchronous job logic, the `cancellation` context manager is used to automatically manage the scope of cancellation. This ensures that any cancellation request interrupts the ongoing task as soon as possible.

```python
@qm.entrypoint("task_entrypoint")
async def process_job(job: Job):
    # The context manager handles entry and exit, applying cancellation checks around the awaited calls
    with qm.get_context(job.id).cancellation:
        # Insert job logic here
        await perform_task(job.data)
```
The context manager in asynchronous code is advantageous because it can suspend the coroutine at `await` points and check for cancellation in a non-blocking manner, ensuring that resources are freed appropriately and cancellation is handled efficiently.

#### Synchronous Job Logic
In synchronous job logic, cancellation is checked manually within the logic flow. A context manager is not used here because synchronous functions do not support the automatic suspension and resumption of execution that asynchronous context managers provide.

```python
@qm.entrypoint("sync_entrypoint")
def process_job(job: Job):
    # Manually access the cancellation scope to check if a cancellation has been requested
    cancel_scope = qm.get_context(job.id).cancellation
    for item in job_steps:
        # Manually check if cancellation has been called at each significant step
        if cancel_scope.cancel_called:
            return  # Exit the function if the job has been canceled
        perform_task(item)
```

### Understanding the Cancellation Object

The cancellation feature in `PGQueuer` utilizes a `cancellation` object, which is central to managing the stoppage of specific tasks. This object is part of the `Context` class, encapsulating the cancellation state and mechanisms used to control job execution.

#### The Cancellation Object

The `cancellation` object within each job's context is an instance of `anyio.CancelScope`. This is a powerful tool from the `anyio` library that allows asynchronous tasks to be cancelled cooperatively. The `CancelScope` is used to initiate a cancellation request and check if a cancellation has been requested, providing a flexible and thread-safe way to manage job interruptions.

For a deeper understanding of how cancellation works and how to implement it in your asynchronous operations, you should refer to the official `anyio` documentation. The `anyio` documentation provides comprehensive details on using `CancelScope` effectively, including examples and best practices for integrating it into your projects.

You can find the `anyio` documentation here: [AnyIO cancellation](https://anyio.readthedocs.io/en/stable/cancellation.html)

## Configuring QueueManager

The QueueManager offers several configurable parameters to optimize job processing:

- **dequeue_timeout**: Specifies the maximum time to wait for a job to be available in the queue before timing out, which helps manage idle periods effectively. The default value is 30 seconds.
- **retry_timer**: Defines the time interval after which jobs in 'picked' status for longer than this duration will be retried, ensuring jobs are reprocessed if not successfully handled within the specified timeframe.

## Job Buffer

To enhance the efficiency of job handling, the `QueueManager` uses a job buffer. This buffer is a temporary storage area for jobs before they are processed, minimizing database access and enabling the system to handle bursts of jobs more effectively.

### Why use a Signal Handler?

Using an asyncio.Event for the shutdown event ensures that the shutdown process integrates smoothly with the asynchronous nature of QueueManager. When the application receives a termination signal (e.g., SIGINT or SIGTERM), the signal handler sets the shutdown event, which allows the main loop to detect this and stop processing new jobs while completing the current ones.

This mechanism helps maintain the integrity and consistency of the job queue, ensuring that all jobs in progress are finalized properly before the system shuts down, thereby preventing data corruption or incomplete tasks.

## Retry with backoff executor

The `RetryWithBackoffEntrypointExecutor` is a specialized executor that provides retry logic with exponential backoff and jitter for handling failures during job execution. This executor is suitable for scenarios where job failures are expected to be transient, and retrying with delays could lead to successful completion.

### Setting Up the RetryWithBackoffEntrypointExecutor

The `RetryWithBackoffEntrypointExecutor` is designed to retry jobs that fail during execution. It utilizes exponential backoff along with random jitter to avoid contention and reduce the risk of repeated failures.

#### How It Works

The executor attempts to execute the job and, in case of failure, retries up to a specified number of times (`max_attempts`). Between each retry, an exponential backoff delay is applied, starting with an `initial_delay` and multiplying by a `backoff_multiplier` for each subsequent attempt. To further reduce the chances of contention, a jitter is added to each retry delay.

The retry logic also includes an overall `max_time` limit, beyond which retries are no longer attempted, regardless of the number of attempts left. This ensures that the job does not exceed a reasonable amount of time, preventing resource exhaustion.

#### Key Features

- **Max Attempts**: Controls how many times the job will be retried before giving up.
- **Exponential Backoff**: The delay between retries grows exponentially, reducing the load on the system during repeated failures.
- **Jitter**: A random component is added to the delay to avoid synchronization issues when multiple jobs are retrying concurrently.

#### Use Cases

- **Transient Failures**: Ideal for handling jobs that interact with external systems that may have occasional outages or temporary issues, such as network services or rate-limited APIs.
- **Avoiding System Overload**: Exponential backoff combined with jitter helps to avoid overwhelming external services when multiple jobs fail simultaneously.
- **Graceful Handling of Flaky Dependencies**: Suitable for retrying jobs when interacting with services that have unreliable performance or are prone to timeouts.

## Custom Job Executors

Executors are responsible for executing jobs that have been dequeued from the job queue. The `QueueManager` provides a default job executor called `DefaultEntrypointExecutor`, but you can also create and register your own custom executors to extend its functionality.

### The `Executor` Interface

The `AbstractEntrypointExecutor` is an abstract base class that defines the interface for any custom job executors. It contains the necessary methods and properties that your custom executor must implement. By subclassing `AbstractEntrypointExecutor`, you can control how jobs are executed, modify concurrency behavior, add custom logging, or even interact with external systems as needed.

The key method that needs to be implemented is:

- `async def execute(self, job: models.Job, context: models.Context) -> None`: This method is called to execute the given job. Your implementation should handle all the logic associated with processing the job, including error handling and logging.

Below is an example of how you can create your own custom job executor.

### Example: Creating a Custom Job Executor

Let's create a custom job executor called `NotificationExecutor` that dispatches notifications via email, SMS, or push notifications based on the job data. This example showcases how you can use a custom executor to integrate different notification channels in a flexible and centralized way.

```python
import random
from datetime import timedelta
from pgqueuer.executors import JobExecutor
from pgqueuer.models import Job

class NotificationExecutor(AbstractEntrypointExecutor):
    async def execute(self, job: Job,  context: Context) -> None:
        # Extract notification type and message from job data
        notification_type, message = job.data.decode().split('|')

        if notification_type == 'email':
            await self.send_email(message)
        elif notification_type == 'sms':
            await self.send_sms(message)
        elif notification_type == 'push':
            await self.send_push_notification(message)

        # Execute the original job function if required
        await self.parameters.func(job)

    async def send_email(self, message: str) -> None:
        print(f"Sending Email: {message}")

    async def send_sms(self, message: str) -> None:
        print(f"Sending SMS: {message}")

    async def send_push_notification(self, message: str) -> None:
        print(f"Sending Push Notification: {message}")
```

### Registering the Custom Executor with `QueueManager`

Here's how you would use the `NotificationExecutor` with `QueueManager` to handle different types of user notifications.

```python
from pgqueuer.qm import QueueManager
from pgqueuer.models import Job
from pgqueuer.db import AsyncpgDriver
import asyncpg
import asyncio

async def main() -> None:
    # Establish a database connection; asyncpg and psycopg are supported.
    connection = await asyncpg.connect()
    # Initialize a database driver
    driver = AsyncpgDriver(connection)
    # Create a QueueManager instance
    qm = QueueManager(driver)

    # Register an entrypoint for notifications with the custom executor
    @qm.entrypoint(
        "user_notification",
        executor_factory=NotificationExecutor,  # Use the custom NotificationExecutor
    )
    async def notification_task(job: Job) -> None:
        print(f"Executing notification job with ID: {job.id}")

    # Enqueue jobs for the notification entrypoint
    notifications = [
        "email|Welcome to our service!",
        "sms|Your verification code is 123456",
        "push|You have a new friend request",
    ]
    
    await qm.queries.enqueue(
        ["user_notification"] * len(notifications),
        [notif.encode() for notif in notifications],
    )

    await qm.run()

if __name__ == "__main__":
    asyncio.run(main())
```

In this example, we create a custom `NotificationExecutor` that can dispatch notifications via email, SMS, or push notifications based on the job data. The `@qm.entrypoint` decorator registers the custom executor to manage these notification jobs, enabling the `QueueManager` to process them efficiently.
