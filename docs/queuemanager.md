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
        qm.alive = False

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

### Why Add a Signal Handler?

Incorporating a signal handler that sets a flag (such as `alive` to `False`) is essential for ensuring a graceful shutdown of the `QueueManager`. When the application receives a termination signal (e.g., SIGINT or SIGTERM), the signal handler can set the `alive` flag to `False`, informing the `QueueManager` to stop processing new jobs and complete the jobs currently in the buffer. This helps maintain the integrity and consistency of the job queue by ensuring that no jobs are left unprocessed or partially processed.

In this setup, the `handle_signal` signal handler sets `alive` to `False` upon receiving a termination signal. The main loop checks the `alive` flag, and if set to `False`, it stops running the `QueueManager`, ensuring a clean and graceful shutdown.
