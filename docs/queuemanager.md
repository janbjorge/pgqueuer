# QueueManager

The `QueueManager` in PgQueuer facilitate the management and processing of jobs within the job queue.

## Setting Up the QueueManager

1. **Create a Connection**: First, you need to establish a connection with PostgreSQL using asyncpg. This connection will be used by the QueueManager to interact with the database.

2. **Instantiate the QueueManager**: Wrap the connection in a `Driver` and pass to `QueueManager` upon instantiation.

3. **Define Entrypoints**: Entrypoints are functions that the `QueueManager` will call to process jobs. Each entrypoint corresponds to a job type.

### Example Code

Here's a simple example to demonstrate the setup and basic usage of `QueueManager`:

```python
import asyncio

import asyncpg
from PgQueuer.db import AsyncpgDriver
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    N = 1_000
    # Enqueue jobs.
    await qm.queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(N)],
        [0] * N,
    )

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
```

## Configuring QueueManager

The QueueManager provides a few parameters that can be configured to optimize job processing:

- dequeue_timeout: Specifies the maximum time to wait for a job to be available in the queue before timing out. This helps in managing idle periods effectively. The default value is 30 seconds.
- retry_timer: Defines the time interval after which jobs that have been in 'picked' status for longer than this duration will be retried. This ensures that jobs are reprocessed if they have not been successfully handled within a specified timeframe.

## Job Buffer

To improve the efficiency of job handling, the `QueueManager` uses a job buffer. The job buffer is a temporary storage area where jobs are kept before they are processed. This buffering mechanism helps in minimizing database access and ensures that the processing system can handle bursts of jobs more effectively.

### Why Add a Signal Handler?

Adding a signal handler that sets a flag (such as `alive` to `False`) is crucial for ensuring graceful shutdown of the `QueueManager`. When the application receives a termination signal (e.g., SIGINT or SIGTERM), the signal handler can set the `alive` flag to `False`, which informs the `QueueManager` to stop processing new jobs and focus on completing the jobs currently in the buffer. This helps in ensuring that no jobs are left unprocessed or partially processed, maintaining the integrity and consistency of the job queue.

### Implementing the Signal Handler

Here's how you can implement a signal handler in the `QueueManager` setup:

```python
import asyncio
import signal

import asyncpg
from PgQueuer.db import AsyncpgDriver
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    def handle_signal(signum: object, frame: object) -> None:
        qm.alive = False

    signal.signal(signal.SIGINT, handle_signal)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    N = 1_000
    # Enqueue jobs.
    await qm.queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(N)],
        [0] * N,
    )

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
```

In this setup, the signal handler `handle_signal` sets `alive` to `False` when a termination signal is received. The main loop checks the `alive` flag, and if it is `False`, it stops running the `QueueManager`. This approach ensures a clean and graceful shutdown of the job processing system.