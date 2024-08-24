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

## Configuring QueueManager

The QueueManager offers several configurable parameters to optimize job processing:

- **dequeue_timeout**: Specifies the maximum time to wait for a job to be available in the queue before timing out, which helps manage idle periods effectively. The default value is 30 seconds.
- **retry_timer**: Defines the time interval after which jobs in 'picked' status for longer than this duration will be retried, ensuring jobs are reprocessed if not successfully handled within the specified timeframe.

## Job Buffer

To enhance the efficiency of job handling, the `QueueManager` uses a job buffer. This buffer is a temporary storage area for jobs before they are processed, minimizing database access and enabling the system to handle bursts of jobs more effectively.

### Why Add a Signal Handler?

Incorporating a signal handler that sets a flag (such as `alive` to `False`) is essential for ensuring a graceful shutdown of the `QueueManager`. When the application receives a termination signal (e.g., SIGINT or SIGTERM), the signal handler can set the `alive` flag to `False`, informing the `QueueManager` to stop processing new jobs and complete the jobs currently in the buffer. This helps maintain the integrity and consistency of the job queue by ensuring that no jobs are left unprocessed or partially processed.

In this setup, the `handle_signal` signal handler sets `alive` to `False` upon receiving a termination signal. The main loop checks the `alive` flag, and if set to `False`, it stops running the `QueueManager`, ensuring a clean and graceful shutdown.
