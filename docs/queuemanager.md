# QueueManager

The `QueueManager` in PgQueuer facilitate the management and processing of jobs within the job queue.

## Setting Up the QueueManager

1. **Create a Connection Pool**: First, you need to establish a connection pool with PostgreSQL using `asyncpg`. This pool will be used by the `QueueManager` to interact with the database.

2. **Instantiate the QueueManager**: Pass the connection pool to the `QueueManager` upon instantiation.

3. **Define Entrypoints**: Entrypoints are functions that the `QueueManager` will call to process jobs. Each entrypoint corresponds to a job type.

### Example Code

Here's a simple example to demonstrate the setup and basic usage of `QueueManager`:

```python
import asyncio
import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager

async def main() -> None:
    # Create a connection pool
    pool = await asyncpg.create_pool(min_size=2)

    # Instantiate the QueueManager
    qm = QueueManager(pool)

    # Define an entrypoint for handling 'fetch' job types
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job.payload.decode()}")

    # Enqueue some jobs
    await qm.queries.enqueue(["fetch"] * 10, [f"message {i}".encode() for i in range(10)])

    # Start the QueueManager to process jobs
    await qm.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuring QueueManager

The QueueManager provides a few parameters that can be configured to optimize job processing:

- dequeue_timeout: Specifies the maximum time to wait for a job to be available in the queue before timing out. This helps in managing idle periods effectively. The default value is 30 seconds.
- retry_timer: Defines the time interval after which jobs that have been in 'picked' status for longer than this duration will be retried. This ensures that jobs are reprocessed if they have not been successfully handled within a specified timeframe.