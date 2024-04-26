## PgQueuer

PgQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PgQueuer uses PostgreSQL's native features like transactions, row locking, and LISTEN/NOTIFY to manage job queues effortlessly.

### Features

- **Simple Integration**: Easy to integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.
- **Scalable**: Built to efficiently scale with your application's needs.

### Getting Started

#### Installation

To install PgQueuer, simply run the following command:

```bash
pip install PgQueuer
```

This command installs PgQueuer along with its dependencies, setting up everything you need to start integrating it into your application.

#### Database Configuration

Configure your database connection settings by setting the appropriate environment variables for your PostgreSQL credentials:

```bash
export PGHOST='localhost'
export PGDATABASE='your_database'
export PGUSER='your_username'
export PGPASSWORD='your_password'
```

#### CLI Usage

PgQueuer provides a command-line interface for easy management of installation and uninstallation. Ensure you have configured your environment variables or use the appropriate flags to specify your database credentials.

- **Installing PgQueuer Database Components**:
  ```bash
  python -m PgQueuer install 
  ```

- **Uninstalling PgQueuer Database Components**:
  ```bash
  python -m  uninstall 
  ```

The CLI supports several flags to customize the connection settings. Use `--help` to see all available options.

### Use-Case: Processing Incoming Data Messages

In this scenario, the system is designed to manage a queue of incoming data messages that need to be processed. Each message is encapsulated as a job, and these jobs are then processed by a designated function linked to an entrypoint. This is typical in systems where large volumes of data are collected continuously, such as telemetry data from IoT devices, logs from web servers, or transaction data in financial systems.

#### Function Description:
The `fetch` function, tied to the `fetch` entrypoint, is responsible for processing each message. It simulates a processing task by printing the content of each message. This function could be adapted to perform more complex operations such as parsing, analyzing, storing, or forwarding the data as required by the application.

```python
import asyncio
import time

import asyncpg

from PgQueuer import qm, queries


async def main() -> None:
    # Set up a connection pool with a minimum of 2 connections.
    pool = await asyncpg.create_pool(min_size=2)
    # Initialize the QueueManager with the connection pool and query handler.
    app = qm.QueueManager(
        pool, 
        queries.PgQueuerQueries(pool),
        queries.PgQueuerLogQueries(pool),
    )

    # Number of messages to simulate.
    N = 10_000

    # Enqueue messages.
    for n in range(N):
        await app.q.enqueue("fetch", f"this is from me: {n}".encode())

    # Define a processing function for each message.
    @c.entrypoint("fetch")
    async def process_message(context: bytes) -> None:
        # Simulate parsing the message.
        message = context.decode()
        # Print the message to simulate processing.
        print(f"Processed message: {message}")

    await app.run()


if __name__ == "__main__":
    asyncio.run(main())
```