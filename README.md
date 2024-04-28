##  🚀 PgQueuer - Building Smoother Workflows One Queue at a Time 🚀
[![CI](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml?query=branch%3Amain)
[![pypi](https://img.shields.io/pypi/v/PgQueuer.svg)](https://pypi.python.org/pypi/PgQueuer)
[![downloads](https://static.pepy.tech/badge/PgQueuer/month)](https://pepy.tech/project/PgQueuer)
[![versions](https://img.shields.io/pypi/pyversions/PgQueuer.svg)](https://github.com/janbjorge/PgQueuer)

---

📚 **Documentation**: [Explore the Docs 📖](https://github.com/janbjorge/PgQueuer/wiki/)

🔍 **Source Code**: [View on GitHub 💾](https://github.com/janbjorge/PgQueuer/)

---
## PgQueuer

PgQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PgQueuer uses PostgreSQL's LISTEN/NOTIFY to manage job queues effortlessly.

### Features

- **Simple Integration**: Easy to integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.

### Installation

To install PgQueuer, simply install with pip the following command:

```bash
pip install PgQueuer
```

### Database Configuration

PgQueuer provides a command-line interface for easy management of installation and uninstallation. Ensure you have configured your [environment variables](https://magicstack.github.io/asyncpg/current/api/index.html#connection) or use the appropriate flags to specify your database credentials.

- **Installing PgQueuer Database Components**:
  ```bash
  python -m PgQueuer install 
  ```

- **Uninstalling PgQueuer Database Components**:
  ```bash
  python -m  uninstall 
  ```

The CLI supports several flags to customize the connection settings. Use `--help` to see all available options.

### Show-Case: Processing Incoming Data Messages

In this scenario, the system is designed to manage a queue of incoming data messages that need to be processed. Each message is encapsulated as a job, and these jobs are then processed by a designated function linked to an entrypoint. This is typical in systems where large volumes of data are collected continuously, such as telemetry data from IoT devices, logs from web servers, or transaction data in financial systems.

#### Function Description:
The `fetch` function, tied to the `fetch` entrypoint, is responsible for processing each message. It simulates a processing task by printing the content of each message. This function could be adapted to perform more complex operations such as parsing, analyzing, storing, or forwarding the data as required by the application.

```python
import asyncio

import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    # Set up a connection pool with a minimum of 2 connections.
    pool = await asyncpg.create_pool(min_size=2)
    # Initialize the QueueManager with the connection pool and query handler.
    qm = QueueManager(pool)

    # Number of messages to simulate.
    N = 10_000

    # Enqueue messages.
    for n in range(N):
        await qm.queries.enqueue("fetch", f"this is from me: {n}".encode())

    # Define a processing function for each message.
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        # Print the message to simulate processing.
        print(f"Processed message: {job}")

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Benchmark Summary

The PgQueuer underwent basic benchmark testing to assess its performance across varying job volumes and concurrency levels.

#### Key Observations:
- **Scalability**: Performance increases with higher concurrency, demonstrating the library's ability to efficiently manage larger workloads.
- **Consistency**: PgQueuer maintains consistent throughput across different job counts, ensuring reliable performance.
- **Optimal Performance**: The highest throughput observed was 5,224 jobs per second at a concurrency level of 5, highlighting the library's robust handling capabilities.
