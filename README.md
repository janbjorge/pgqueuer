# 🚀 PGQueuer - Building Smoother Workflows One Queue at a Time 🚀

[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain) [![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer) [![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer) [![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

---

- 📚 **Documentation**: [Explore the Docs](https://pgqueuer.readthedocs.io/en/latest/)
- 🔍 **Source Code**: [View on GitHub](https://github.com/janbjorge/pgqueuer/)
- 💬 **Join the Discussion**: [Discord Community](https://discord.gg/C7YMBzcRMQ)

---

PGQueuer is a minimalist, high-performance job queue library for Python, leveraging PostgreSQL's robustness. Designed with simplicity and efficiency in mind, PGQueuer offers real-time, high-throughput processing for background jobs using PostgreSQL's LISTEN/NOTIFY and `FOR UPDATE SKIP LOCKED` mechanisms.

## Features

- **💡 Simple Integration**: PGQueuer seamlessly integrates with any Python application using PostgreSQL, providing a clean and lightweight interface.
- **⚛️ Efficient Concurrency Handling**: Supports `FOR UPDATE SKIP LOCKED` to ensure reliable concurrency control and smooth job processing without contention.
- **🚧 Real-time Notifications**: Uses PostgreSQL's `LISTEN` and `NOTIFY` commands to trigger real-time job status updates.
- **👨‍🎓 Batch Processing**: Supports large job batches, optimizing enqueueing and dequeuing with minimal overhead.
- **⏳ Graceful Shutdowns**: Built-in signal handling ensures safe job processing shutdown without data loss.
- **⌛ Recurring Job Scheduling**: Register and manage recurring tasks using cron-like expressions for periodic execution.

## Installation

Install PGQueuer via pip:

```bash
pip install pgqueuer
```

## Quick Start

Below is a minimal example of how to use PGQueuer to process data.

### Step 1: Consumer - Run the Worker

Start a consumer to process incoming jobs as soon as they are enqueued. PGQueuer ensures graceful shutdowns using pre-configured signal handlers.

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver, dsn
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager

async def main() -> QueueManager:
    connection = await asyncpg.connect(dsn())
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    return qm
```
Run the consumer:
```bash
pgq run examples.consumer.main
```

### Step 2: Producer - Add Jobs to Queue

Now, produce jobs that will be processed by the consumer. Below is a simple script to enqueue 10,000 jobs.

```python
import asyncio
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main(N: int) -> None:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    queries = Queries(driver)
    await queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(1, N + 1)],
        [0] * N,
    )

if __name__ == "__main__":
    asyncio.run(main(10000))
```
Run the producer:
```bash
python3 examples/producer.py 10000
```

### Step 3: Scheduler - Recurring Jobs

PGQueuer also supports recurring job scheduling, allowing you to register tasks that run periodically based on cron-like expressions.

Here is a minimal example of how to use the scheduling feature to run tasks periodically:

```python
import asyncio
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.scheduler import Scheduler
from pgqueuer.models import Schedule

async def create_scheduler() -> Scheduler:
    connection = await asyncpg.connect("postgresql://user:password@localhost:5432/yourdatabase")
    driver = AsyncpgDriver(connection)
    scheduler = Scheduler(driver)

    # Define and register recurring tasks using cron expressions
    # The cron expression "* * * * *" means the task will run every minute
    @scheduler.schedule("update_product_catalog", "* * * * *")
    async def update_product_catalog(schedule: Schedule) -> None:
        print(f"Running update_product_catalog task: {schedule}")
        await asyncio.sleep(0.1)
        print("update_product_catalog task completed.")

    # The cron expression "0 0 * * *" means the task will run every day at midnight
    @scheduler.schedule("clean_expired_tokens", "0 0 * * *")
    async def clean_expired_tokens(schedule: Schedule) -> None:
        print(f"Running clean_expired_tokens task: {schedule}")
        await asyncio.sleep(0.2)
        print("clean_expired_tokens task completed.")

    return scheduler

async def main():
    # Create and run the scheduler
    scheduler = await create_scheduler()
    await scheduler.run()

if __name__ == "__main__":
    asyncio.run(main())
```
Run the scheduler:
```bash
pgq run myapp.create_scheduler
```

This example showcases how you can use the new scheduling feature to automate recurring tasks such as data synchronization or cleanup jobs.

## Dashboard

Monitor job processing statistics in real-time using the built-in dashboard:

```bash
pgq dashboard --interval 10 --tail 25 --table-format grid
```
This provides a real-time, refreshing view of job queues and their status.

Example output:

```bash
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
...
+---------------------------+-------+------------+--------------------------+------------+----------+
```

## Why Choose PGQueuer?

- **Built for Scale**: Handles thousands of jobs per second, making it ideal for high-throughput applications.
- **PostgreSQL Native**: Utilizes advanced PostgreSQL features for robust job handling.
- **Flexible Concurrency**: Offers rate and concurrency limiting to cater to different use-cases, from bursty workloads to critical resource-bound tasks.

## License

PGQueuer is MIT licensed. See [LICENSE](LICENSE) for more information.

---
Ready to supercharge your workflows? Install PGQueuer today and take your job management to the next level!

