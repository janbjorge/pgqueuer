# 🚀 PGQueuer - Building Smoother Workflows One Queue at a Time 🚀

[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain) [![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer) [![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer) [![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

---

- 📚 **Documentation**: [Explore the Docs](https://pgqueuer.readthedocs.io/en/latest/)
- 🔍 **Source Code**: [View on GitHub](https://github.com/janbjorge/pgqueuer/)
- 💬 **Join the Discussion**: [Discord Community](https://discord.gg/C7YMBzcRMQ)

---

PGQueuer is a minimalist, high-performance job queue library for Python, leveraging PostgreSQL's robustness. Designed with simplicity and efficiency in mind, PGQueuer offers real-time, high-throughput processing for background jobs using PostgreSQL's LISTEN/NOTIFY and `FOR UPDATE SKIP LOCKED` mechanisms.

## Features

- **💡 Simple Integration**: Seamlessly integrates with Python applications using PostgreSQL, providing a clean and lightweight interface.
- **⚛️ Efficient Concurrency Handling**: Supports `FOR UPDATE SKIP LOCKED` to ensure reliable concurrency control and smooth job processing without contention.
- **🚧 Real-time Notifications**: Uses PostgreSQL's `LISTEN` and `NOTIFY` commands for real-time job status updates.
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

### Step 1: Write a consumer

```python
from __future__ import annotations

from datetime import datetime

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule


async def main() -> PgQueuer:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    # Entrypoint for jobs whose entrypoint is named 'fetch'.
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    # Define and register recurring tasks using cron expressions
    # The cron expression "* * * * *" means the task will run every minute
    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute {schedule!r} {datetime.now()!r}")

    return pgq
```

The above example is located in the examples folder, and can be run by using the `pgq` cli.
```bash
pgq run examples.consumer.main
```

### Step 2: Write a producer

```python
from __future__ import annotations

import asyncio
import sys

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
    N = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
    asyncio.run(main(N))
```

Run the producer:
```bash
python3 examples/producer.py 10000
```

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

