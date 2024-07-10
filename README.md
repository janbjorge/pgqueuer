### Readme
## 🚀 PgQueuer - Building Smoother Workflows One Queue at a Time 🚀
[![CI](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml?query=branch%3Amain)
[![pypi](https://img.shields.io/pypi/v/PgQueuer.svg)](https://pypi.python.org/pypi/PgQueuer)
[![downloads](https://static.pepy.tech/badge/PgQueuer/month)](https://pepy.tech/project/PgQueuer)
[![versions](https://img.shields.io/pypi/pyversions/PgQueuer.svg)](https://github.com/janbjorge/PgQueuer)

---

📚 **Documentation**: [Explore the Docs 📖](https://pgqueuer.readthedocs.io/en/latest/)

🔍 **Source Code**: [View on GitHub 💾](https://github.com/janbjorge/PgQueuer/)

💬 **Join the Discussion**: [Discord Community](https://discord.gg/C7YMBzcRMQ)

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

### Example Usage

Here's how you can use PgQueuer in a typical scenario processing incoming data messages:

```python
import asyncio

import asyncpg
from PgQueuer.db import AsyncpgDriver
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    # Establish a database connection; asyncpg and psycopg are supported.
    connection = await asyncpg.connect()
    # Initialize a database driver
    driver = AsyncpgDriver(connection)
    # Create a queue manager which orchestrates entrypoints based on registered names.
    qm = QueueManager(driver)

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
