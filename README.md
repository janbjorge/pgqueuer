## 🚀 PgQueuer - Building Smoother Workflows One Queue at a Time 🚀
[![CI](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/PgQueuer/actions/workflows/ci.yml?query=branch%3Amain)
[![pypi](https://img.shields.io/pypi/v/PgQueuer.svg)](https://pypi.python.org/pypi/PgQueuer)
[![downloads](https://static.pepy.tech/badge/PgQueuer/month)](https://pepy.tech/project/PgQueuer)
[![versions](https://img.shields.io/pypi/pyversions/PgQueuer.svg)](https://github.com/janbjorge/PgQueuer)

---

📚 **Documentation**: [Explore the Docs 📖](https://pgqueuer.readthedocs.io/en/latest/)

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

### Example Usage

Here's how you can use PgQueuer in a typical scenario processing incoming data messages:

```python
import asyncio

import asyncpg
from PgQueuer.db import AsyncPGDriver
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    connection = await asyncpg.connect()
    driver = AsyncPGDriver(connection)
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
