### Readme
## 🚀 PGQueuer - Building Smoother Workflows One Queue at a Time 🚀
[![CI](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml/badge.svg)](https://github.com/janbjorge/pgqueuer/actions/workflows/ci.yml?query=branch%3Amain)
[![pypi](https://img.shields.io/pypi/v/pgqueuer.svg)](https://pypi.python.org/pypi/pgqueuer)
[![downloads](https://static.pepy.tech/badge/pgqueuer/month)](https://pepy.tech/project/pgqueuer)
[![versions](https://img.shields.io/pypi/pyversions/pgqueuer.svg)](https://github.com/janbjorge/pgqueuer)

---

📚 **Documentation**: [Explore the Docs 📖](https://pgqueuer.readthedocs.io/en/latest/)

🔍 **Source Code**: [View on GitHub 💾](https://github.com/janbjorge/pgqueuer/)

💬 **Join the Discussion**: [Discord Community](https://discord.gg/C7YMBzcRMQ)

---

## PGQueuer

PGQueuer is a minimalist, high-performance job queue library for Python, leveraging the robustness of PostgreSQL. Designed for simplicity and efficiency, PGQueuer uses PostgreSQL's LISTEN/NOTIFY to manage job queues effortlessly.

### Features

- **Simple Integration**: Easy to integrate with existing Python applications using PostgreSQL.
- **Efficient Concurrency Handling**: Utilizes PostgreSQL's `FOR UPDATE SKIP LOCKED` for reliable and concurrent job processing.
- **Real-time Notifications**: Leverages `LISTEN` and `NOTIFY` for real-time updates on job status changes.

### Installation

To install PGQueuer, simply install with pip the following command:

```bash
pip install pgqueuer
```

### Example Usage

Here's how you can use PGQueuer in a typical scenario processing incoming data messages:

#### Write and run a consumer
Start a long-lived consumer that will begin processing jobs as soon as they are enqueued by another process. In this case we want to be a bit more carefull as we want gracefull shutdowns, `pgqueuer run` will setup signals to
ensure this.

```python
from __future__ import annotations

import asyncpg
from pgqueuer.db import AsyncpgDriver, dsn
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


async def main() -> QueueManager:
    connection = await asyncpg.connect(dsn())
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    return qm
```

```bash
python3 -m pgqueuer run tools.consumer.main
```

#### Write and run a producer
Start a short-lived producer that will enqueue 10,000 jobs.
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
        [f"this is from me: {n}".encode() for n in range(1, N+1)],
        [0] * N,
    )


if __name__ == "__main__":
    print(sys.argv)
    N = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
    asyncio.run(main(N))
```

```bash
python3 tools/producer.py 10000
```
