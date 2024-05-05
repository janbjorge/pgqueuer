## 🚀 PgQueuer - Building Smoother Workflows One Queue at a Time 🚀
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
  python -m PgQueuer uninstall 
  ```

The CLI supports several flags to customize the connection settings. Use `--help` to see all available options.

#### Dashboard Command

The dashboard command provides a real-time view of job processing statistics, which can be refreshed at a specified interval. This is particularly useful for monitoring the status of jobs dynamically. Below are the options available for customizing the dashboard display:

  - `--interval <seconds>`: Set the refresh interval in seconds for updating the dashboard display. If not set, the dashboard will update only once and then exit.
  - `--tail <number>`: Specify the number of the most recent log entries to display.
  - `--table-format <format>`: Choose the format of the table used to display statistics. Supported formats include grid, plain, html, and others provided by the tabulate library.

Example command to launch the dashboard:
```bash
python -m PgQueuer dashboard --interval 10 --tail 25 --table-format grid
```

Example output from the dashboard:
```bash
+---------------------------+-------+------------+--------------------------+------------+----------+
|          Created          | Count | Entrypoint | Time in Queue (HH:MM:SS) |   Status   | Priority |
+---------------------------+-------+------------+--------------------------+------------+----------+
| 2024-05-05 16:44:26+00:00 |  49   |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:26+00:00 |  82   |   async    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:26+00:00 | 1615  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:26+00:00 | 1586  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:25+00:00 |  198  |    sync    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:25+00:00 |  230  |   async    |         0:00:01          | successful |    0     |
| 2024-05-05 16:44:25+00:00 | 1802  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:25+00:00 | 1778  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:24+00:00 | 1500  |    sync    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:24+00:00 | 1506  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:23+00:00 | 1505  |   async    |         0:00:00          | successful |    0     |
| 2024-05-05 16:44:23+00:00 | 1500  |    sync    |         0:00:00          | successful |    0     |
+---------------------------+-------+------------+--------------------------+------------+----------+
```

### Example Usage

Here's how you can use PgQueuer in a typical scenario processing incoming data messages:

```python
import asyncio

import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    pool = await asyncpg.create_pool(min_size=2)
    qm = QueueManager(pool)

    N = 1_000
    # Enqueue messages.
    for n in range(N):
        await qm.queries.enqueue("fetch", f"this is from me: {n}".encode())

    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Benchmark Summary

PgQueuer underwent basic benchmark testing to assess its performance across varying job volumes and concurrency levels.

#### Key Observations:
- **Scalability**: Performance increases with higher concurrency, demonstrating the library's ability to efficiently manage larger workloads.
- **Consistency**: PgQueuer maintains consistent throughput across different job counts, ensuring reliable performance.
- **Optimal Performance**: The highest throughput observed was ~5,200 jobs per second at a concurrency level of 5.