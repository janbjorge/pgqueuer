from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta

import asyncpg
import icecream

from pgqueuer.db import AsyncpgDriver, dsn
from pgqueuer.models import Job
from pgqueuer.qb import DBSettings
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def probe(driver: AsyncpgDriver) -> None:
    while True:
        jobs = await driver.fetch(
            f"SELECT COUNT(*), entrypoint, status FROM {DBSettings().queue_table} "
            "GROUP BY (entrypoint, status) ORDER BY entrypoint, status"
        )
        icecream.ic(jobs)
        await asyncio.sleep(1)


async def producer(driver: AsyncpgDriver) -> None:
    queries = Queries(driver)
    await queries.clear_queue()
    await queries.clear_log()
    for i in range(100):
        for j in range(1, 10):
            await queries.enqueue(f"fetch_{j}", str(i).encode())

    for i in range(100):
        await queries.enqueue("fetch_0", str(i).encode())


async def consumer(driver: AsyncpgDriver) -> None:
    qm = QueueManager(driver)

    for i in range(1, 10):

        @qm.entrypoint(f"fetch_{i}", retry_timer=timedelta(minutes=2), concurrency_limit=1)
        async def process_message(job: Job) -> None:
            await asyncio.sleep(3)

    @qm.entrypoint("fetch_0", retry_timer=timedelta(minutes=3), concurrency_limit=1)
    async def fetch2(job: Job) -> None:
        assert job.payload
        print("fetch_0 start", job.payload.decode(), datetime.now().astimezone())
        await asyncio.sleep(10)

    print("startup", datetime.now().astimezone())
    await qm.run(batch_size=1, dequeue_timeout=timedelta(minutes=1))


async def main() -> None:
    connection = await asyncpg.connect(dsn())
    driver = AsyncpgDriver(connection)

    await producer(driver)
    await asyncio.gather(consumer(driver), probe(driver))


if __name__ == "__main__":
    with suppress(asyncio.CancelledError, KeyboardInterrupt):
        asyncio.run(main())
