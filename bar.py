from __future__ import annotations

import asyncio

import asyncpg

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def main() -> None:
    connection = await asyncpg.connect()

    driver = AsyncpgDriver(connection)
    N = 10
    await Queries(driver).enqueue(["fetch"] * N, [None] * N, [0] * N)
    qm = QueueManager(driver)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")
        await asyncio.sleep(10000)

    await qm.run()


asyncio.run(main())
