import asyncio

import asyncpg

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def main() -> None:
    qm_conn = await asyncpg.connect()

    driver = AsyncpgDriver(qm_conn)
    qm = QueueManager(driver)

    @qm.entrypoint("fetch", concurrency_limit=200)
    async def fetch(job: Job) -> None:
        await asyncio.sleep(0.1)
        # await asyncio.sleep(float("inf"))

    enq_conn = await asyncpg.connect()
    q = Queries(AsyncpgDriver(enq_conn))
    N = 100

    async def enqueue() -> None:
        while True:
            await q.enqueue(
                ["fetch"] * N,
                [None] * N,
                [0] * N,
            )
            await asyncio.sleep(0)
            print(len(asyncio.all_tasks()), (sum(q.count for q in await q.queue_size())))

    await asyncio.gather(enqueue(), qm.run(batch_size=N))


asyncio.run(main())
