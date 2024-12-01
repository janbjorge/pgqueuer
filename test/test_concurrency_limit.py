from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import count

import async_timeout
import pytest

from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qb import DBSettings
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import JobId


async def _inspect_queue_jobs(jids: list[JobId], driver: Driver) -> list[Job]:
    sql = f"""SELECT * FROM {DBSettings().queue_table} WHERE id = ANY($1::integer[])"""
    return [Job.model_validate(x) for x in await driver.fetch(sql, jids)]


@dataclass
class Tally:
    active: int = 0
    max_active: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def __aenter__(self) -> None:
        async with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

    async def __aexit__(self, *_: object) -> None:
        async with self.lock:
            self.active -= 1


async def consumer(qm: QueueManager, tally: Tally, limit: int) -> None:
    @qm.entrypoint("fetch", concurrency_limit=limit)
    async def fetch(job: Job) -> None:
        async with tally:
            await asyncio.sleep(0.001)

    await qm.run(dequeue_timeout=timedelta(seconds=0))


async def enqueue(
    queries: Queries,
    size: int,
    cnt: count = count(),
) -> None:
    assert size > 0
    await queries.enqueue(
        ["fetch"] * size,
        [f"{next(cnt)}".encode() for _ in range(size)],
        [0] * size,
    )


@pytest.mark.parametrize("n_consumers", (1, 4))
@pytest.mark.parametrize("max_concurrency", (1, 5, 10))
async def test_max_concurrency(
    n_consumers: int,
    max_concurrency: int,
    apgdriver: Driver,
    n_tasks: int = 500,
    wait: int = 1,
) -> None:
    await enqueue(Queries(apgdriver), size=n_tasks)

    tallys = [Tally() for _ in range(n_consumers)]
    qms = [QueueManager(apgdriver) for _ in range(n_consumers)]

    async def dequeue() -> None:
        consumers = [consumer(q, tally, max_concurrency) for q, tally in zip(qms, tallys)]
        await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    await asyncio.gather(timer(), dequeue())

    for tally in tallys:
        assert max_concurrency - 1 <= tally.max_active <= max_concurrency


@pytest.mark.parametrize("concurrency_limit", (1, 5, 10))
@pytest.mark.parametrize("batch_size", (1, 5, 10))
@pytest.mark.parametrize("dequeue_timeout", (1, 5, 10))
async def test_concurrency_entrypoint_isolation(
    apgdriver: Driver,
    concurrency_limit: int,
    batch_size: int,
    dequeue_timeout: float,
) -> None:
    event = asyncio.Event()
    N = concurrency_limit * 1_000
    await Queries(apgdriver).enqueue(
        ["fetch_1", "fetch_2"] * N,
        [None, None] * N,
        [0, 0] * N,
    )

    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch_1", concurrency_limit=concurrency_limit)
    async def fetch_1(job: Job) -> None:
        await event.wait()

    @qm.entrypoint("fetch_2", concurrency_limit=concurrency_limit)
    async def fetch_2(job: Job) -> None:
        await event.wait()

    async def timer() -> None:
        async with async_timeout.timeout(10):
            while len(event._waiters) < 2 * concurrency_limit:
                await asyncio.sleep(0.001)
            len_waiter = len(event._waiters)
            qm.shutdown.set()
            event.set()
            assert len_waiter == 2 * concurrency_limit

    await asyncio.gather(
        timer(),
        qm.run(
            batch_size=batch_size,
            dequeue_timeout=timedelta(seconds=dequeue_timeout),
        ),
    )
