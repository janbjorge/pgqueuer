from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import count

import pytest

from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


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
) -> None:
    assert size > 0
    cnt = count()
    entrypoints = ["fetch"] * size
    await queries.enqueue(
        entrypoints,
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
            q.alive.set()

    await asyncio.gather(timer(), dequeue())

    for tally in tallys:
        assert max_concurrency - 1 <= tally.max_active <= max_concurrency
