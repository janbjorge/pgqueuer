from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import timedelta
from itertools import count

import pytest

from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


@dataclass
class Tally:
    count: int


async def consumer(qm: QueueManager, tally: Tally) -> None:
    @qm.entrypoint("asyncfetch", requests_per_second=100)
    async def asyncfetch(job: Job) -> None:
        tally.count += 1

    @qm.entrypoint("syncfetch", requests_per_second=10)
    def syncfetch(job: Job) -> None:
        tally.count += 1

    await qm.run(dequeue_timeout=timedelta(seconds=0))


async def enqueue(
    queries: Queries,
    size: int,
) -> None:
    assert size > 0
    cnt = count()
    entrypoints = ["syncfetch", "asyncfetch"] * size
    await queries.enqueue(
        random.sample(entrypoints, k=size),
        [f"{next(cnt)}".encode() for _ in range(size)],
        [0] * size,
    )


@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_rps(
    concurrency: int,
    apgdriver: Driver,
    n_tasks: int = 1_000,
    wait: int = 5,
) -> None:
    tally = Tally(count=0)

    await enqueue(Queries(apgdriver), size=n_tasks)

    qms = [QueueManager(apgdriver) for _ in range(concurrency)]

    async def dequeue() -> None:
        consumers = [consumer(q, tally) for q in qms]
        await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.alive.set()

    await asyncio.gather(timer(), dequeue())
    assert 100 <= tally.count / wait <= 140
