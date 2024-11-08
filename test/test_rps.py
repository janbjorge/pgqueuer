from __future__ import annotations

import asyncio
import random
from datetime import timedelta
from itertools import count

import pytest

from pgqueuer.db import Driver
from pgqueuer.models import Job, JobId
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def consumer(qm: QueueManager, jobs: list[JobId]) -> None:
    @qm.entrypoint("asyncfetch", requests_per_second=100)
    async def asyncfetch(job: Job) -> None:
        jobs.append(job.id)

    @qm.entrypoint("syncfetch", requests_per_second=10)
    def syncfetch(job: Job) -> None:
        jobs.append(job.id)

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
    jobs = list[JobId]()

    await enqueue(Queries(apgdriver), size=n_tasks)

    qms = [QueueManager(apgdriver) for _ in range(concurrency)]

    async def dequeue() -> None:
        consumers = [consumer(q, jobs) for q in qms]
        await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    await asyncio.gather(timer(), dequeue())
    lower, upper = 50 * wait, 140 * wait
    assert lower <= len(jobs) <= upper
    assert len(set(jobs)) == len(jobs)
