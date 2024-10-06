from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import timedelta
from itertools import chain

import pytest

from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def raises(lock: asyncio.Lock) -> None:
    if lock.locked():
        raise ValueError("Locked")


async def consumer(
    qm: QueueManager,
    lock: asyncio.Lock,
) -> defaultdict[str, list[Job]]:
    consumed = defaultdict[str, list[Job]](list)

    @qm.entrypoint("serialized_dispatch_true", serialized_dispatch=True)
    async def serialized_dispatch_true(job: Job) -> None:
        consumed["serialized_dispatch_true"].append(job)
        await asyncio.sleep(0.1)
        await raises(lock)

    @qm.entrypoint("serialized_dispatch_false", serialized_dispatch=False)
    async def fetch(job: Job) -> None:
        consumed["serialized_dispatch_false"].append(job)
        await asyncio.sleep(0.1)

    await qm.run(dequeue_timeout=timedelta(seconds=0))
    return consumed


async def enqueue(
    queries: Queries,
    size: int,
) -> None:
    assert size > 0
    await queries.enqueue(
        ["serialized_dispatch_true"] * size,
        [f"{n}".encode() for n in range(size)],
        [0] * size,
    )
    await queries.enqueue(
        ["serialized_dispatch_false"] * size,
        [f"{n}".encode() for n in range(size)],
        [0] * size,
    )


@pytest.mark.parametrize("n_consumers", (1, 2, 4, 32))
async def test_serialized_dispatch(
    n_consumers: int,
    apgdriver: Driver,
    n_tasks: int = 500,
    wait: int = 1,
) -> None:
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(apgdriver) for _ in range(n_consumers)]

    async def dequeue() -> None:
        consumers = [consumer(q, lock) for q in qms]
        await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.alive.set()

    await asyncio.gather(timer(), dequeue())


@pytest.mark.parametrize("n_consumers", (1, 2, 4))
async def test_serialized_serialized_dispatch_true_and_false(
    n_consumers: int,
    apgdriver: Driver,
    n_tasks: int = 10_000,
    wait: int = 1,
) -> None:
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(apgdriver) for _ in range(n_consumers)]

    async def dequeue() -> list[defaultdict[str, list[Job]]]:
        consumers = [consumer(q, lock) for q in qms]
        return await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.alive.set()

    _, consumed = await asyncio.gather(timer(), dequeue())
    serialized_dispatch_true = list(
        chain.from_iterable(x["serialized_dispatch_true"] for x in consumed)
    )
    serialized_dispatch_false = list(
        chain.from_iterable(x["serialized_dispatch_false"] for x in consumed)
    )
    assert len(serialized_dispatch_false) / len(serialized_dispatch_true) > n_consumers
