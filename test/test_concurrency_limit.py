from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import async_timeout
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


@pytest.mark.parametrize("n_consumers", (1, 2, 4))
@pytest.mark.parametrize("max_concurrency", (1, 5, 10))
async def test_max_concurrency(
    n_consumers: int,
    max_concurrency: int,
    apgdriver: Driver,
    n_tasks: int = 500,
    wait: int = 2,
) -> None:
    """concurrency_limit is enforced globally across all workers."""
    await Queries(apgdriver).enqueue(
        ["fetch"] * n_tasks,
        [f"{i}".encode() for i in range(n_tasks)],
        [0] * n_tasks,
    )

    shared = Tally()
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def run_consumer(qm: QueueManager) -> None:
        @qm.entrypoint("fetch", concurrency_limit=max_concurrency)
        async def fetch(job: Job) -> None:
            async with shared:
                await asyncio.sleep(0.001)

        await qm.run(dequeue_timeout=timedelta(seconds=0))

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    await asyncio.gather(timer(), *(run_consumer(q) for q in qms))

    assert 0 < shared.max_active <= max_concurrency


@pytest.mark.parametrize("concurrency_limit", (1, 5, 10))
async def test_concurrency_entrypoint_isolation(
    apgdriver: Driver,
    concurrency_limit: int,
) -> None:
    event = asyncio.Event()
    N = concurrency_limit * 1_000
    await Queries(apgdriver).enqueue(
        ["fetch_1", "fetch_2"] * N,
        [None, None] * N,
        [0, 0] * N,
    )

    qm = QueueManager(Queries(apgdriver))

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
            batch_size=5,
            dequeue_timeout=timedelta(seconds=5),
        ),
    )


async def test_tight_entrypoint_does_not_throttle_unlimited_entrypoint(
    apgdriver: Driver,
) -> None:
    """A limit=1 entrypoint must not collapse dequeue batches for other entrypoints."""
    N = 20
    batch_size = 10
    q = Queries(apgdriver)
    await q.enqueue(["unlimited"] * N, [None] * N, [0] * N)

    qm = QueueManager(q)
    event = asyncio.Event()

    @qm.entrypoint("tight", concurrency_limit=1)
    async def tight(job: Job) -> None:
        await event.wait()

    @qm.entrypoint("unlimited")
    async def unlimited(job: Job) -> None:
        await event.wait()

    dequeue_batches = list[int]()
    original_dequeue = q.dequeue

    async def recording_dequeue(*, batch_size: int, **kwargs: Any) -> list[Job]:
        dequeue_batches.append(batch_size)
        return await original_dequeue(batch_size=batch_size, **kwargs)

    q.dequeue = recording_dequeue  # type: ignore[assignment]

    async def timer() -> None:
        async with async_timeout.timeout(10):
            while len(event._waiters) < N:
                await asyncio.sleep(0.001)
            qm.shutdown.set()
            event.set()

    await asyncio.gather(
        timer(),
        qm.run(batch_size=batch_size, dequeue_timeout=timedelta(seconds=1)),
    )

    assert dequeue_batches
    assert set(dequeue_batches) == {batch_size}
