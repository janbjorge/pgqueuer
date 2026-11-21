"""A worker at capacity must wait for a NOTIFY instead of re-polling every 100 ms."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

WINDOW = 1.0


def count_dequeues(q: Queries) -> list[int]:
    calls = [0]
    original = q.dequeue

    async def counting(**kwargs: Any) -> list[Job]:
        calls[0] += 1
        return await original(**kwargs)

    q.dequeue = counting  # type: ignore[assignment]
    return calls


async def dequeues_while_saturated(
    qm: QueueManager,
    calls: list[int],
    release: asyncio.Event,
    saturated: Callable[[], bool],
    during: Callable[[], Awaitable[None]] | None = None,
    batch_size: int = 10,
    max_concurrent_tasks: int | None = None,
) -> int:
    """Run *qm* until *saturated*, then count dequeues over WINDOW seconds."""

    async def measure() -> int:
        try:
            async with asyncio.timeout(10):
                while not saturated():
                    await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)
            before = calls[0]
            if during is not None:
                await during()
            await asyncio.sleep(WINDOW)
            return calls[0] - before
        finally:
            qm.shutdown.set()
            release.set()

    count, _ = await asyncio.gather(
        measure(),
        qm.run(
            dequeue_timeout=timedelta(seconds=5),
            batch_size=batch_size,
            max_concurrent_tasks=max_concurrent_tasks,
        ),
    )
    return count


async def test_worker_budget_full_does_not_repoll(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    calls = count_dequeues(q)
    qm = QueueManager(q)
    release = asyncio.Event()
    running = 0

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None:
        nonlocal running
        running += 1
        await release.wait()

    await q.enqueue(["ep"] * 10, [None] * 10, [0] * 10)
    count = await dequeues_while_saturated(
        qm, calls, release, lambda: running == 4, batch_size=2, max_concurrent_tasks=4
    )
    assert count <= 2, count


async def test_entrypoint_limit_full_does_not_repoll(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    calls = count_dequeues(q)
    qm = QueueManager(q)
    release = asyncio.Event()
    running = 0

    @qm.entrypoint("ep", concurrency_limit=1)
    async def ep(job: Job) -> None:
        nonlocal running
        running += 1
        await release.wait()

    await q.enqueue(["ep"] * 3, [None] * 3, [0] * 3)
    count = await dequeues_while_saturated(qm, calls, release, lambda: running == 1)
    assert count <= 2, count


async def test_notify_burst_wakes_saturated_worker_once(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    calls = count_dequeues(q)
    qm = QueueManager(q)
    release = asyncio.Event()
    running = 0

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None:
        nonlocal running
        running += 1
        await release.wait()

    async def burst() -> None:
        await asyncio.gather(*(q.notify_table_changed("insert") for _ in range(20)))

    await q.enqueue(["ep"] * 10, [None] * 10, [0] * 10)
    count = await dequeues_while_saturated(
        qm, calls, release, lambda: running == 4, burst, batch_size=2, max_concurrent_tasks=4
    )
    assert count <= 3, count


async def test_held_failure_wakes_full_worker(apgdriver: Driver) -> None:
    """A failed job held in the queue table frees budget, and its flush must wake the worker."""
    q = Queries(apgdriver)
    qm = QueueManager(q)
    release = asyncio.Event()
    seen = 0

    @qm.entrypoint("ep", on_failure="hold")
    async def ep(job: Job) -> None:
        nonlocal seen
        seen += 1
        await release.wait()
        raise ValueError("held")

    await q.enqueue(["ep"] * 4, [None] * 4, [0] * 4)

    async def driver() -> None:
        try:
            async with asyncio.timeout(10):
                while seen < 2:
                    await asyncio.sleep(0.01)
            await asyncio.sleep(0.2)
            assert seen == 2
            release.set()
            # dequeue_timeout is 30 s, so only a NOTIFY can bring the last two jobs this fast.
            async with asyncio.timeout(3):
                while seen < 4:
                    await asyncio.sleep(0.01)
        finally:
            release.set()
            qm.shutdown.set()

    await asyncio.gather(
        driver(),
        qm.run(dequeue_timeout=timedelta(seconds=30), batch_size=1, max_concurrent_tasks=2),
    )
