"""Workers denied capacity on a limited entrypoint re-poll instead of sleeping (#761)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import timedelta

import asyncpg
import pytest

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def drain_with_fleet(
    dsn: str,
    n_workers: int,
    n_jobs: int,
    concurrency_limit: int,
    handler_sleep: float,
    dequeue_timeout: timedelta,
) -> float:
    """Run *n_workers* managers over a shared limited entrypoint, return wall seconds."""
    async with contextlib.AsyncExitStack() as stack:

        async def connect() -> asyncpg.Connection:
            connection = await asyncpg.connect(dsn=dsn)
            stack.push_async_callback(connection.close)
            return connection

        await Queries(AsyncpgDriver(await connect())).enqueue(
            ["contended"] * n_jobs,
            [None] * n_jobs,
            [0] * n_jobs,
        )

        drained = asyncio.Event()
        processed = 0
        managers = list[QueueManager]()

        for _ in range(n_workers):
            manager = QueueManager(Queries(AsyncpgDriver(await connect())))

            @manager.entrypoint("contended", concurrency_limit=concurrency_limit)
            async def contended(_: Job) -> None:
                nonlocal processed
                await asyncio.sleep(handler_sleep)
                processed += 1
                if processed >= n_jobs:
                    drained.set()

            managers.append(manager)

        async def stop_when_drained() -> None:
            await drained.wait()
            for manager in managers:
                manager.shutdown.set()

        workers = [m.run(batch_size=10, dequeue_timeout=dequeue_timeout) for m in managers]
        started = time.monotonic()
        await asyncio.gather(stop_when_drained(), *workers)
        elapsed = time.monotonic() - started

        assert processed == n_jobs
        return elapsed


@pytest.mark.parametrize("n_workers", (1, 4, 16))
async def test_slot_contention_does_not_park_workers(
    dsn: str,
    n_workers: int,
    n_jobs: int = 30,
    concurrency_limit: int = 1,
    handler_sleep: float = 0.05,
    dequeue_timeout: timedelta = timedelta(seconds=10),
) -> None:
    """A fleet contending for one capacity slot drains without waiting out a timeout."""
    elapsed = await drain_with_fleet(
        dsn,
        n_workers,
        n_jobs,
        concurrency_limit,
        handler_sleep,
        dequeue_timeout,
    )

    assert elapsed < dequeue_timeout.total_seconds(), (
        f"{n_workers} workers took {elapsed:.2f}s to drain {n_jobs} jobs, "
        f"which means at least one worker waited out the {dequeue_timeout} dequeue timeout"
    )
