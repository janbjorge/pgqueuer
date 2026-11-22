"""A worker run with default settings claims a bounded share of a backlog (#1090).

The check runs at the moment the worker first goes idle: its fetch loop has stopped
claiming, so whatever it holds then is what it took. No assertion depends on timing;
GUARD fires only on a hang.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.ports import RepositoryPort
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

DEFAULT_BUDGET = 100
BACKLOG = DEFAULT_BUDGET + 50
GUARD = 15


async def claimed_when_first_idle(q: RepositoryPort) -> int:
    qm = QueueManager(q)
    release = asyncio.Semaphore(0)
    claimed = 0
    idle = asyncio.Event()

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None:
        await release.acquire()

    original_dequeue = q.dequeue

    async def counting_dequeue(*args: Any, **kwargs: Any) -> Any:
        nonlocal claimed
        jobs = await original_dequeue(*args, **kwargs)
        claimed += len(jobs)
        return jobs

    q.dequeue = counting_dequeue

    original_timeout = qm._effective_dequeue_timeout

    async def first_idle(*args: Any, **kwargs: Any) -> timedelta:
        idle.set()
        return await original_timeout(*args, **kwargs)

    qm._effective_dequeue_timeout = first_idle

    await q.enqueue(["ep"] * BACKLOG, [None] * BACKLOG, [0] * BACKLOG)
    task = asyncio.create_task(qm.run(dequeue_timeout=timedelta(hours=1)))
    try:
        async with asyncio.timeout(GUARD):
            await idle.wait()
        return claimed
    finally:
        qm.shutdown.set()
        for _ in range(BACKLOG):
            release.release()
        await task


async def test_default_run_claims_bounded_share(apgdriver: Driver) -> None:
    assert await claimed_when_first_idle(Queries(apgdriver)) == DEFAULT_BUDGET


async def test_default_run_claims_bounded_share_inmemory() -> None:
    queries = InMemoryQueries(driver=InMemoryDriver())
    assert await claimed_when_first_idle(queries) == DEFAULT_BUDGET
