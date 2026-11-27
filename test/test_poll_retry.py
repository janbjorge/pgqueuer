"""A failed poll does not end the run at once (#1024)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


def flaky_dequeue(queries: InMemoryQueries, script: list[bool]) -> list[bool]:
    """Make dequeue raise on each call that *script* marks True. Return the calls."""
    original = queries.dequeue
    calls: list[bool] = []

    async def dequeue(*args: Any, **kwargs: Any) -> list[Job]:
        fail = script[len(calls)] if len(calls) < len(script) else False
        calls.append(fail)
        if fail:
            raise RuntimeError("dequeue failed")
        return await original(*args, **kwargs)

    queries.dequeue = dequeue
    return calls


async def wait_or_end(run: asyncio.Task[None], condition: Callable[[], bool]) -> None:
    """Wait for *condition*. If *run* ends first, raise its error."""
    async with asyncio.timeout(10):
        while not condition():
            if run.done():
                await run
                pytest.fail("run ended early")
            await asyncio.sleep(0.01)


async def test_one_failed_poll_does_not_end_run(queries: InMemoryQueries) -> None:
    qm = QueueManager(queries)
    done = asyncio.Event()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        done.set()

    calls = flaky_dequeue(queries, [True])
    run = asyncio.create_task(qm.run(dequeue_timeout=timedelta(seconds=0.01)))
    await wait_or_end(run, lambda: len(calls) >= 1)

    await queries.enqueue(["fetch"], [None], [0])
    await wait_or_end(run, done.is_set)

    qm.shutdown.set()
    await asyncio.wait_for(run, timeout=5)


async def test_three_failed_polls_end_run(queries: InMemoryQueries) -> None:
    qm = QueueManager(queries)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None: ...

    calls = flaky_dequeue(queries, [True] * 4)
    with pytest.raises(RuntimeError, match="dequeue failed"):
        async with asyncio.timeout(10):
            await qm.run(dequeue_timeout=timedelta(seconds=0.01))

    assert len(calls) == 3


async def test_successful_poll_resets_failure_count(queries: InMemoryQueries) -> None:
    qm = QueueManager(queries)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None: ...

    calls = flaky_dequeue(queries, [True, False, True, False, True])
    run = asyncio.create_task(qm.run(dequeue_timeout=timedelta(seconds=0.01)))
    await wait_or_end(run, lambda: len(calls) >= 6)

    qm.shutdown.set()
    await asyncio.wait_for(run, timeout=5)
