import asyncio
from datetime import datetime, timedelta

import async_timeout
import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


@pytest.mark.parametrize(
    "retry_timer",
    (
        timedelta(milliseconds=10),
        timedelta(milliseconds=100),
    ),
)
async def test_retry(
    apgdriver: db.Driver,
    retry_timer: timedelta,
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    seen = set[datetime]()

    @c.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        seen.add(context.heartbeat)
        await e.wait()

    await c.queries.enqueue(["fetch"], [None], [0])

    async def until_retry_updated() -> None:
        while len(seen) < 2:
            await asyncio.sleep(0)

        c.alive.set()
        e.set()

    await asyncio.gather(
        c.run(dequeue_timeout=timedelta(seconds=0)),
        until_retry_updated(),
    )
    assert len(seen) == 2


async def test_retry_retries(
    apgdriver: db.Driver,
    min_retries: int = 10,
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    seen = set[datetime]()

    @c.entrypoint("fetch", retry_timer=timedelta(seconds=0.01))
    async def fetch(context: Job) -> None:
        seen.add(context.heartbeat)
        await e.wait()

    await c.queries.enqueue(["fetch"], [None], [0])

    async def until_retry_updated() -> None:
        while len(seen) < min_retries:
            await asyncio.sleep(0)
        c.alive.set()
        e.set()

    async with async_timeout.timeout(10):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )
