# test/test_retry.py

import asyncio
import asyncio.selector_events
from datetime import datetime, timedelta

import async_timeout
import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


@pytest.mark.parametrize("N", (2, 4, 8, 16))
async def test_retry(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
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
        while len(seen) <= N:
            await asyncio.sleep(0)
        c.alive.set()
        e.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 2 * N):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert (
        pytest.approx(
            (max(seen) - min(seen)).total_seconds(),
            abs=retry_timer.total_seconds(),
        )
        == (N * retry_timer).total_seconds()
    )


async def test_no_retry_on_zero_timer(
    apgdriver: db.Driver,
    retry_timer: timedelta = timedelta(seconds=0),
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
        await asyncio.sleep(1)
        c.alive.set()
        e.set()

    await asyncio.gather(
        c.run(dequeue_timeout=timedelta(seconds=0)),
        until_retry_updated(),
    )

    assert len(seen) == 1


@pytest.mark.parametrize("N", (2, 4))
async def test_heartbeat_updates(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    seen = []

    @c.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        seen.append(context.heartbeat)
        await e.wait()

    await c.queries.enqueue(["fetch"], [None], [0])

    async def until_retry_updated() -> None:
        while len(seen) < N:
            await asyncio.sleep(0)
        c.alive.set()
        e.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 2 * N):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    for earlier, later in zip(seen, seen[1:]):
        assert (later - earlier) >= retry_timer


@pytest.mark.parametrize("N", (2, 4))
async def test_concurrent_retry_jobs(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e1 = asyncio.Event()
    e2 = asyncio.Event()
    c = QueueManager(apgdriver)
    seen_job1 = set[datetime]()
    seen_job2 = set[datetime]()

    @c.entrypoint("fetch1", retry_timer=retry_timer)
    async def fetch1(context: Job) -> None:
        seen_job1.add(context.heartbeat)
        await e1.wait()

    @c.entrypoint("fetch2", retry_timer=retry_timer)
    async def fetch2(context: Job) -> None:
        seen_job2.add(context.heartbeat)
        await e2.wait()

    await c.queries.enqueue(["fetch1", "fetch2"], [None, None], [0, 0])

    async def until_retry_updated() -> None:
        while len(seen_job1) < N or len(seen_job2) < N:
            await asyncio.sleep(0)
        c.alive.set()
        e1.set()
        e2.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 2 * N):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert (
        pytest.approx(
            (max(seen_job1) - min(seen_job1)).total_seconds(),
            abs=retry_timer.total_seconds(),
        )
        == (N * retry_timer).total_seconds()
    )
    assert (
        pytest.approx(
            (max(seen_job2) - min(seen_job2)).total_seconds(),
            abs=retry_timer.total_seconds(),
        )
        == (N * retry_timer).total_seconds()
    )


@pytest.mark.parametrize("N", (2, 4))
async def test_varying_retry_timers(
    apgdriver: db.Driver,
    N: int,
    retry_timer_short: timedelta = timedelta(seconds=0.050),
    retry_timer_long: timedelta = timedelta(seconds=0.150),
) -> None:
    e_short = asyncio.Event()
    e_long = asyncio.Event()
    c = QueueManager(apgdriver)
    seen_short = set[datetime]()
    seen_long = set[datetime]()

    @c.entrypoint("fetch_short", retry_timer=retry_timer_short)
    async def fetch_short(context: Job) -> None:
        seen_short.add(context.heartbeat)
        await e_short.wait()

    @c.entrypoint("fetch_long", retry_timer=retry_timer_long)
    async def fetch_long(context: Job) -> None:
        seen_long.add(context.heartbeat)
        await e_long.wait()

    await c.queries.enqueue(["fetch_short", "fetch_long"], [None, None], [0, 0])

    async def until_retry_updated() -> None:
        while len(seen_short) < N or len(seen_long) < N:
            await asyncio.sleep(0)
        c.alive.set()
        e_short.set()
        e_long.set()

    async with async_timeout.timeout(
        max(retry_timer_short, retry_timer_long).total_seconds() * 2 * N
    ):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    for earlier, later in zip(sorted(seen_short), sorted(seen_short)[1:]):
        assert (later - earlier) >= retry_timer_short
    for earlier, later in zip(sorted(seen_long), sorted(seen_long)[1:]):
        assert (later - earlier) >= retry_timer_long


@pytest.mark.parametrize("N", (2, 4))
async def test_retry_with_cancellation(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    seen = set[datetime]()
    call_count = 0

    @c.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        nonlocal call_count
        seen.add(context.heartbeat)
        call_count += 1
        if call_count < N:
            raise asyncio.CancelledError("Simulated cancellation")
        await e.wait()

    await c.queries.enqueue(["fetch"], [None], [0])

    async def until_retry_updated() -> None:
        while len(seen) < N:
            await asyncio.sleep(0)
        c.alive.set()
        e.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 2 * N):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert call_count == N
    assert (
        pytest.approx(
            (max(seen) - min(seen)).total_seconds(),
            abs=retry_timer.total_seconds(),
        )
        == (N * retry_timer).total_seconds()
    )
