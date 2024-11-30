import asyncio
import asyncio.selector_events
from collections import Counter
from datetime import datetime, timedelta

import async_timeout
import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qb import DBSettings
from pgqueuer.qm import QueueManager
from pgqueuer.types import JobId


async def _inspect_queue_jobs(jids: list[JobId], driver: db.Driver) -> list[Job]:
    sql = f"""SELECT * FROM {DBSettings().queue_table} WHERE id = ANY($1::integer[])"""
    return [Job.model_validate(x) for x in await driver.fetch(sql, jids)]


@pytest.mark.parametrize("N", (2, 4, 8, 16))
async def test_retry(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    jobids = Counter[JobId]()

    @c.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        jobids[context.id] += 1
        await e.wait()

    await c.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def stop_after() -> None:
        async with async_timeout.timeout(retry_timer.total_seconds() * 10):
            while True:
                if len(jobids) == N and all(v > 1 for v in jobids.values()):
                    break
                await asyncio.sleep(0)

        e.set()
        c.shutdown.set()

    await asyncio.gather(
        c.run(dequeue_timeout=timedelta(seconds=0)),
        stop_after(),
    )
    assert len(jobids) == N
    assert all(v > 1 for v in jobids.values())


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
        c.shutdown.set()
        e.set()

    await asyncio.gather(
        c.run(dequeue_timeout=timedelta(seconds=0)),
        until_retry_updated(),
    )

    assert len(seen) == 1


async def test_heartbeat_updates(
    apgdriver: db.Driver,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e = asyncio.Event()
    c = QueueManager(apgdriver)
    seen = []

    @c.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        seen.append(context.heartbeat)
        await e.wait()

    jids = await c.queries.enqueue(["fetch"], [None], [0])
    (before,) = await _inspect_queue_jobs(jids, apgdriver)
    after: None | Job = None

    async def until_retry_updated() -> None:
        nonlocal after
        while len(seen) < 2:
            await asyncio.sleep(0)
        (after,) = await _inspect_queue_jobs(jids, apgdriver)
        c.shutdown.set()
        e.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 10):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert after is not None
    assert after.heartbeat > before.heartbeat
    assert len(set(seen)) > 1


@pytest.mark.parametrize("N", (2, 4))
async def test_concurrent_retry_jobs(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.100),
) -> None:
    e1 = asyncio.Event()
    e2 = asyncio.Event()
    c = QueueManager(apgdriver)
    seen_job1 = Counter[JobId]()
    seen_job2 = Counter[JobId]()

    @c.entrypoint("fetch1", retry_timer=retry_timer)
    async def fetch1(context: Job) -> None:
        seen_job1[context.id] += 1
        await e1.wait()

    @c.entrypoint("fetch2", retry_timer=retry_timer)
    async def fetch2(context: Job) -> None:
        seen_job2[context.id] += 1
        await e2.wait()

    await c.queries.enqueue(["fetch1", "fetch2"] * N, [None, None] * N, [0, 0] * N)

    async def until_retry_updated() -> None:
        while True:
            if (
                len(seen_job1) == N
                and all(v > 1 for v in seen_job1.values())
                and len(seen_job2) == N
                and all(v > 1 for v in seen_job2.values())
            ):
                break
            await asyncio.sleep(0)

        c.shutdown.set()
        e1.set()
        e2.set()

    async with async_timeout.timeout(retry_timer.total_seconds() * 2 * N):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert len(seen_job1) == N
    assert all(v > 1 for v in seen_job1.values())

    assert len(seen_job2) == N
    assert all(v > 1 for v in seen_job2.values())


@pytest.mark.parametrize("N", (2, 4))
async def test_varying_retry_timers(
    apgdriver: db.Driver,
    N: int,
    retry_timer_short: timedelta = timedelta(seconds=0.200),
    retry_timer_long: timedelta = timedelta(seconds=0.400),
) -> None:
    e_short = asyncio.Event()
    e_long = asyncio.Event()
    c = QueueManager(apgdriver)
    seen_short = Counter[JobId]()
    seen_long = Counter[JobId]()

    @c.entrypoint("fetch_short", retry_timer=retry_timer_short)
    async def fetch_short(context: Job) -> None:
        seen_short[context.id] += 1
        await e_short.wait()

    @c.entrypoint("fetch_long", retry_timer=retry_timer_long)
    async def fetch_long(context: Job) -> None:
        seen_long[context.id] += 1
        await e_long.wait()

    await c.queries.enqueue(["fetch_short", "fetch_long"], [None, None], [0, 0])

    async def until_retry_updated() -> None:
        while True:
            if (
                seen_short
                and all(v > 1 for v in seen_short.values())
                and seen_long
                and all(v > 1 for v in seen_long.values())
            ):
                break
            await asyncio.sleep(0)

        c.shutdown.set()
        e_short.set()
        e_long.set()

    async with async_timeout.timeout(2):
        await asyncio.gather(
            c.run(dequeue_timeout=timedelta(seconds=0)),
            until_retry_updated(),
        )

    assert seen_short and all(v > 1 for v in seen_short.values())
    assert seen_long and all(v > 1 for v in seen_long.values())


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
        c.shutdown.set()
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
