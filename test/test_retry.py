import asyncio
import asyncio.selector_events
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qb import DBSettings
from pgqueuer.qm import QueueManager
from pgqueuer.types import JobId

# Buffer timing is challenging due to built-in jitter, which helps avoid too
# much concurrent writing.


async def inspect_queued_jobs(jids: list[JobId], driver: db.Driver) -> list[Job]:
    sql = f"""SELECT * FROM {DBSettings().queue_table} WHERE id = ANY($1::integer[])"""
    return [Job.model_validate(x) for x in await driver.fetch(sql, jids)]


async def test_retry_after_timer_expired(apgdriver: db.Driver) -> None:
    N = 25
    retry_timer = timedelta(seconds=0.250)

    e = asyncio.Event()
    qm = QueueManager(apgdriver)
    calls = Counter[JobId]()

    @qm.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        calls[context.id] += 1
        await e.wait()

    await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def stop_after() -> None:
        # Wait for all jobs to be dequeued
        while len(calls) <= N and all(v <= 2 for v in calls.values()):
            await asyncio.sleep(0.001)

        e.set()
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        stop_after(),
    )

    assert len(calls) == N
    assert all(v > 1 for v in calls.values())


async def test_no_retry_on_zero_timer(apgdriver: db.Driver) -> None:
    N = 10
    retry_timer: timedelta = timedelta(seconds=0)
    event = asyncio.Event()
    qm = QueueManager(apgdriver)
    heartbeat = defaultdict[JobId, list[datetime]](list)

    async def fetch_db_heartbeat(jobid: JobId) -> datetime:
        rows = await inspect_queued_jobs([jobid], apgdriver)
        assert len(rows) == 1
        return rows[0].heartbeat

    @qm.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        heartbeat[context.id].append(await fetch_db_heartbeat(context.id))
        await event.wait()
        heartbeat[context.id].append(await fetch_db_heartbeat(context.id))

    await qm.queries.enqueue(
        ["fetch"] * N,
        [None] * N,
        [0] * N,
    )

    async def until_retry_updated() -> None:
        while not heartbeat:
            await asyncio.sleep(0)
        event.set()
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        until_retry_updated(),
    )

    assert len(heartbeat) == N
    for k, v in heartbeat.items():
        assert len(v) == 2
        assert v[0] == v[1]


async def test_heartbeat_no_updates(apgdriver: db.Driver) -> None:
    retry_timer = timedelta(seconds=0.100)
    event = asyncio.Event()
    qm = QueueManager(apgdriver)
    heartbeats = defaultdict[JobId, list[datetime]](list)

    async def fetch_db_heartbeat(jobid: JobId) -> datetime:
        rows = await inspect_queued_jobs([jobid], apgdriver)
        assert len(rows) == 1
        return rows[0].heartbeat

    @qm.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        heartbeats[context.id].append(await fetch_db_heartbeat(context.id))
        await event.wait()
        heartbeats[context.id].append(await fetch_db_heartbeat(context.id))

    await qm.queries.enqueue(["fetch"], [None], [0])

    async def entrypoint_waiter() -> None:
        while not heartbeats:
            await asyncio.sleep(0)
        event.set()
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        entrypoint_waiter(),
    )

    assert len(heartbeats) == 1
    for k, v in heartbeats.items():
        assert len(v) == 2
        assert v[1] == v[0]


async def test_varying_retry_timers(apgdriver: db.Driver) -> None:
    retry_timer_short = timedelta(seconds=0.200)
    e_short = asyncio.Event()

    retry_timer_long = timedelta(seconds=0.400)
    e_long = asyncio.Event()

    c = QueueManager(apgdriver)
    calls = Counter[JobId]()

    @c.entrypoint("fetch_short", retry_timer=retry_timer_short)
    async def fetch_short(context: Job) -> None:
        calls[context.id] += 1
        await e_short.wait()

    @c.entrypoint("fetch_long", retry_timer=retry_timer_long)
    async def fetch_long(context: Job) -> None:
        calls[context.id] += 1
        await e_long.wait()

    await c.queries.enqueue(
        ["fetch_short", "fetch_long"],
        [None, None],
        [0, 0],
    )

    async def entrypoint_waiter() -> None:
        # Wait for all jobs to be dequeued
        # while sum(calls.values()) < 4:
        while not (all(v > 1 for v in calls.values()) and len(calls) > 1):
            await asyncio.sleep(0)
        # await asyncio.sleep(retry_timer_long.total_seconds())
        # await asyncio.sleep(retry_timer_short.total_seconds() * 2)
        e_short.set()
        e_long.set()
        c.shutdown.set()

    await asyncio.gather(
        c.run(dequeue_timeout=timedelta(seconds=0)),
        entrypoint_waiter(),
    )

    print(calls)
    assert len(calls) == 2
    assert all(v > 1 for v in calls.values())


async def test_retry_with_cancellation(apgdriver: db.Driver) -> None:
    N = 4
    retry_timer = timedelta(seconds=0.100)
    event = asyncio.Event()
    qm = QueueManager(apgdriver)
    calls = Counter[JobId]()

    @qm.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        calls[context.id] += 1
        if calls[context.id] < N:
            raise asyncio.CancelledError("Simulated cancellation")
        await event.wait()

    await qm.queries.enqueue(["fetch"], [None], [0])

    async def entrypoint_waiter() -> None:
        while sum(v for v in calls.values()) < N:
            await asyncio.sleep(0.001)
        event.set()
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        entrypoint_waiter(),
    )

    assert len(calls) == 1
    assert sum(v for v in calls.values()) == N


async def test_heartbeat_db_datetime(apgdriver: db.Driver) -> None:
    retry_timer = timedelta(seconds=0.150)
    event = asyncio.Event()
    qm = QueueManager(apgdriver)

    async def fetch_db_heartbeat(jobid: JobId) -> timedelta:
        sql = f"""SELECT NOW() - heartbeat AS dt FROM {DBSettings().queue_table} WHERE id = ANY($1::integer[])"""  # noqa: E501
        rows = await apgdriver.fetch(sql, [jobid])
        assert len(rows) == 1
        return rows[0]["dt"]

    @qm.entrypoint("fetch", retry_timer=retry_timer)
    async def fetch(context: Job) -> None:
        await event.wait()

    jid, *_ = await qm.queries.enqueue(["fetch"], [None], [0])

    async def entrypoint_waiter() -> None:
        await asyncio.sleep(retry_timer.total_seconds() * 2)
        event.set()
        qm.shutdown.set()

    async def poller() -> list[timedelta]:
        samples = list[timedelta]()
        while not event.is_set():
            samples.append(await fetch_db_heartbeat(jid))
        return samples

    samples, *_ = await asyncio.gather(
        poller(),
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        entrypoint_waiter(),
    )

    leeway = retry_timer / 10
    for sample in samples:
        assert sample - leeway < retry_timer, (sample, retry_timer, sample - retry_timer)
