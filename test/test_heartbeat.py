import asyncio
from datetime import datetime, timedelta

import pytest

from pgqueuer.buffers import HeartbeatBuffer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.heartbeat import Heartbeat
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.types import JobId, QueueExecutionMode


@pytest.mark.parametrize(
    "interval",
    (
        timedelta(seconds=0.01),
        timedelta(seconds=0.05),
    ),
)
async def test_heartbeat_interval(interval: timedelta) -> None:
    callbacks = list[JobId]()

    async def callback(jids: list[JobId]) -> None:
        nonlocal callbacks
        callbacks.extend(jids)

    async with (
        HeartbeatBuffer(
            max_size=1_000,
            timeout=interval,
            callback=callback,
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=interval / 2,
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(interval.total_seconds())

    assert 2 <= len(callbacks) <= 3


@pytest.mark.parametrize("max_size", (10, 100))
async def test_heartbeat_max_size(max_size: int) -> None:
    callbacks = list[tuple[list[JobId], datetime]]()

    async def callback(jids: list[JobId]) -> None:
        nonlocal callbacks
        callbacks.append((jids, datetime.now()))

    async with (
        HeartbeatBuffer(
            max_size=max_size,
            timeout=timedelta(seconds=0.01),
            callback=callback,
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=timedelta(seconds=0.001),
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(timedelta(seconds=0.1).total_seconds())

    assert len(callbacks) >= 2


async def test_heartbeat_interval_qm_dispatch(apgdriver: AsyncpgDriver) -> None:
    retry_timer = timedelta(seconds=0.1)
    q = QueueManager(apgdriver)
    waiter = asyncio.Event()

    @q.entrypoint("endpoint", retry_timer=retry_timer)
    async def endpoint(job: Job) -> None:
        await waiter.wait()

    async def fetch_heartbeat(job_id: int) -> timedelta:
        row, *_ = await apgdriver.fetch(
            f"""
            SELECT
                NOW() - heartbeat AS dt
            FROM
                {q.queries.qbq.settings.queue_table}
            WHERE
                id = $1
            """,
            job_id,
        )
        return row["dt"]

    async def heartbeat_sampler(job_id: int) -> set[timedelta]:
        samples = set[timedelta]()
        while not waiter.is_set():
            samples.add(await fetch_heartbeat(job_id))
        return samples

    async def timer(deadline: timedelta) -> None:
        await asyncio.sleep(deadline.total_seconds())
        waiter.set()

    job_id, *_ = await q.queries.enqueue("endpoint", None)
    heartbeat_samples, *_ = await asyncio.gather(
        heartbeat_sampler(job_id),
        q.run(mode=QueueExecutionMode.drain),
        timer(retry_timer * 5),
    )

    assert all(x < retry_timer for x in heartbeat_samples)
