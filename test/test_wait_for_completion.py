from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.models import JOB_STATUS, Job
from pgqueuer.qm import QueueManager
from pgqueuer.wait_for_completion import WaitForCompletion


async def test_wait_for_completion_successful(apgdriver: db.Driver) -> None:
    e = asyncio.Event()
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        await e.wait()

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def stop_after() -> None:
        await asyncio.sleep(1)
        e.set()
        await asyncio.sleep(1)
        qm.shutdown.set()

    async def wait_for_completion() -> list[asyncio.Future[JOB_STATUS]]:
        async with WaitForCompletion(apgdriver) as grp:
            return [grp.wait_for(jid) for jid in jids]

    waiters, *_ = await asyncio.gather(
        wait_for_completion(),
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        stop_after(),
    )

    assert len(waiters) == N
    assert all(w.result() == "successful" for w in waiters)


async def test_wait_for_completion_exception(apgdriver: db.Driver) -> None:
    e = asyncio.Event()
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        await e.wait()
        raise ValueError

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def stop_after() -> None:
        await asyncio.sleep(1)
        e.set()
        await asyncio.sleep(1)
        qm.shutdown.set()

    async def wait_for_completion() -> list[asyncio.Future[JOB_STATUS]]:
        async with WaitForCompletion(apgdriver) as grp:
            return [grp.wait_for(jid) for jid in jids]

    waiters, *_ = await asyncio.gather(
        wait_for_completion(),
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        stop_after(),
    )

    assert len(waiters) == N
    assert all(w.result() == "exception" for w in waiters)


async def test_wait_for_completion_canceled(apgdriver: db.Driver) -> None:
    e = asyncio.Event()
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        await e.wait()

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def stop_after() -> None:
        await asyncio.sleep(1)
        await qm.queries.mark_job_as_cancelled(jids)
        await asyncio.sleep(1)
        e.set()
        qm.shutdown.set()

    async def wait_for_completion() -> list[asyncio.Future[JOB_STATUS]]:
        async with WaitForCompletion(apgdriver) as grp:
            return [grp.wait_for(jid) for jid in jids]

    waiters, *_ = await asyncio.gather(
        wait_for_completion(),
        qm.run(dequeue_timeout=timedelta(seconds=0)),
        stop_after(),
    )

    assert len(waiters) == N
    assert all(w.result() == "canceled" for w in waiters)


async def test_wait_for_completion_deleted(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    @dataclass
    class FakeJob:
        id: int

    async def delete_after() -> None:
        await asyncio.sleep(0.1)
        await qm.queries.log_jobs(
            [
                (
                    FakeJob(jid),  # type: ignore
                    "deleted",
                    None,
                )
                for jid in jids
            ]
        )

    async def wait_for_completion() -> list[asyncio.Future[JOB_STATUS]]:
        async with WaitForCompletion(apgdriver) as grp:
            return [grp.wait_for(jid) for jid in jids]

    waiters, *_ = await asyncio.gather(
        wait_for_completion(),
        delete_after(),
    )

    assert len(waiters) == N
    assert all(w.result() == "deleted" for w in waiters)


@pytest.mark.parametrize("status", ("canceled", "deleted", "exception", "successful"))
async def test_wait_for_completion_is_terminal(apgdriver: db.Driver, status: str) -> None:
    assert WaitForCompletion(apgdriver)._is_terminal(status)  # type: ignore
