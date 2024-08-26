import asyncio
import threading
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.models import Context, Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


@pytest.mark.parametrize("N", (1, 4, 32, 100))
async def test_cancellation_async(
    pgdriver: db.Driver,
    N: int,
) -> None:
    event = asyncio.Event()
    cancel_called_not_cancel_called = list[str]()
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job, ctx: Context) -> None:
        await event.wait()
        cancel_called_not_cancel_called.append(
            "cancel_called" if ctx.cancellation.is_set() else "not_cancel_called"
        )

    job_ids = await q.enqueue(
        ["to_be_canceled"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.status == "picked") < N:
            await asyncio.sleep(0.01)

        await q.mark_job_as_cancelled(job_ids)
        event.set()

        qm.alive = False

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert cancel_called_not_cancel_called == ["cancel_called"] * N

    # Logged as canceled
    assert sum(x.count for x in await q.log_statistics(tail=1_000) if x.status == "canceled") == N


@pytest.mark.parametrize("N", (1, 4, 32, 100))
async def test_cancellation_sync(
    pgdriver: db.Driver,
    N: int,
) -> None:
    event = threading.Event()
    cancel_called_not_cancel_called = list[str]()
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_canceled")
    def to_be_canceled(job: Job, ctx: Context) -> None:
        nonlocal event
        event.wait()
        cancel_called_not_cancel_called.append(
            "cancel_called" if ctx.cancellation.is_set() else "not_cancel_called"
        )

    job_ids = await q.enqueue(
        ["to_be_canceled"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.status == "picked") < N:
            await asyncio.sleep(0)

        await q.mark_job_as_cancelled(job_ids)
        event.set()

        qm.alive = False

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert cancel_called_not_cancel_called == ["cancel_called"] * N

    # Logged as canceled
    assert sum(x.count for x in await q.log_statistics(tail=1_000) if x.status == "canceled") == N
