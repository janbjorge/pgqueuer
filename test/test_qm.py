import asyncio
import asyncio.selector_events
import time
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


@pytest.mark.parametrize("N", (1, 2, 32))
async def test_job_queing(
    pgdriver: db.Driver,
    N: int,
) -> None:
    c = QueueManager(pgdriver)
    seen = list[int]()

    @c.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        if context.payload is None:
            c.alive = False
            return
        assert context
        seen.append(int(context.payload))

    await c.queries.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    # Stop flag
    await c.queries.enqueue("fetch", None)

    await asyncio.wait_for(c.run(), timeout=1)
    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_job_fetch(
    pgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = Queries(pgdriver)
    qmpool = [QueueManager(pgdriver) for _ in range(concurrency)]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        async def fetch(context: Job) -> None:
            if context.payload is None:
                for qm in qmpool:
                    qm.alive = False
                return
            assert context
            seen.append(int(context.payload))

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    # Stop flag
    await q.enqueue("fetch", None)

    await asyncio.wait_for(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        timeout=10,
    )
    assert sorted(seen) == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_sync_entrypoint(
    pgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = Queries(pgdriver)
    qmpool = [QueueManager(pgdriver) for _ in range(concurrency)]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        def fetch(context: Job) -> None:
            time.sleep(1)  # Sim. heavy CPU/IO.
            if context.payload is None:
                for qm in qmpool:
                    qm.alive = False
                return
            assert context
            seen.append(int(context.payload))

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    # Stop flag
    await q.enqueue("fetch", None)

    await asyncio.wait_for(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        timeout=10,
    )
    assert sorted(seen) == list(range(N))


async def test_pick_local_entrypoints(
    pgdriver: db.Driver,
    N: int = 100,
) -> None:
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_picked")
    async def to_be_picked(job: Job) -> None:
        if job.payload is None:
            qm.alive = False

    await q.enqueue(
        ["to_be_picked"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await q.enqueue(
        ["not_picked"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def waiter() -> None:
        await asyncio.sleep(2)
        qm.alive = False

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "to_be_picked") == 0
    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "not_picked") == N


@pytest.mark.parametrize("N", (0, 1, 4, 32, 100))
async def test_cancellation(
    pgdriver: db.Driver,
    N: int,
) -> None:
    event = asyncio.Event()
    cancel_called_not_cancel_called = list[str]()
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job) -> None:
        scope = qm.get_cancel_scope(job.id)
        await event.wait()
        cancel_called_not_cancel_called.append(
            "cancel_called" if scope.cancel_called else "not_cancel_called"
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


@pytest.mark.parametrize("N", (0, 1, 4, 32, 100))
async def test_cancellation_ctx_mngr(
    pgdriver: db.Driver,
    N: int,
) -> None:
    event = asyncio.Event()
    cancel_called_not_cancel_called = list[str]()
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job) -> None:
        with qm.get_cancel_scope(job.id) as scope:
            await event.wait()
            cancel_called_not_cancel_called.append(
                "cancel_called" if scope.cancel_called else "not_cancel_called"
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

    assert cancel_called_not_cancel_called == []

    # Logged as canceled
    assert sum(x.count for x in await q.log_statistics(tail=1_000) if x.status == "canceled") == N
