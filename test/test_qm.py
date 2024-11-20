import asyncio
import time
import uuid
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


async def wait_until_empty_queue(
    q: Queries,
    qms: list[QueueManager],
) -> None:
    while sum(x.count for x in await q.queue_size()) > 0:
        await asyncio.sleep(0.01)

    for qm in qms:
        qm.shutdown.set()


@pytest.mark.parametrize("N", (1, 2, 32))
async def test_job_queuing(
    apgdriver: db.Driver,
    N: int,
) -> None:
    c = QueueManager(apgdriver)
    seen = list[int]()

    @c.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        assert context.payload is not None
        seen.append(int(context.payload))

    await c.queries.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        c.run(),
        wait_until_empty_queue(c.queries, [c]),
    )

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_job_fetch(
    apgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = Queries(apgdriver)
    qmpool = [QueueManager(apgdriver) for _ in range(concurrency)]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        async def fetch(context: Job) -> None:
            assert context.payload is not None
            seen.append(int(context.payload))

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        wait_until_empty_queue(q, qmpool),
    )

    assert sorted(seen) == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_sync_entrypoint(
    apgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = Queries(apgdriver)
    qmpool = [QueueManager(apgdriver) for _ in range(concurrency)]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        def fetch(context: Job) -> None:
            time.sleep(1)  # Sim. heavy CPU/IO.
            assert context.payload is not None
            seen.append(int(context.payload))

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        wait_until_empty_queue(q, qmpool),
    )
    assert sorted(seen) == list(range(N))


async def test_pick_local_entrypoints(
    apgdriver: db.Driver,
    N: int = 100,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(apgdriver)
    pikced_by = list[str]()

    @qm.entrypoint("to_be_picked")
    async def to_be_picked(job: Job) -> None:
        pikced_by.append(job.entrypoint)

    await q.enqueue(["to_be_picked"] * N, [None] * N, [0] * N)
    await q.enqueue(["not_picked"] * N, [None] * N, [0] * N)

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.entrypoint == "to_be_picked"):
            await asyncio.sleep(0.01)
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert pikced_by == ["to_be_picked"] * N
    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "to_be_picked") == 0
    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "not_picked") == N


async def test_pick_set_queue_manager_id(
    apgdriver: db.Driver,
    N: int = 100,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(apgdriver)
    qmids = set[uuid.UUID]()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        assert job.queue_manager_id is not None
        qmids.add(job.queue_manager_id)

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size()):
            await asyncio.sleep(0.01)
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert len(qmids) == 1
