import asyncio
import time
from contextlib import suppress

import pytest
from PgQueuer import db
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager
from PgQueuer.queries import Queries


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
            c.alive.set()
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
                    qm.alive.set()
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
            time.sleep(2)  # Sim. heavy CPU/IO.
            if context.payload is None:
                for qm in qmpool:
                    qm.alive.set()
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
) -> None:
    q = Queries(pgdriver)
    qm = QueueManager(pgdriver)

    @qm.entrypoint("to_be_picked")
    async def to_be_picked(job: Job) -> None:
        if job.payload is None:
            qm.alive.set()

    N = 100
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

    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(qm.run(), timeout=2)

    assert (
        sum(s.count for s in await q.queue_size() if s.entrypoint == "to_be_picked")
        == 0
    )
    assert (
        sum(s.count for s in await q.queue_size() if s.entrypoint == "not_picked") == N
    )
