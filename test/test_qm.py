import asyncio
import time
import uuid
from datetime import timedelta

import async_timeout
import pytest

from pgqueuer import db
from pgqueuer.models import Job, Log
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import QueueExecutionMode


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


@pytest.mark.parametrize("N", (1, 10, 100))
async def test_drain_mode(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(apgdriver)
    jobs = list[Job]()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        jobs.append(job)

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await qm.run(mode=QueueExecutionMode.drain)

    assert len(jobs) == N


@pytest.mark.parametrize("N", (1, 10, 100))
async def test_traceback_log(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        raise ValueError(f"Test error {job.id}")

    jids = await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await qm.run(mode=QueueExecutionMode.drain)

    logs = await q.queue_log()
    assert sum(log.status == "exception" for log in logs) == N
    assert sum(log.traceback is not None for log in logs if log.status == "exception") == N
    assert sum(log.job_id in jids and log.status == "exception" for log in logs) == N


@pytest.mark.parametrize("N", (100, 200))
@pytest.mark.parametrize("max_concurrent_tasks", (40, 80))
async def test_max_concurrent_tasks(
    apgdriver: db.Driver,
    N: int,
    max_concurrent_tasks: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(apgdriver)
    picked_jobs = list[Log]()

    async def log_sampler() -> None:
        await asyncio.sleep(0.25)
        logs = [log for log in await q.queue_log() if log.status == "picked"]
        qm.shutdown.set()
        picked_jobs.extend(logs)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        await qm.shutdown.wait()

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await asyncio.gather(
            qm.run(max_concurrent_tasks=max_concurrent_tasks),
            log_sampler(),
        )

    assert len(picked_jobs) == max_concurrent_tasks
