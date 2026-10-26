from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from datetime import timedelta
from itertools import chain

import pytest

from pgqueuer.db import Driver
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import EntrypointExecutionParameter, Queries


async def raises(lock: asyncio.Lock) -> None:
    if lock.locked():
        raise ValueError("Locked")


async def consumer(
    qm: QueueManager,
    lock: asyncio.Lock,
) -> defaultdict[str, list[Job]]:
    consumed = defaultdict[str, list[Job]](list)

    @qm.entrypoint("serialized_dispatch_true", concurrency_limit=1)
    async def serialized_dispatch_true(job: Job) -> None:
        consumed["serialized_dispatch_true"].append(job)
        await asyncio.sleep(0.1)
        await raises(lock)

    @qm.entrypoint("serialized_dispatch_false")
    async def fetch(job: Job) -> None:
        consumed["serialized_dispatch_false"].append(job)
        await asyncio.sleep(0.1)

    await qm.run(dequeue_timeout=timedelta(seconds=0))
    return consumed


async def enqueue(
    queries: Queries,
    size: int,
) -> None:
    assert size > 0
    await queries.enqueue(
        ["serialized_dispatch_true"] * size,
        [f"{n}".encode() for n in range(size)],
        [0] * size,
    )
    await queries.enqueue(
        ["serialized_dispatch_false"] * size,
        [f"{n}".encode() for n in range(size)],
        [0] * size,
    )


async def locked_consumer(
    qm: QueueManager,
) -> defaultdict[str, list[Job]]:
    consumed = defaultdict[str, list[Job]](list)

    @qm.entrypoint("serialized_dispatch_true", concurrency_limit=1)
    async def serialized_dispatch_true(job: Job) -> None:
        consumed["serialized_dispatch_true"].append(job)
        await asyncio.sleep(0.1)
        raise ValueError("Locked")  # Always raise to simulate locked state

    await qm.run(dequeue_timeout=timedelta(seconds=0))
    return consumed


@pytest.mark.parametrize("n_consumers", (1, 2, 4, 32))
async def test_serialized_dispatch(
    n_consumers: int,
    apgdriver: Driver,
) -> None:
    n_tasks = 500
    wait = 1
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def dequeue() -> None:
        consumers = [consumer(q, lock) for q in qms]
        await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    await asyncio.gather(timer(), dequeue())


@pytest.mark.parametrize("n_consumers", (2, 4))
async def test_serialized_dispatch_mixed(
    n_consumers: int,
    apgdriver: Driver,
) -> None:
    n_tasks = 10_000
    wait = 1
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def dequeue() -> list[defaultdict[str, list[Job]]]:
        consumers = [consumer(q, lock) for q in qms]
        return await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    _, consumed = await asyncio.gather(timer(), dequeue())
    serialized_dispatch_true = list(
        chain.from_iterable(x["serialized_dispatch_true"] for x in consumed)
    )
    serialized_dispatch_false = list(
        chain.from_iterable(x["serialized_dispatch_false"] for x in consumed)
    )

    # Add debug information to monitor job counts
    assert len(serialized_dispatch_true) > 0, "No jobs processed for serialized_dispatch_true"
    assert len(serialized_dispatch_false) > 0, "No jobs processed for serialized_dispatch_false"


@pytest.mark.parametrize("n_consumers", (1, 2, 4))
async def test_no_jobs_processed_when_locked(
    n_consumers: int,
    apgdriver: Driver,
) -> None:
    n_tasks = 100
    wait = 3
    # Set one job to picked state to simulate an ongoing process
    queries = Queries(apgdriver)
    await enqueue(queries, size=n_tasks)
    picked_job = await queries.dequeue(
        1,
        {QueueEntrypoint("serialized_dispatch_true"): EntrypointExecutionParameter(1)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(picked_job) == 1, "Failed to pick a job for locking"

    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def dequeue() -> list[defaultdict[str, list[Job]]]:
        consumers = [locked_consumer(q) for q in qms]
        return await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    _, consumed = await asyncio.gather(timer(), dequeue())
    serialized_dispatch_true = list(
        chain.from_iterable(x["serialized_dispatch_true"] for x in consumed)
    )
    assert len(serialized_dispatch_true) == 0, (
        "Jobs should not have been processed while lock is acquired"
    )


@pytest.mark.parametrize("n_consumers", (1, 2, 4))
async def test_mixed_serialized_and_concurrent_processing(
    n_consumers: int,
    apgdriver: Driver,
) -> None:
    n_tasks = 1000
    wait = 1
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def dequeue() -> list[defaultdict[str, list[Job]]]:
        consumers = [consumer(q, lock) for q in qms]
        return await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    _, consumed = await asyncio.gather(timer(), dequeue())
    serialized_dispatch_true = list(
        chain.from_iterable(x["serialized_dispatch_true"] for x in consumed)
    )
    serialized_dispatch_false = list(
        chain.from_iterable(x["serialized_dispatch_false"] for x in consumed)
    )
    # Ensure some jobs were processed with serialization and some concurrently
    assert len(serialized_dispatch_true) > 0
    assert len(serialized_dispatch_false) > 0


async def test_single_consumer_serialized_behavior(
    apgdriver: Driver,
) -> None:
    n_tasks = 50
    wait = 10
    n_consumers = 1
    lock = asyncio.Lock()
    await enqueue(Queries(apgdriver), size=n_tasks)
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def dequeue() -> list[defaultdict[str, list[Job]]]:
        consumers = [consumer(q, lock) for q in qms]
        return await asyncio.gather(*consumers)

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    _, consumed = await asyncio.gather(timer(), dequeue())
    serialized_dispatch_true = list(
        chain.from_iterable(x["serialized_dispatch_true"] for x in consumed)
    )
    # Verify that all jobs were processed sequentially by a single consumer
    assert len(serialized_dispatch_true) == n_tasks
