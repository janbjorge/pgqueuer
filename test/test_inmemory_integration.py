"""Integration tests — InMemory adapter with real QueueManager/SchedulerManager."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import QueueExecutionMode


# ---------------------------------------------------------------------------
# PgQueuer.in_memory() factory
# ---------------------------------------------------------------------------


def test_in_memory_factory_creates_instance() -> None:
    pq = PgQueuer.in_memory()
    assert isinstance(pq, PgQueuer)
    assert isinstance(pq.qm.queries, InMemoryQueries)
    assert isinstance(pq.sm.queries, InMemoryQueries)
    assert isinstance(pq.connection, InMemoryDriver)


# ---------------------------------------------------------------------------
# QueueManager drain mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qm_drain_processes_all_jobs() -> None:
    pq = PgQueuer.in_memory()
    processed: list[int] = []

    @pq.entrypoint("drain_ep")
    async def handler(job: Job) -> None:
        processed.append(int(job.id))

    n = 50
    await pq.qm.queries.enqueue(
        ["drain_ep"] * n,
        [b"x"] * n,
        [0] * n,
    )

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    assert len(processed) == n


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qm_priority_ordering() -> None:
    pq = PgQueuer.in_memory()
    order: list[int] = []

    @pq.entrypoint("prio_ep")
    async def handler(job: Job) -> None:
        order.append(job.priority)

    await pq.qm.queries.enqueue(
        ["prio_ep", "prio_ep", "prio_ep"],
        [None, None, None],
        [1, 10, 5],
    )

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    assert order == sorted(order, reverse=True)


# ---------------------------------------------------------------------------
# Concurrency limit respected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qm_concurrency_limit() -> None:
    pq = PgQueuer.in_memory()
    max_concurrent = 0
    current = 0
    lock = asyncio.Lock()

    @pq.entrypoint("conc_ep", concurrency_limit=2)
    async def handler(job: Job) -> None:
        nonlocal current, max_concurrent
        async with lock:
            current += 1
            if current > max_concurrent:
                max_concurrent = current
        await asyncio.sleep(0.01)
        async with lock:
            current -= 1

    n = 10
    await pq.qm.queries.enqueue(
        ["conc_ep"] * n,
        [None] * n,
        [0] * n,
    )

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    assert max_concurrent <= 2


# ---------------------------------------------------------------------------
# Multiple entrypoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qm_multiple_entrypoints() -> None:
    pq = PgQueuer.in_memory()
    processed_a: list[int] = []
    processed_b: list[int] = []

    @pq.entrypoint("ep_a")
    async def handler_a(job: Job) -> None:
        processed_a.append(int(job.id))

    @pq.entrypoint("ep_b")
    async def handler_b(job: Job) -> None:
        processed_b.append(int(job.id))

    await pq.qm.queries.enqueue(
        ["ep_a", "ep_b", "ep_a", "ep_b"],
        [None, None, None, None],
        [0, 0, 0, 0],
    )

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    assert len(processed_a) == 2
    assert len(processed_b) == 2


# ---------------------------------------------------------------------------
# Exception handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qm_exception_handling() -> None:
    pq = PgQueuer.in_memory()

    @pq.entrypoint("fail_ep")
    async def handler(job: Job) -> None:
        raise RuntimeError("boom")

    await pq.qm.queries.enqueue("fail_ep", None)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
    )

    # Job should be logged as 'exception'
    log = await pq.qm.queries.queue_log()
    statuses = [e.status for e in log if e.entrypoint == "fail_ep"]
    assert "exception" in statuses


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_performance_enqueue_dequeue() -> None:
    """10k enqueue + dequeue cycle should complete quickly."""
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver=driver)

    n = 10_000
    entrypoints = ["perf_ep"] * n
    payloads: list[bytes | None] = [None] * n
    priorities = [0] * n

    t0 = time.perf_counter()
    ids = await queries.enqueue(entrypoints, payloads, priorities)
    enqueue_time = time.perf_counter() - t0

    assert len(ids) == n
    enqueue_rate = n / enqueue_time
    # Target: >10k jobs/sec
    assert enqueue_rate > 5_000, f"Enqueue rate: {enqueue_rate:.0f} jobs/sec (target: >5000)"

    from pgqueuer.ports.repository import EntrypointExecutionParameter

    qm_id = uuid.uuid4()
    t0 = time.perf_counter()
    dequeued = await queries.dequeue(
        n,
        {"perf_ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    dequeue_time = time.perf_counter() - t0

    assert len(dequeued) == n
    dequeue_rate = n / dequeue_time
    assert dequeue_rate > 500, f"Dequeue rate: {dequeue_rate:.0f} jobs/sec (target: >500)"
