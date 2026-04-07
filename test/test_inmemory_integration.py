"""Integration tests — InMemory adapter with real QueueManager/SchedulerManager."""

from __future__ import annotations

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
        {"perf_ep": EntrypointExecutionParameter(timedelta(0), 0)},
        qm_id,
        None,
    )
    dequeue_time = time.perf_counter() - t0

    assert len(dequeued) == n
    dequeue_rate = n / dequeue_time
    assert dequeue_rate > 500, f"Dequeue rate: {dequeue_rate:.0f} jobs/sec (target: >500)"
