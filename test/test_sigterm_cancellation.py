"""
Regression tests for GitHub issue #630.

When a dispatched job is interrupted by ``asyncio.CancelledError`` (e.g. SIGTERM
during a pod rollout), the row must transition out of ``'picked'`` with a
``'canceled'`` log entry so it does not block the queue's concurrency slot.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import timedelta

import anyio
import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.core import buffers
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain import models
from pgqueuer.ports.repository import EntrypointExecutionParameter

EP_UNLIMITED = EntrypointExecutionParameter(0)
EP_SERIAL = EntrypointExecutionParameter(1)


# ---------------------------------------------------------------------------
# Layer 1: contract — log_jobs('canceled') releases a picked row
# ---------------------------------------------------------------------------


async def test_log_canceled_releases_picked_row(queries: InMemoryQueries) -> None:
    """Logging a picked job with status='canceled' frees the slot."""
    await queries.enqueue("ep", b"x", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EP_UNLIMITED},
        qm_id,
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == 1
    job = jobs[0]

    await queries.log_jobs([(job, "canceled", None)])

    log = await queries.queue_log()
    assert [e.status for e in log if e.job_id == job.id and e.status == "canceled"] == ["canceled"]
    # The row is no longer pickable.
    again = await queries.dequeue(
        10,
        {"ep": EP_UNLIMITED},
        qm_id,
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert again == []


# ---------------------------------------------------------------------------
# Layer 2: regression — dispatcher CancelledError writes a 'canceled' log
# ---------------------------------------------------------------------------


async def test_dispatch_cancellation_error_logs_canceled(
    queries: InMemoryQueries,
) -> None:
    """Cancelling an in-flight dispatch must mark the row 'canceled', not leave it 'picked'."""
    qm = QueueManager(queries=queries)
    started = asyncio.Event()

    @qm.entrypoint("hang")
    async def handler(job: models.Job) -> None:
        started.set()
        await asyncio.sleep(3600)  # blocked until cancelled — simulates SIGTERM

    await queries.enqueue("hang", b"payload", priority=1)
    jobs = await queries.dequeue(
        10,
        {"hang": EP_UNLIMITED},
        qm.queue_manager_id,
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == 1
    job = jobs[0]
    qm.job_context[job.id] = models.Context(cancellation=anyio.CancelScope())

    async with (
        buffers.JobStatusLogBuffer(max_size=1, repository=queries) as jbuff,
        buffers.HeartbeatBuffer(
            max_size=10,
            timeout=timedelta(seconds=1),
            repository=queries,
        ) as hbuff,
    ):
        dispatch = asyncio.create_task(qm._dispatch(job, jbuff, hbuff, timedelta(seconds=30)))
        await asyncio.wait_for(started.wait(), timeout=2)

        dispatch.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dispatch

    # Bug condition: row stays at 'picked' forever.
    statuses = await queries.job_status([job.id])
    assert statuses == [] or all(st != "picked" for _, st in statuses), (
        f"Job stuck in 'picked' state after cancellation: {statuses}"
    )

    log = await queries.queue_log()
    canceled = [e for e in log if e.job_id == job.id and e.status == "canceled"]
    assert len(canceled) == 1, (
        f"Expected one 'canceled' log entry, got {[(e.job_id, e.status) for e in log]}"
    )


# ---------------------------------------------------------------------------
# Layer 3: deadlock — concurrency slot is freed after cancel
# ---------------------------------------------------------------------------


async def test_cancel_frees_concurrency_slot(queries: InMemoryQueries) -> None:
    """With concurrency_limit=1, a cancelled job must not block the next one."""
    qm = QueueManager(queries=queries)
    started = asyncio.Event()

    @qm.entrypoint("serial", concurrency_limit=1)
    async def handler(job: models.Job) -> None:
        started.set()
        await asyncio.sleep(3600)

    await queries.enqueue("serial", b"first", priority=1)
    jobs = await queries.dequeue(
        10,
        {"serial": EP_SERIAL},
        qm.queue_manager_id,
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    job = jobs[0]
    qm.job_context[job.id] = models.Context(cancellation=anyio.CancelScope())

    async with (
        buffers.JobStatusLogBuffer(max_size=1, repository=queries) as jbuff,
        buffers.HeartbeatBuffer(
            max_size=10,
            timeout=timedelta(seconds=1),
            repository=queries,
        ) as hbuff,
    ):
        dispatch = asyncio.create_task(qm._dispatch(job, jbuff, hbuff, timedelta(seconds=30)))
        await asyncio.wait_for(started.wait(), timeout=2)
        dispatch.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dispatch

    # Concurrency slot must now be free.
    await queries.enqueue("serial", b"second", priority=1)
    next_jobs = await queries.dequeue(
        10,
        {"serial": EP_SERIAL},
        qm.queue_manager_id,
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert [j.payload for j in next_jobs] == [b"second"], (
        f"Concurrency slot deadlocked — could not pick next job: {next_jobs}"
    )


# ---------------------------------------------------------------------------
# Layer 4: stale recovery must bypass the concurrency gate (GH #630, problem 2)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="GH #630 problem 2: stale recovery deadlocked by concurrency gate. "
    "Fix lands in follow-up PR.",
    strict=True,
)
async def test_stale_recovery_bypasses_concurrency_limit(
    queries: InMemoryQueries,
) -> None:
    """A new worker must recover stale 'picked' rows even when the slot is full.

    Reproduces the recovery deadlock from GH #630: with concurrency_limit=1,
    a leaked 'picked' row from a dead worker occupies the slot. The recovery
    path (stale-heartbeat re-pick) currently applies the same concurrency gate
    as new dequeues, so it sees ``picked >= limit`` and returns nothing —
    permanent deadlock with no self-healing.

    Re-picking just transfers ownership; live execution is unchanged.
    The gate should be dropped from the stale branch.
    """
    heartbeat_timeout = timedelta(milliseconds=20)

    # Worker A picks the job, then "dies" without logging — row stays 'picked'.
    qm_a = uuid.uuid4()
    await queries.enqueue("ep", b"x", priority=1)
    jobs_a = await queries.dequeue(
        10,
        {"ep": EP_SERIAL},
        qm_a,
        None,
        heartbeat_timeout=heartbeat_timeout,
    )
    assert len(jobs_a) == 1
    leaked = jobs_a[0]

    # Wait for the heartbeat to go stale.
    await asyncio.sleep(heartbeat_timeout.total_seconds() * 3)

    # Worker B arrives. The slot looks full (concurrency_limit=1, one row
    # at status='picked'), but the row is stale and must be recoverable.
    qm_b = uuid.uuid4()
    jobs_b = await queries.dequeue(
        10,
        {"ep": EP_SERIAL},
        qm_b,
        None,
        heartbeat_timeout=heartbeat_timeout,
    )
    assert [j.id for j in jobs_b] == [leaked.id], (
        f"Stale row not recovered — recovery path blocked by concurrency gate. Got: {jobs_b}"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
