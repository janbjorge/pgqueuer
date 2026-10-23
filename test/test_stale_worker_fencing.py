"""A worker whose job was re-picked after a heartbeat timeout must not resolve it (#956)."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JOB_STATUS, QueueEntrypoint, QueueManagerId
from pgqueuer.ports.repository import EntrypointExecutionParameter, QueueRepositoryPort

Stall = Callable[[Job], Awaitable[None]]


class Takeover(NamedTuple):
    stale: Job
    current: Job


async def dequeue_as(queries: QueueRepositoryPort, queue_manager_id: QueueManagerId) -> list[Job]:
    return await queries.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


async def take_over(queries: QueueRepositoryPort, stall: Stall) -> Takeover:
    """Worker A picks the job, stalls past the heartbeat timeout, worker B re-picks it."""
    await queries.enqueue(["ep"], [None], [0])
    (stale,) = await dequeue_as(queries, QueueManagerId(uuid.uuid4()))
    await stall(stale)
    (current,) = await dequeue_as(queries, QueueManagerId(uuid.uuid4()))
    assert current.id == stale.id
    assert current.queue_manager_id != stale.queue_manager_id
    return Takeover(stale=stale, current=current)


async def take_over_in_postgres(queries: Queries, apgdriver: AsyncpgDriver) -> Takeover:
    async def stall(job: Job) -> None:
        # Backdating the heartbeat stands in for worker A stalling past heartbeat_timeout.
        await apgdriver.execute(
            f"""
            UPDATE {queries.qbq.qualified.queue_table}
            SET heartbeat = NOW() - interval '1 hour'
            WHERE id = $1
            """,
            job.id,
        )

    return await take_over(queries, stall)


async def take_over_in_memory(queries: InMemoryQueries) -> Takeover:
    async def stall(job: Job) -> None:
        queries.jobs[int(job.id)].heartbeat = datetime(2000, 1, 1, tzinfo=UTC)

    return await take_over(queries, stall)


async def queue_rows(
    queries: Queries, apgdriver: AsyncpgDriver, job: Job
) -> list[dict[str, object]]:
    return await apgdriver.fetch(
        f"""
        SELECT status, queue_manager_id
        FROM {queries.qbq.qualified.queue_table}
        WHERE id = $1
        """,
        job.id,
    )


def in_memory_rows(queries: InMemoryQueries, job: Job) -> list[dict[str, object]]:
    row = queries.jobs.get(int(job.id))
    if row is None:
        return []
    return [{"status": row.status, "queue_manager_id": row.queue_manager_id}]


async def log_statuses(queries: QueueRepositoryPort, job: Job) -> list[str]:
    return sorted(log.status for log in await queries.queue_log() if log.job_id == job.id)


@pytest.mark.parametrize("status", ["successful", "exception", "failed"])
async def test_stale_worker_cannot_resolve_a_job_another_worker_holds(
    apgdriver: AsyncpgDriver,
    status: JOB_STATUS,
) -> None:
    """A's late completion leaves B's claim and logs nothing for A."""
    queries = Queries(apgdriver)
    takeover = await take_over_in_postgres(queries, apgdriver)

    await queries.log_jobs([(takeover.stale, status, None)])

    assert await queue_rows(queries, apgdriver, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert await log_statuses(queries, takeover.current) == ["picked", "picked", "queued"]


async def test_stale_worker_cannot_requeue_a_job_another_worker_holds(
    apgdriver: AsyncpgDriver,
) -> None:
    """A's late retry leaves B's claim, so a third worker cannot pick the job."""
    queries = Queries(apgdriver)
    takeover = await take_over_in_postgres(queries, apgdriver)

    await queries.retry_job(takeover.stale, timedelta(0), None)

    assert await queue_rows(queries, apgdriver, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert await dequeue_as(queries, QueueManagerId(uuid.uuid4())) == []


@pytest.mark.parametrize("status", ["successful", "exception", "failed"])
async def test_in_memory_stale_worker_cannot_resolve_a_job_another_worker_holds(
    queries: InMemoryQueries,
    status: JOB_STATUS,
) -> None:
    """The in-memory adapter applies the same owner check as Postgres."""
    takeover = await take_over_in_memory(queries)

    await queries.log_jobs([(takeover.stale, status, None)])

    assert in_memory_rows(queries, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert await log_statuses(queries, takeover.current) == ["picked", "picked", "queued"]


async def test_in_memory_stale_worker_cannot_requeue_a_job_another_worker_holds(
    queries: InMemoryQueries,
) -> None:
    """The in-memory adapter applies the same owner check as Postgres."""
    takeover = await take_over_in_memory(queries)

    await queries.retry_job(takeover.stale, timedelta(0), None)

    assert in_memory_rows(queries, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert await dequeue_as(queries, QueueManagerId(uuid.uuid4())) == []
