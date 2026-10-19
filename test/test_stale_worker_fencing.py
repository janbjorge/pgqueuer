"""A worker whose job was re-picked after a heartbeat timeout must not resolve it (#956)."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import NamedTuple

import pytest

from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JOB_STATUS, QueueEntrypoint, QueueManagerId
from pgqueuer.ports.repository import EntrypointExecutionParameter


class Takeover(NamedTuple):
    stale: Job
    current: Job


async def dequeue_as(queries: Queries, queue_manager_id: QueueManagerId) -> list[Job]:
    return await queries.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


async def take_over(queries: Queries, apgdriver: AsyncpgDriver) -> Takeover:
    """Worker A picks the job, stalls past the heartbeat timeout, worker B re-picks it."""
    await queries.enqueue(["ep"], [None], [0])
    (stale,) = await dequeue_as(queries, QueueManagerId(uuid.uuid4()))

    # Backdating the heartbeat stands in for worker A stalling past heartbeat_timeout.
    await apgdriver.execute(
        f"""
        UPDATE {queries.qbq.qualified.queue_table}
        SET heartbeat = NOW() - interval '1 hour'
        WHERE id = $1
        """,
        stale.id,
    )
    (current,) = await dequeue_as(queries, QueueManagerId(uuid.uuid4()))
    assert current.id == stale.id
    assert current.queue_manager_id != stale.queue_manager_id
    return Takeover(stale=stale, current=current)


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


@pytest.mark.parametrize("status", ["successful", "exception", "failed"])
async def test_stale_worker_cannot_resolve_a_job_another_worker_holds(
    apgdriver: AsyncpgDriver,
    status: JOB_STATUS,
) -> None:
    """A's late completion leaves B's claim and logs nothing for A."""
    queries = Queries(apgdriver)
    takeover = await take_over(queries, apgdriver)

    await queries.log_jobs([(takeover.stale, status, None)])

    assert await queue_rows(queries, apgdriver, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert sorted(
        log.status for log in await queries.queue_log() if log.job_id == takeover.current.id
    ) == ["picked", "picked", "queued"]


async def test_stale_worker_cannot_requeue_a_job_another_worker_holds(
    apgdriver: AsyncpgDriver,
) -> None:
    """A's late retry leaves B's claim, so a third worker cannot pick the job."""
    queries = Queries(apgdriver)
    takeover = await take_over(queries, apgdriver)

    await queries.retry_job(takeover.stale, timedelta(0), None)

    assert await queue_rows(queries, apgdriver, takeover.current) == [
        {"status": "picked", "queue_manager_id": takeover.current.queue_manager_id}
    ]
    assert await dequeue_as(queries, QueueManagerId(uuid.uuid4())) == []
