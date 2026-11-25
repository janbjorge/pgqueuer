"""A takeover of a stale job adds one to ``attempts``, and ``max_attempts`` ends the job (#1116)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JobId, QueueEntrypoint, QueueExecutionMode, QueueManagerId
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


@pytest.fixture(params=["postgres", "inmemory"])
def queries(request: pytest.FixtureRequest) -> RepositoryPort:
    # Sync on purpose: getfixturevalue cannot set up an async fixture from a running loop.
    if request.param == "inmemory":
        return InMemoryQueries(driver=InMemoryDriver())
    return Queries(request.getfixturevalue("apgdriver"))


async def pick(q: RepositoryPort, manager: QueueManagerId) -> Job:
    (job,) = await q.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=manager,
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    return job


async def stop_heartbeat(q: RepositoryPort, job_id: JobId) -> None:
    """Leave the row as a dead worker does: picked, with a heartbeat past any timeout."""
    if isinstance(q, InMemoryQueries):
        q.jobs[int(job_id)].heartbeat = datetime(2000, 1, 1, tzinfo=UTC)
        return
    assert isinstance(q, Queries)
    await q.driver.execute(
        f"UPDATE {q.qbq.qualified.queue_table} SET heartbeat = NOW() - interval '1 hour'"
        " WHERE id = $1",
        job_id,
    )


async def crash(q: RepositoryPort) -> Job:
    job = await pick(q, QueueManagerId(uuid.uuid4()))
    await stop_heartbeat(q, job.id)
    return job


async def test_takeover_adds_one_to_attempts(queries: RepositoryPort) -> None:
    await queries.enqueue("ep", None)
    assert [(await crash(queries)).attempts for _ in range(3)] == [0, 1, 2]


async def test_max_attempts_ends_the_job(queries: RepositoryPort) -> None:
    await queries.enqueue("ep", None)
    await crash(queries)
    await crash(queries)
    qm = QueueManager(queries)
    ran: list[Job] = []

    @qm.entrypoint("ep", max_attempts=1, on_failure="hold")
    async def ep(job: Job) -> None:
        ran.append(job)

    await qm.run(dequeue_timeout=timedelta(hours=1), mode=QueueExecutionMode.drain)

    assert ran == []
    (log,) = [x for x in await queries.queue_log() if x.status == "failed"]
    assert log.traceback is not None
    assert "abandoned after 2 attempts" in log.traceback.exception_message
