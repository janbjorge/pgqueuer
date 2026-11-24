"""A takeover of a dead worker's job counts as an attempt, bounded by ``max_attempts`` (#1116)."""

from __future__ import annotations

import asyncio
import sys
import textwrap
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.db import AsyncpgDriver
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


async def test_takeover_counts_as_an_attempt(queries: RepositoryPort) -> None:
    await queries.enqueue("ep", None)
    assert [(await crash(queries)).attempts for _ in range(3)] == [0, 1, 2]


async def test_repick_by_its_own_manager_is_not_an_attempt(queries: RepositoryPort) -> None:
    await queries.enqueue("ep", None)
    owner = QueueManagerId(uuid.uuid4())
    await stop_heartbeat(queries, (await pick(queries, owner)).id)
    assert (await pick(queries, owner)).attempts == 0


async def test_job_past_max_attempts_is_not_run(queries: RepositoryPort) -> None:
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


# The issue's script: each worker is killed by the job it picks.
POISON_WORKER = textwrap.dedent(
    """
    import asyncio, os, sys
    from datetime import timedelta

    import asyncpg

    from pgqueuer.db import AsyncpgDriver
    from pgqueuer.qm import QueueManager
    from pgqueuer.queries import Queries
    from pgqueuer.types import QueueExecutionMode


    async def worker() -> None:
        qm = QueueManager(Queries(AsyncpgDriver(await asyncpg.connect(sys.argv[1]))))

        @qm.entrypoint("poison", max_attempts=2)
        async def poison(job) -> None:
            os._exit(137)  # Stands in for an OOM kill or a segfault.

        await qm.run(dequeue_timeout=timedelta(hours=1), mode=QueueExecutionMode.drain)


    asyncio.run(worker())
    """
)


async def test_issue_reproduction(dsn: str, apgdriver: AsyncpgDriver) -> None:
    """Fresh pick and takeovers 1 and 2 crash; the fourth worker abandons the job."""
    q = Queries(apgdriver)
    (job_id,) = await q.enqueue("poison", None)

    exit_codes = []
    for _ in range(4):
        proc = await asyncio.create_subprocess_exec(sys.executable, "-c", POISON_WORKER, dsn)
        exit_codes.append(await proc.wait())
        await stop_heartbeat(q, job_id)

    assert exit_codes == [137, 137, 137, 0]
    assert await q.queue_size() == []
