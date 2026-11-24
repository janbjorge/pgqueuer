"""A job whose worker dies mid-run is bounded by ``max_attempts`` (#1116).

Taking over a dead worker's job counts as an attempt, next to retries. A dead worker
leaves its row ``picked`` with a heartbeat that stops moving. ``crash`` reproduces
that state without a process: dequeue under a fresh manager id, then backdate the
heartbeat past any timeout. Every wait here is an event or drain mode; GUARD fires
only on a stall.
"""

from __future__ import annotations

import asyncio
import sys
import textwrap
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import Job, Log
from pgqueuer.domain.types import (
    JobId,
    OnFailure,
    QueueEntrypoint,
    QueueExecutionMode,
    QueueManagerId,
)
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

HOUR = timedelta(hours=1)
# Below pytest's 20 s per-test timeout, so a stall reports as a TimeoutError here.
GUARD = 15


@dataclass
class Harness:
    q: RepositoryPort
    stall: Callable[[JobId], Awaitable[None]]

    async def dequeue_as(self, queue_manager_id: QueueManagerId) -> list[Job]:
        return await self.q.dequeue(
            batch_size=1,
            entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(concurrency_limit=0)},
            queue_manager_id=queue_manager_id,
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )

    async def crash(self) -> Job:
        """A worker picks the job and dies: its heartbeat stops and the row stays picked."""
        (job,) = await self.dequeue_as(QueueManagerId(uuid.uuid4()))
        await self.stall(job.id)
        return job


@pytest.fixture(params=["postgres", "inmemory"])
def harness(request: pytest.FixtureRequest) -> Harness:
    # Sync on purpose: getfixturevalue cannot set up an async fixture from a running loop.
    if request.param == "inmemory":
        queries = InMemoryQueries(driver=InMemoryDriver())

        async def stall_in_memory(job_id: JobId) -> None:
            queries.jobs[int(job_id)].heartbeat = datetime(2000, 1, 1, tzinfo=UTC)

        return Harness(queries, stall_in_memory)

    apgdriver: AsyncpgDriver = request.getfixturevalue("apgdriver")
    pg = Queries(apgdriver)

    async def stall_in_postgres(job_id: JobId) -> None:
        await apgdriver.execute(
            f"""
            UPDATE {pg.qbq.qualified.queue_table}
            SET heartbeat = NOW() - interval '1 hour'
            WHERE id = $1
            """,
            job_id,
        )

    return Harness(pg, stall_in_postgres)


async def log_for(q: RepositoryPort, job_id: JobId, status: str) -> Log:
    (log,) = [x for x in await q.queue_log() if x.job_id == job_id and x.status == status]
    return log


# Repository: a takeover counts as an attempt.


async def test_fresh_pick_has_no_attempts(harness: Harness) -> None:
    await harness.q.enqueue("ep", None)
    (job,) = await harness.dequeue_as(QueueManagerId(uuid.uuid4()))
    assert job.attempts == 0


async def test_each_takeover_counts_one_attempt(harness: Harness) -> None:
    await harness.q.enqueue("ep", None)
    seen = [await harness.crash() for _ in range(4)]
    assert [job.attempts for job in seen] == [0, 1, 2, 3]


async def test_repick_by_its_own_manager_is_not_an_attempt(harness: Harness) -> None:
    """A late heartbeat lets a live worker re-pick its own running job; that is no takeover."""
    await harness.q.enqueue("ep", None)
    owner = QueueManagerId(uuid.uuid4())
    (first,) = await harness.dequeue_as(owner)
    await harness.stall(first.id)

    (again,) = await harness.dequeue_as(owner)

    assert again.id == first.id
    assert again.attempts == 0


async def test_retry_after_takeover_counts_both(harness: Harness) -> None:
    await harness.q.enqueue("ep", None)
    await harness.crash()
    (owner,) = await harness.dequeue_as(QueueManagerId(uuid.uuid4()))
    assert owner.attempts == 1

    await harness.q.retry_job(owner, timedelta(0), None)
    (retried,) = await harness.dequeue_as(QueueManagerId(uuid.uuid4()))

    assert retried.attempts == 2


async def test_requeue_of_held_job_resets_attempts(harness: Harness) -> None:
    """An operator requeue is a fresh start, or a job held for its takeovers is abandoned unrun."""
    await harness.q.enqueue("ep", None)
    await harness.crash()
    (owner,) = await harness.dequeue_as(QueueManagerId(uuid.uuid4()))
    await harness.q.log_jobs([(owner, "failed", None)])

    await harness.q.requeue_jobs([owner.id])
    (requeued,) = await harness.dequeue_as(QueueManagerId(uuid.uuid4()))

    assert requeued.attempts == 0


# QueueManager: the per-entrypoint bound.


async def drain(q: RepositoryPort, max_attempts: int | None, on_failure: OnFailure) -> list[Job]:
    """Run one worker in drain mode and return the jobs its handler was called with."""
    qm = QueueManager(q)
    ran: list[Job] = []

    @qm.entrypoint("ep", max_attempts=max_attempts, on_failure=on_failure)
    async def ep(job: Job) -> None:
        ran.append(job)

    async with asyncio.timeout(GUARD):
        await qm.run(dequeue_timeout=HOUR, mode=QueueExecutionMode.drain)
    return ran


@pytest.mark.parametrize(
    ("crashes", "max_attempts"),
    [(0, 0), (1, 1), (2, 2), (3, None)],
    ids=["fresh-limit-0", "at-limit-1", "at-limit-2", "unlimited"],
)
async def test_job_within_its_limit_runs(
    harness: Harness, crashes: int, max_attempts: int | None
) -> None:
    """The handler runs, and sees how many takeovers led to this run."""
    (job_id,) = await harness.q.enqueue("ep", None)
    for _ in range(crashes):
        await harness.crash()

    ran = await drain(harness.q, max_attempts, "delete")

    assert [(job.id, job.attempts) for job in ran] == [(job_id, crashes)]
    await log_for(harness.q, job_id, "successful")


@pytest.mark.parametrize(("crashes", "max_attempts"), [(1, 0), (2, 1), (4, 2)])
async def test_job_past_its_limit_is_abandoned(
    harness: Harness, crashes: int, max_attempts: int
) -> None:
    """The issue's crash loop: a job past its limit is not run again and ends as exception."""
    (job_id,) = await harness.q.enqueue("ep", None)
    for _ in range(crashes):
        await harness.crash()

    ran = await drain(harness.q, max_attempts, "delete")

    assert ran == []
    assert await harness.q.queue_size() == []
    traceback = (await log_for(harness.q, job_id, "exception")).traceback
    assert traceback is not None
    assert traceback.exception_type == "MaxAttemptsExceededError"
    assert f"abandoned after {crashes} attempts" in traceback.exception_message


async def test_abandoned_job_is_held_with_on_failure_hold(harness: Harness) -> None:
    (job_id,) = await harness.q.enqueue("ep", None)
    await harness.crash()
    await harness.crash()

    ran = await drain(harness.q, 1, "hold")

    assert ran == []
    (held,) = await harness.q.list_failed_jobs()
    assert (held.id, held.status, held.attempts) == (job_id, "failed", 2)
    traceback = (await log_for(harness.q, job_id, "failed")).traceback
    assert traceback is not None
    assert "abandoned after 2 attempts" in traceback.exception_message


async def test_requeued_abandoned_job_runs_again(harness: Harness) -> None:
    """An operator requeue gives a held job its full budget back."""
    (job_id,) = await harness.q.enqueue("ep", None)
    await harness.crash()
    await harness.crash()
    assert await drain(harness.q, 1, "hold") == []

    await harness.q.requeue_jobs([job_id])
    ran = await drain(harness.q, 1, "hold")

    assert [(job.id, job.attempts) for job in ran] == [(job_id, 0)]


# Registration.


@pytest.mark.parametrize("max_attempts", [-1, 1.5, "3", True])
def test_entrypoint_rejects_invalid_max_attempts(max_attempts: object) -> None:
    qm = QueueManager(InMemoryQueries(driver=InMemoryDriver()))
    with pytest.raises(ValueError, match="max_attempts"):
        qm.entrypoint("ep", max_attempts=max_attempts)  # type: ignore[arg-type]


def test_pgqueuer_entrypoint_forwards_max_attempts() -> None:
    pgq = PgQueuer.in_memory()

    @pgq.entrypoint("ep", max_attempts=3)
    async def ep(job: Job) -> None: ...

    assert pgq.qm.entrypoint_registry[QueueEntrypoint("ep")].parameters.max_attempts == 3


# The issue's reproduction: the author's worker script and loop, with max_attempts set.

POISON_WORKER = textwrap.dedent(
    """
    import asyncio, os, sys
    from datetime import timedelta

    import asyncpg

    from pgqueuer.db import AsyncpgDriver
    from pgqueuer.models import Job
    from pgqueuer.qm import QueueManager
    from pgqueuer.queries import Queries
    from pgqueuer.types import QueueExecutionMode


    async def worker() -> None:
        qm = QueueManager(Queries(AsyncpgDriver(await asyncpg.connect(sys.argv[1]))))

        @qm.entrypoint("poison", max_attempts=2)
        async def poison(job: Job) -> None:
            os._exit(137)  # Stands in for an OOM kill or a segfault caused by this job.

        await qm.run(dequeue_timeout=timedelta(hours=1), mode=QueueExecutionMode.drain)


    asyncio.run(worker())
    """
)


async def test_issue_reproduction_poison_job_stops_after_max_attempts(
    dsn: str, apgdriver: AsyncpgDriver
) -> None:
    """The issue's script: four workers in turn, each picks the poison job and is killed.

    On 2.1.0 all four die and the job stays picked. With max_attempts=2 the first
    three die (fresh pick, takeovers 1 and 2) and the fourth abandons it and exits.
    Instead of the issue's sleep past heartbeat_timeout, the heartbeat is backdated.
    """
    q = Queries(apgdriver)
    (job_id,) = await q.enqueue("poison", None)

    exit_codes = []
    async with asyncio.timeout(GUARD):
        for _ in range(4):
            proc = await asyncio.create_subprocess_exec(sys.executable, "-c", POISON_WORKER, dsn)
            exit_codes.append(await proc.wait())
            await apgdriver.execute(
                f"""
                UPDATE {q.qbq.qualified.queue_table}
                SET heartbeat = NOW() - interval '1 hour'
                WHERE id = $1
                """,
                job_id,
            )

    assert exit_codes == [137, 137, 137, 0]
    assert await q.queue_size() == []
    traceback = (await log_for(q, job_id, "exception")).traceback
    assert traceback is not None
    assert "abandoned after 3 attempts" in traceback.exception_message
