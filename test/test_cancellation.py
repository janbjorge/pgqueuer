import asyncio
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.errors import RetryRequested
from pgqueuer.executors import DatabaseRetryEntrypointExecutor, EntrypointExecutorParameters
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import JOB_STATUS, OnFailure, QueueExecutionMode


@pytest.mark.parametrize("N", (1, 4, 32, 100))
async def test_cancellation_async(
    apgdriver: db.Driver,
    N: int,
) -> None:
    event = asyncio.Event()
    cancel_called_not_cancel_called = list[bool]()
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver), resources={"test_key": "async"})

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job) -> None:
        scope = qm.get_context(job.id).cancellation
        assert qm.get_context(job.id).resources["test_key"] == "async"
        await event.wait()
        cancel_called_not_cancel_called.append(scope.cancel_called)

    job_ids = await q.enqueue(
        ["to_be_canceled"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.status == "picked") < N:
            await asyncio.sleep(0.01)

        await q.mark_job_as_cancelled(job_ids)
        event.set()

        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.0)),
        waiter(),
    )

    assert sum(cancel_called_not_cancel_called) == N

    # Logged as canceled

    assert sum(x.status == "canceled" for x in await q.queue_log()) == N


@pytest.mark.parametrize("N", (1, 4, 32, 100))
async def test_cancellation_async_context_manager(
    apgdriver: db.Driver,
    N: int,
) -> None:
    event = asyncio.Event()
    cancel_called_not_cancel_called = list[bool]()
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver), resources={"test_key": "async_cm"})

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job) -> None:
        with qm.get_context(job.id).cancellation as scope:
            assert qm.get_context(job.id).resources["test_key"] == "async_cm"
            await event.wait()
            cancel_called_not_cancel_called.append(scope.cancel_called)

    job_ids = await q.enqueue(
        ["to_be_canceled"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.status == "picked") < N:
            await asyncio.sleep(0)

        await q.mark_job_as_cancelled(job_ids)
        event.set()

        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert sum(cancel_called_not_cancel_called) == 0

    # Logged as canceled
    assert sum(x.status == "canceled" for x in await q.queue_log()) == N


async def test_cancellation_drain_mode(apgdriver: db.Driver) -> None:
    N = 10
    event = asyncio.Event()
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver), resources={"test_key": "drain"})

    @qm.entrypoint("to_be_canceled")
    async def to_be_canceled(job: Job) -> None:
        ctx = qm.get_context(job.id)
        with ctx.cancellation:
            assert ctx.resources["test_key"] == "drain"
            await event.wait()

    jids = await q.enqueue(
        ["to_be_canceled"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    async def cancel_after() -> None:
        await asyncio.sleep(0.5)
        await q.mark_job_as_cancelled(jids)

    await asyncio.gather(
        cancel_after(),
        qm.run(mode=QueueExecutionMode.drain),
    )

    assert sum(x.status == "canceled" for x in await q.queue_log()) == N


@pytest.mark.parametrize(
    ("on_failure", "end", "held"),
    (("delete", "exception", []), ("hold", "failed", [("failed", 1)])),
)
async def test_handler_raised_cancelled_error_fails_the_job(
    queries: InMemoryQueries,
    on_failure: OnFailure,
    end: JOB_STATUS,
    held: list[tuple[JOB_STATUS, int]],
) -> None:
    """A CancelledError that the handler raises with no cancel request fails the job."""
    qm = QueueManager(queries)
    seen = list[int]()

    @qm.entrypoint("ep", on_failure=on_failure)
    async def handler(job: Job) -> None:
        seen.append(job.attempts)
        sub_task = asyncio.create_task(asyncio.Event().wait())
        sub_task.cancel()
        await sub_task

    await queries.enqueue(["ep"], [None], [0])
    async with asyncio.timeout(5):
        await qm.run(mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(seconds=1))

    log = await queries.queue_log()
    assert [entry.status for entry in log] == ["queued", "picked", end]
    assert log[-1].traceback is not None
    assert log[-1].traceback.exception_type == "CancelledError"
    assert seen == [0]
    assert [(row.status, row.count) for row in await queries.queue_size()] == held


async def test_cancelled_error_after_a_cancel_request_ends_the_job_canceled(
    queries: InMemoryQueries,
) -> None:
    """A handler that sees a cancel request and raises CancelledError ends the job as canceled."""
    qm = QueueManager(queries)
    started = asyncio.Event()
    resume = asyncio.Event()
    seen = list[int]()

    @qm.entrypoint("ep")
    async def handler(job: Job) -> None:
        seen.append(job.attempts)
        started.set()
        await resume.wait()
        if qm.get_context(job.id).cancellation.cancel_called:
            raise asyncio.CancelledError

    [job_id] = await queries.enqueue(["ep"], [None], [0])

    async def cancel_while_running() -> None:
        await started.wait()
        await queries.mark_job_as_cancelled([job_id])
        resume.set()

    async with asyncio.timeout(5):
        await asyncio.gather(
            qm.run(mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(seconds=1)),
            cancel_while_running(),
        )

    log = await queries.queue_log()
    assert [entry.status for entry in log] == ["queued", "picked", "canceled"]
    assert log[-1].traceback is None
    assert seen == [0]
    assert await queries.queue_size() == []


def retry_without_delay(
    parameters: EntrypointExecutorParameters,
) -> DatabaseRetryEntrypointExecutor:
    return DatabaseRetryEntrypointExecutor(parameters=parameters, initial_delay=timedelta(0))


async def retry_after_cancel(qm: QueueManager, *, through_executor: bool) -> list[int]:
    """Cancel the running job, then let its handler ask for a retry. Return the attempts seen."""
    seen = list[int]()

    @qm.entrypoint("ep", executor_factory=retry_without_delay if through_executor else None)
    async def handler(job: Job) -> None:
        seen.append(job.attempts)
        if job.attempts > 0:
            return
        await qm.queries.mark_job_as_cancelled([job.id])
        with qm.get_context(job.id).cancellation:
            await asyncio.Event().wait()  # The cancel ends this wait when it reaches the worker.
        if through_executor:
            raise ConnectionError("upstream down")
        raise RetryRequested(reason="transient")

    await qm.queries.enqueue(["ep"], [None], [0])
    async with asyncio.timeout(5):
        await qm.run(mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(seconds=1))
    return seen


@pytest.mark.parametrize(
    "through_executor", (False, True), ids=("retry_requested", "retry_executor")
)
async def test_retry_after_cancel_ends_the_job_canceled(
    queries: InMemoryQueries,
    through_executor: bool,
) -> None:
    """A retry request after the worker saw a cancel ends the job as canceled, with no re-run."""
    seen = await retry_after_cancel(QueueManager(queries), through_executor=through_executor)

    assert [entry.status for entry in await queries.queue_log()] == [
        "queued",
        "picked",
        "canceled",
    ]
    assert seen == [0]
    assert await queries.queue_size() == []


async def test_retry_after_cancel_ends_the_job_canceled_postgres(apgdriver: db.Driver) -> None:
    """In PostgreSQL, a retry request after the worker saw a cancel ends the job as canceled."""
    q = Queries(apgdriver)
    seen = await retry_after_cancel(QueueManager(q), through_executor=False)

    # The log query has no ORDER BY.
    assert sorted(entry.status for entry in await q.queue_log()) == ["canceled", "picked", "queued"]
    assert seen == [0]
    assert await q.queue_size() == []
