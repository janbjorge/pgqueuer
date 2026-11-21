"""A worker with free capacity and claimable work must claim it without waiting for its poll.

Every worker here runs with a one-hour dequeue_timeout, so progress can only come from a
NOTIFY or a computed short timeout (#1087, #1092). GUARD fires only when the worker stalls.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any
from unittest.mock import patch

import asyncpg

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.domain.errors import RetryRequested
from pgqueuer.models import Job
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import JobId, OnFailure, QueueEntrypoint, QueueManagerId

HOUR = timedelta(hours=1)
# Below pytest's 20 s per-test timeout, so a stall reports as a TimeoutError here.
GUARD = 15


class Gate:
    """Handlers report each start, then block until the test lets one finish."""

    def __init__(self) -> None:
        self.started: asyncio.Queue[JobId] = asyncio.Queue()
        self.finish = asyncio.Semaphore(0)

    async def enter(self, job: Job) -> None:
        self.started.put_nowait(job.id)
        await self.finish.acquire()

    def open(self) -> None:
        for _ in range(64):
            self.finish.release()


@contextlib.asynccontextmanager
async def worker(qm: QueueManager, gate: Gate, **run_kwargs: Any) -> AsyncIterator[None]:
    task = asyncio.create_task(qm.run(dequeue_timeout=HOUR, **run_kwargs))
    try:
        yield
    finally:
        qm.shutdown.set()
        gate.open()
        await task


async def freed_budget_slot_is_refilled(
    q: RepositoryPort,
    outcome: Callable[[], None],
    on_failure: OnFailure = "delete",
) -> None:
    qm = QueueManager(q)
    gate = Gate()

    @qm.entrypoint("ep", on_failure=on_failure)
    async def ep(job: Job) -> None:
        await gate.enter(job)
        outcome()

    await q.enqueue(["ep"] * 3, [None] * 3, [0] * 3)
    async with asyncio.timeout(GUARD), worker(qm, gate, batch_size=1, max_concurrent_tasks=2):
        await gate.started.get()
        await gate.started.get()
        gate.finish.release()
        await gate.started.get()


def succeed() -> None:
    pass


def fail() -> None:
    raise ValueError("held")


def retry() -> None:
    raise RetryRequested


async def test_budget_success_refills_slot(apgdriver: Driver) -> None:
    await freed_budget_slot_is_refilled(Queries(apgdriver), succeed)


async def test_budget_success_refills_slot_inmemory() -> None:
    await freed_budget_slot_is_refilled(InMemoryQueries(driver=InMemoryDriver()), succeed)


async def test_budget_hold_refills_slot(apgdriver: Driver) -> None:
    """The held row's DELETE CTE statement trigger is the only wake."""
    await freed_budget_slot_is_refilled(Queries(apgdriver), fail, on_failure="hold")


async def test_budget_hold_refills_slot_inmemory() -> None:
    queries = InMemoryQueries(driver=InMemoryDriver())
    await freed_budget_slot_is_refilled(queries, fail, on_failure="hold")


async def test_budget_retry_refills_slot(apgdriver: Driver) -> None:
    """retry_job's explicit NOTIFY is the only wake; the UPDATE trigger stays silent."""
    await freed_budget_slot_is_refilled(Queries(apgdriver), retry)


async def test_budget_retry_refills_slot_inmemory() -> None:
    await freed_budget_slot_is_refilled(InMemoryQueries(driver=InMemoryDriver()), retry)


async def test_entrypoint_slot_freed_on_one_worker_is_refilled(apgdriver: Driver, dsn: str) -> None:
    connection = await asyncpg.connect(dsn=dsn)
    try:
        qa, qb = Queries(apgdriver), Queries(AsyncpgDriver(connection))
        a, b = QueueManager(qa), QueueManager(qb)
        gate = Gate()
        for qm in (a, b):
            qm.entrypoint("ep", concurrency_limit=1)(gate.enter)

        b_idle = asyncio.Event()
        dequeue = qb.dequeue

        async def idle_dequeue(**kwargs: Any) -> list[Job]:
            jobs = await dequeue(**kwargs)
            if not jobs:
                b_idle.set()
            return jobs

        await qa.enqueue(["ep"] * 3, [None] * 3, [0] * 3)
        with patch.object(qb, "dequeue", idle_dequeue):
            async with asyncio.timeout(GUARD), worker(a, gate):
                await gate.started.get()
                async with worker(b, gate):
                    # B found the slot taken and heads for its wait.
                    await b_idle.wait()
                    gate.finish.release()
                    await gate.started.get()
    finally:
        await connection.close()


async def notice_racing_the_probe_is_kept(q: RepositoryPort, driver: Driver) -> None:
    """A NOTIFY landing after the drain but before the wait must still wake the worker."""
    qm = QueueManager(q)
    gate = Gate()
    qm.entrypoint("ep")(gate.enter)

    heard = asyncio.Event()

    def on_notice(payload: str | bytes | bytearray) -> None:
        if b"table_changed_event" in (payload.encode() if isinstance(payload, str) else payload):
            heard.set()

    await driver.add_listener(qm.channel, on_notice)
    probe = qm._effective_dequeue_timeout
    raced = False

    async def stale_probe(dequeue_timeout: timedelta, *args: Any) -> timedelta:
        nonlocal raced
        if raced:
            return await probe(dequeue_timeout, *args)
        raced = True
        # Commit work and let its NOTIFY reach the listener, then answer from an older snapshot.
        await q.enqueue("ep", None)
        await heard.wait()
        return dequeue_timeout

    with patch.object(qm, "_effective_dequeue_timeout", stale_probe):
        async with asyncio.timeout(GUARD), worker(qm, gate):
            await gate.started.get()


async def test_notice_racing_the_probe_is_kept(apgdriver: Driver) -> None:
    await notice_racing_the_probe_is_kept(Queries(apgdriver), apgdriver)


async def test_notice_racing_the_probe_is_kept_inmemory() -> None:
    driver = InMemoryDriver()
    await notice_racing_the_probe_is_kept(InMemoryQueries(driver=driver), driver)


async def stale_slot_is_recovered(q: RepositoryPort) -> None:
    """A dead worker's picked row holding the only slot is re-picked once its heartbeat expires."""
    heartbeat_timeout = timedelta(seconds=2)
    qm = QueueManager(q)
    gate = Gate()
    qm.entrypoint("ep", concurrency_limit=1)(gate.enter)

    await q.enqueue(["ep"] * 2, [None] * 2, [0] * 2)
    (dead,) = await q.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(concurrency_limit=1)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=heartbeat_timeout,
    )
    async with asyncio.timeout(GUARD), worker(qm, gate, heartbeat_timeout=heartbeat_timeout):
        assert await gate.started.get() == dead.id


async def test_stale_slot_is_recovered(apgdriver: Driver) -> None:
    await stale_slot_is_recovered(Queries(apgdriver))


async def test_stale_slot_is_recovered_inmemory() -> None:
    await stale_slot_is_recovered(InMemoryQueries(driver=InMemoryDriver()))
