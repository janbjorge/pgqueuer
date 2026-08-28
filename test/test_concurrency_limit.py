from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, AsyncGenerator, Awaitable, Callable

import async_timeout
import asyncpg
import pytest
import pytest_asyncio

from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import EntrypointExecutionParameter, Queries


@dataclass
class Tally:
    active: int = 0
    max_active: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def __aenter__(self) -> None:
        async with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

    async def __aexit__(self, *_: object) -> None:
        async with self.lock:
            self.active -= 1


@pytest.mark.parametrize("n_consumers", (1, 2, 4))
@pytest.mark.parametrize("max_concurrency", (1, 5, 10))
async def test_max_concurrency(
    n_consumers: int,
    max_concurrency: int,
    apgdriver: Driver,
    n_tasks: int = 500,
    wait: int = 2,
) -> None:
    """concurrency_limit is enforced globally across all workers."""
    await Queries(apgdriver).enqueue(
        ["fetch"] * n_tasks,
        [f"{i}".encode() for i in range(n_tasks)],
        [0] * n_tasks,
    )

    shared = Tally()
    qms = [QueueManager(Queries(apgdriver)) for _ in range(n_consumers)]

    async def run_consumer(qm: QueueManager) -> None:
        @qm.entrypoint("fetch", concurrency_limit=max_concurrency)
        async def fetch(job: Job) -> None:
            async with shared:
                await asyncio.sleep(0.001)

        await qm.run(dequeue_timeout=timedelta(seconds=0))

    async def timer() -> None:
        await asyncio.sleep(wait)
        for q in qms:
            q.shutdown.set()

    await asyncio.gather(timer(), *(run_consumer(q) for q in qms))

    assert 0 < shared.max_active <= max_concurrency


@pytest.mark.parametrize("concurrency_limit", (1, 5, 10))
async def test_concurrency_entrypoint_isolation(
    apgdriver: Driver,
    concurrency_limit: int,
) -> None:
    event = asyncio.Event()
    N = concurrency_limit * 1_000
    await Queries(apgdriver).enqueue(
        ["fetch_1", "fetch_2"] * N,
        [None, None] * N,
        [0, 0] * N,
    )

    qm = QueueManager(Queries(apgdriver))

    @qm.entrypoint("fetch_1", concurrency_limit=concurrency_limit)
    async def fetch_1(job: Job) -> None:
        await event.wait()

    @qm.entrypoint("fetch_2", concurrency_limit=concurrency_limit)
    async def fetch_2(job: Job) -> None:
        await event.wait()

    async def timer() -> None:
        async with async_timeout.timeout(10):
            while len(event._waiters) < 2 * concurrency_limit:
                await asyncio.sleep(0.001)
            len_waiter = len(event._waiters)
            qm.shutdown.set()
            event.set()
            assert len_waiter == 2 * concurrency_limit

    await asyncio.gather(
        timer(),
        qm.run(
            batch_size=5,
            dequeue_timeout=timedelta(seconds=5),
        ),
    )


async def test_tight_entrypoint_does_not_throttle_unlimited_entrypoint(
    apgdriver: Driver,
) -> None:
    """A limit=1 entrypoint must not collapse dequeue batches for other entrypoints."""
    N = 20
    batch_size = 10
    q = Queries(apgdriver)
    await q.enqueue(["unlimited"] * N, [None] * N, [0] * N)

    qm = QueueManager(q)
    event = asyncio.Event()

    @qm.entrypoint("tight", concurrency_limit=1)
    async def tight(job: Job) -> None:
        await event.wait()

    @qm.entrypoint("unlimited")
    async def unlimited(job: Job) -> None:
        await event.wait()

    dequeue_batches = list[int]()
    original_dequeue = q.dequeue

    async def recording_dequeue(*, batch_size: int, **kwargs: Any) -> list[Job]:
        dequeue_batches.append(batch_size)
        return await original_dequeue(batch_size=batch_size, **kwargs)

    q.dequeue = recording_dequeue  # type: ignore[assignment]

    async def timer() -> None:
        async with async_timeout.timeout(10):
            while len(event._waiters) < N:
                await asyncio.sleep(0.001)
            qm.shutdown.set()
            event.set()

    await asyncio.gather(
        timer(),
        qm.run(batch_size=batch_size, dequeue_timeout=timedelta(seconds=1)),
    )

    assert dequeue_batches
    assert set(dequeue_batches) == {batch_size}


@pytest_asyncio.fixture
async def connect(dsn: str) -> AsyncGenerator[Callable[[], Awaitable[asyncpg.Connection]], None]:
    """Hand out connections to the per-test database; close them on teardown."""
    async with contextlib.AsyncExitStack() as stack:

        async def _connect() -> asyncpg.Connection:
            connection = await asyncpg.connect(dsn=dsn)
            stack.push_async_callback(connection.close)
            return connection

        yield _connect


async def _wait_until_done_or_lock_blocked(
    monitor: asyncpg.Connection,
    task: asyncio.Task[list[Job]],
    backend_pid: int,
) -> None:
    """Poll until *task* finished or its backend is waiting on a heavyweight lock.

    Distinguishes "statement completed" (task done) from "statement blocked on
    worker A's uncommitted claim" (wait_event_type = 'Lock'), so the test never
    relies on a fixed sleep for the interleaving.
    """
    async with async_timeout.timeout(30):
        while not task.done():
            wait_event_type = await monitor.fetchval(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
                backend_pid,
            )
            if wait_event_type == "Lock":
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("concurrency_limit", (1, 2))
async def test_concurrency_limit_holds_across_concurrent_dequeues(
    connect: Callable[[], Awaitable[asyncpg.Connection]],
    concurrency_limit: int,
) -> None:
    """Regression test for #761: concurrency_limit must hold across workers.

    Two workers dequeue over separate connections. Worker A claims a full
    batch inside an open transaction, freezing the in-flight instant where
    its picked rows are neither visible to worker B's snapshot (the capacity
    count reads zero) nor released (SKIP LOCKED slides past them onto the
    remaining queued rows). Worker B must claim nothing; any claim here
    exceeds the entrypoint's global concurrency limit.

    Worker B runs as a task and worker A commits once B has either finished
    or provably blocked on A's transaction, so the test stays deterministic
    regardless of whether the dequeue implementation skips, waits, or retries.
    """
    conn_a = await connect()
    conn_b = await connect()
    conn_monitor = await connect()

    queries_a = Queries(AsyncpgDriver(conn_a))
    queries_b = Queries(AsyncpgDriver(conn_b))

    n_jobs = 2 * concurrency_limit
    await queries_a.enqueue(
        ["fetch"] * n_jobs,
        [f"{n}".encode() for n in range(n_jobs)],
        [0] * n_jobs,
    )

    def dequeue(q: Queries) -> asyncio.Task[list[Job]]:
        return asyncio.ensure_future(
            q.dequeue(
                batch_size=concurrency_limit,
                entrypoints={"fetch": EntrypointExecutionParameter(concurrency_limit)},
                queue_manager_id=uuid.uuid4(),
                global_concurrency_limit=None,
                heartbeat_timeout=timedelta(minutes=10),
            )
        )

    transaction_a = conn_a.transaction()
    await transaction_a.start()
    first = await dequeue(queries_a)
    assert len(first) == concurrency_limit

    # Worker A's claim is now in flight: uncommitted, row locks held.
    task_b = dequeue(queries_b)
    await _wait_until_done_or_lock_blocked(conn_monitor, task_b, conn_b.get_server_pid())
    await transaction_a.commit()

    overlap = await asyncio.wait_for(task_b, timeout=30)
    assert overlap == [], f"picked {len(first) + len(overlap)} jobs, limit {concurrency_limit}"

    # A's claim is committed and visible; capacity is exhausted either way.
    visible = await asyncio.wait_for(dequeue(queries_b), timeout=30)
    assert visible == []


async def test_concurrency_limit_holds_against_higher_priority_arrival(
    connect: Callable[[], Awaitable[asyncpg.Connection]],
) -> None:
    """Regression test for #761, the snapshot-window hole.

    Worker A claims the only queued job inside an open transaction. A higher
    priority job then arrives and commits, so worker B's snapshot counts zero
    picked jobs AND ranks the new job first: no candidate-window shape can
    connect the two claims, because B's window no longer contains A's row.
    B must still claim nothing. The capacity-slot unique index provides that:
    B's claim takes the same slot as A's uncommitted one, waits on A in the
    btree uniqueness check, aborts with a unique violation once A commits,
    and B reports an empty batch.
    """
    conn_a = await connect()
    conn_b = await connect()
    conn_monitor = await connect()

    queries_a = Queries(AsyncpgDriver(conn_a))
    queries_b = Queries(AsyncpgDriver(conn_b))
    queries_monitor = Queries(AsyncpgDriver(conn_monitor))

    await queries_monitor.enqueue("fetch", b"low", priority=0)

    def dequeue(q: Queries) -> asyncio.Task[list[Job]]:
        return asyncio.ensure_future(
            q.dequeue(
                batch_size=1,
                entrypoints={"fetch": EntrypointExecutionParameter(concurrency_limit=1)},
                queue_manager_id=uuid.uuid4(),
                global_concurrency_limit=None,
                heartbeat_timeout=timedelta(minutes=10),
            )
        )

    transaction_a = conn_a.transaction()
    await transaction_a.start()
    first = await dequeue(queries_a)
    assert len(first) == 1

    await queries_monitor.enqueue("fetch", b"high", priority=10)

    task_b = dequeue(queries_b)
    await _wait_until_done_or_lock_blocked(conn_monitor, task_b, conn_b.get_server_pid())
    await transaction_a.commit()

    overlap = await asyncio.wait_for(task_b, timeout=30)
    assert overlap == [], f"picked {len(first) + len(overlap)} jobs, limit 1"
