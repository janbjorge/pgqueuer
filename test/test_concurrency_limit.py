from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.domain.types import JOB_STATUS, JobId, QueueEntrypoint, QueueManagerId
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import EntrypointExecutionParameter, Queries

FETCH = QueueEntrypoint("fetch")


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
) -> None:
    """concurrency_limit is enforced globally across all workers."""
    n_tasks = 500
    wait = 2
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
        async with asyncio.timeout(10):
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
        async with asyncio.timeout(10):
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
    async with asyncio.timeout(30):
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
                entrypoints={FETCH: EntrypointExecutionParameter(concurrency_limit)},
                queue_manager_id=QueueManagerId(uuid.uuid4()),
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


async def test_concurrency_limit_holds_when_priority_arrives_mid_claim(
    connect: Callable[[], Awaitable[asyncpg.Connection]],
) -> None:
    """A higher-priority arrival must not let a second worker exceed the limit.

    Workers agree on a candidate window only while the queue ordering is
    stable. A higher-priority job arriving inside another worker's in-flight
    claim changes that ordering, so worker B windows a row worker A never
    locked. Only the capacity slot stops the claim.
    """
    limit = 2
    conn_a = await connect()
    conn_b = await connect()
    conn_monitor = await connect()

    queries_a = Queries(AsyncpgDriver(conn_a))
    queries_b = Queries(AsyncpgDriver(conn_b))
    queries_monitor = Queries(AsyncpgDriver(conn_monitor))

    await queries_monitor.enqueue(["fetch"] * limit, [b"low"] * limit, [0] * limit)

    def dequeue(q: Queries) -> asyncio.Task[list[Job]]:
        return asyncio.ensure_future(
            q.dequeue(
                batch_size=limit,
                entrypoints={FETCH: EntrypointExecutionParameter(limit)},
                queue_manager_id=QueueManagerId(uuid.uuid4()),
                global_concurrency_limit=None,
                heartbeat_timeout=timedelta(minutes=10),
            )
        )

    transaction_a = conn_a.transaction()
    await transaction_a.start()
    first = await dequeue(queries_a)
    assert len(first) == limit

    await queries_monitor.enqueue(["fetch"], [b"high"], [10])

    task_b = dequeue(queries_b)
    await _wait_until_done_or_lock_blocked(conn_monitor, task_b, conn_b.get_server_pid())
    await transaction_a.commit()

    overlap = await asyncio.wait_for(task_b, timeout=30)
    total = len(first) + len(overlap)
    assert total <= limit, f"picked {total} jobs, limit {limit}"


@pytest.mark.parametrize("concurrency_limit", (1, 2))
async def test_concurrency_limit_rollback_releases_capacity_for_the_other_worker(
    connect: Callable[[], Awaitable[asyncpg.Connection]],
    concurrency_limit: int,
) -> None:
    """A rolled-back claim must free the slot so the waiting worker can pick."""
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
                entrypoints={FETCH: EntrypointExecutionParameter(concurrency_limit)},
                queue_manager_id=QueueManagerId(uuid.uuid4()),
                global_concurrency_limit=None,
                heartbeat_timeout=timedelta(minutes=10),
            )
        )

    transaction_a = conn_a.transaction()
    await transaction_a.start()
    first = await dequeue(queries_a)
    assert len(first) == concurrency_limit

    task_b = dequeue(queries_b)
    await _wait_until_done_or_lock_blocked(conn_monitor, task_b, conn_b.get_server_pid())
    await transaction_a.rollback()

    overlap = await asyncio.wait_for(task_b, timeout=30)
    claimed = overlap or await asyncio.wait_for(dequeue(queries_b), timeout=30)
    assert len(claimed) == concurrency_limit


async def test_slot_race_does_not_lose_unlimited_jobs_in_the_same_batch(
    connect: Callable[[], Awaitable[asyncpg.Connection]],
) -> None:
    """A lost limited slot aborts the whole statement; unlimited work stays queued.

    Limited and unlimited claims share one UPDATE. A unique-violation on the
    limited slot rolls that statement back, including any unlimited rows it
    would have picked. Those rows remain queued, so the next poll takes them.
    """
    limit = 1
    n_loose = 5
    conn_a = await connect()
    conn_b = await connect()
    conn_monitor = await connect()

    queries_a = Queries(AsyncpgDriver(conn_a))
    queries_b = Queries(AsyncpgDriver(conn_b))
    queries_monitor = Queries(AsyncpgDriver(conn_monitor))

    await queries_monitor.enqueue(["tight"] * limit, [b"tight"] * limit, [0] * limit)
    await queries_monitor.enqueue(["loose"] * n_loose, [b"loose"] * n_loose, [0] * n_loose)

    tight = {QueueEntrypoint("tight"): EntrypointExecutionParameter(limit)}
    both = {
        QueueEntrypoint("tight"): EntrypointExecutionParameter(limit),
        QueueEntrypoint("loose"): EntrypointExecutionParameter(0),
    }

    def dequeue(
        q: Queries, entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter]
    ) -> asyncio.Task[list[Job]]:
        return asyncio.ensure_future(
            q.dequeue(
                batch_size=10,
                entrypoints=entrypoints,
                queue_manager_id=QueueManagerId(uuid.uuid4()),
                global_concurrency_limit=None,
                heartbeat_timeout=timedelta(minutes=10),
            )
        )

    transaction_a = conn_a.transaction()
    await transaction_a.start()
    first = await dequeue(queries_a, tight)
    assert len(first) == limit
    assert {job.entrypoint for job in first} == {"tight"}

    # A new tight job changes B's candidate window onto a row A never locked,
    # so B contends for the slot instead of SKIP LOCKED-skipping A's rows.
    await queries_monitor.enqueue(["tight"], [b"late"], [10])

    task_b = dequeue(queries_b, both)
    await _wait_until_done_or_lock_blocked(conn_monitor, task_b, conn_b.get_server_pid())
    await transaction_a.commit()

    overlap = await asyncio.wait_for(task_b, timeout=30)
    tight_overlap = [job for job in overlap if job.entrypoint == "tight"]
    assert len(first) + len(tight_overlap) <= limit

    loose_ids = {job.id for job in overlap if job.entrypoint == "loose"}
    remaining = await asyncio.wait_for(dequeue(queries_b, both), timeout=30)
    loose_ids.update(job.id for job in remaining if job.entrypoint == "loose")
    assert len(loose_ids) == n_loose
    assert all(job.entrypoint != "tight" for job in remaining)


async def test_dequeue_assigns_capacity_slots(apgdriver: Driver) -> None:
    """A limited entrypoint gets a distinct seat in ``Job.slot``; an unlimited one gets none."""
    limit = 3
    n_fetch = 2 * limit
    n_loose = 2
    q = Queries(apgdriver)
    await q.enqueue(["fetch"] * n_fetch, [None] * n_fetch, [0] * n_fetch)
    await q.enqueue(["loose"] * n_loose, [None] * n_loose, [0] * n_loose)

    picked = await q.dequeue(
        batch_size=10,
        entrypoints={
            FETCH: EntrypointExecutionParameter(limit),
            QueueEntrypoint("loose"): EntrypointExecutionParameter(0),
        },
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(minutes=10),
    )

    fetch = [job for job in picked if job.entrypoint == FETCH]
    loose = [job for job in picked if job.entrypoint == "loose"]
    assert len(fetch) == limit
    assert {job.slot for job in fetch} == set(range(limit))
    assert len(loose) == n_loose
    assert all(job.slot is None for job in loose)


async def test_limited_dequeue_claims_top_priority_jobs_within_remaining(apgdriver: Driver) -> None:
    """With fewer free slots than batch_size, the highest-priority, lowest-id jobs are claimed."""
    q = Queries(apgdriver)

    async def dequeue(batch_size: int) -> list[Job]:
        return await q.dequeue(
            batch_size=batch_size,
            entrypoints={FETCH: EntrypointExecutionParameter(3)},
            queue_manager_id=QueueManagerId(uuid.uuid4()),
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(minutes=10),
        )

    await q.enqueue(["fetch"], [None], [0])
    assert len(await dequeue(batch_size=1)) == 1

    priorities = [0, 5, 1, 5, 5, 3]
    ids = await q.enqueue(["fetch"] * len(priorities), [None] * len(priorities), priorities)

    claimed = await dequeue(batch_size=10)
    assert {job.id for job in claimed} == {ids[1], ids[3]}


async def claim(
    q: Queries,
    limits: dict[str, int],
    global_concurrency_limit: int | None = None,
) -> list[Job]:
    """Dequeue up to ten jobs as a new worker with these per-entrypoint limits."""
    return await q.dequeue(
        batch_size=10,
        entrypoints={
            QueueEntrypoint(name): EntrypointExecutionParameter(limit)
            for name, limit in limits.items()
        },
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=global_concurrency_limit,
        heartbeat_timeout=timedelta(minutes=10),
    )


async def slots(q: Queries, status: JOB_STATUS) -> dict[JobId, int | None]:
    """Map each queue row with this status to its slot."""
    return {job.id: job.slot for job in await q.browse_queue(statuses=[status])}


async def retried_under_limit(q: Queries, n_jobs: int) -> list[JobId]:
    """Retry fetch jobs one at a time under concurrency_limit=1. Each one keeps slot 0."""
    ids = list[JobId]()
    for priority in range(n_jobs):
        # A higher priority makes the limited worker take the new job, not a retried one.
        ids += await q.enqueue(["fetch"], [None], [priority])
        (job,) = await claim(q, {"fetch": 1})
        await q.retry_job(job, timedelta(0), None)
    assert await slots(q, "queued") == dict.fromkeys(ids, 0)
    return ids


@pytest.mark.parametrize("global_concurrency_limit", (None, 10))
async def test_unlimited_dequeue_claims_jobs_retried_under_a_limit(
    apgdriver: Driver,
    global_concurrency_limit: int | None,
) -> None:
    """A dequeue without limits claims retried jobs that kept a slot from a limited run."""
    q = Queries(apgdriver)
    first, second = await retried_under_limit(q, n_jobs=2)
    (other,) = await q.enqueue(["other"], [None], [0])

    claimed = await claim(q, {"fetch": 0, "other": 0}, global_concurrency_limit)

    assert sorted(job.id for job in claimed) == [first, second, other]
    assert await slots(q, "picked") == {first: None, second: None, other: None}


async def test_unlimited_dequeue_claims_a_retried_job_while_its_slot_is_held(
    apgdriver: Driver,
) -> None:
    """A dequeue without limits claims a retried job while a limited worker holds its slot."""
    q = Queries(apgdriver)
    (retried,) = await retried_under_limit(q, n_jobs=1)
    (held,) = await q.enqueue(["fetch"], [None], [1])
    assert [job.id for job in await claim(q, {"fetch": 1})] == [held]

    claimed = await claim(q, {"fetch": 0})

    assert [job.id for job in claimed] == [retried]
    assert await slots(q, "picked") == {held: 0, retried: None}


async def test_unlimited_dequeue_claims_jobs_held_and_requeued_under_a_limit(
    apgdriver: Driver,
) -> None:
    """A dequeue without limits claims held jobs that kept a slot through a requeue."""
    q = Queries(apgdriver)
    first, second = await q.enqueue(["fetch"] * 2, [None] * 2, [0] * 2)
    for _ in range(2):
        (job,) = await claim(q, {"fetch": 1})
        await q.log_jobs([(job, "failed", None)])
    await q.requeue_jobs([first, second])
    assert await slots(q, "queued") == {first: 0, second: 0}

    claimed = await claim(q, {"fetch": 0})

    assert sorted(job.id for job in claimed) == [first, second]
    assert await slots(q, "picked") == {first: None, second: None}


async def test_worker_without_the_limit_drains_jobs_that_kept_a_slot(apgdriver: Driver) -> None:
    """A worker without the limit runs each job once, also the jobs that kept a slot."""
    q = Queries(apgdriver)
    expected = [*await retried_under_limit(q, n_jobs=2), *await q.enqueue(["other"], [None], [0])]
    qm = QueueManager(q)
    ran = list[JobId]()
    drained = asyncio.Event()

    async def run_once(job: Job) -> None:
        ran.append(job.id)
        if len(ran) == len(expected):
            drained.set()

    qm.entrypoint("fetch")(run_once)
    qm.entrypoint("other")(run_once)

    async def stop_when_drained() -> None:
        # The bound only ends a stalled run. A working run sets drained long before.
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(10):
                await drained.wait()
        qm.shutdown.set()

    await asyncio.gather(qm.run(dequeue_timeout=timedelta(hours=1)), stop_when_drained())

    assert sorted(ran) == expected
    assert await q.queue_size() == []
