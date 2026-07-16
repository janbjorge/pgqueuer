import asyncio
import uuid
from datetime import timedelta

import async_timeout
import pytest

from pgqueuer import db
from pgqueuer.domain.errors import FailingListenerError
from pgqueuer.models import Job, Log
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import QueueExecutionMode
from test.helpers import wait_until_empty_queue


@pytest.mark.parametrize("N", (1, 2, 32))
async def test_job_queuing(
    apgdriver: db.Driver,
    N: int,
) -> None:
    c = QueueManager(Queries(apgdriver), resources={"test": "job_queuing"})
    seen = list[int]()

    @c.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        assert context.payload is not None
        assert c.resources["test"] == "job_queuing"
        seen.append(int(context.payload))

    await c.queries.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        c.run(),
        wait_until_empty_queue(c.queries, [c]),
    )

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_job_fetch(
    apgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = Queries(apgdriver)
    qmpool = [
        QueueManager(Queries(apgdriver), resources={"test": "job_fetch"})
        for _ in range(concurrency)
    ]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        async def fetch(context: Job) -> None:
            assert context.payload is not None
            assert qm.resources["test"] == "job_fetch"
            seen.append(int(context.payload))

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        wait_until_empty_queue(q, qmpool),
    )

    assert sorted(seen) == list(range(N))


async def test_pick_local_entrypoints(
    apgdriver: db.Driver,
    N: int = 100,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver), resources={"test": "pick_local"})
    pikced_by = list[str]()

    @qm.entrypoint("to_be_picked")
    async def to_be_picked(job: Job) -> None:
        pikced_by.append(job.entrypoint)
        assert qm.resources["test"] == "pick_local"

    await q.enqueue(["to_be_picked"] * N, [None] * N, [0] * N)
    await q.enqueue(["not_picked"] * N, [None] * N, [0] * N)

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size() if x.entrypoint == "to_be_picked"):
            await asyncio.sleep(0.01)
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert pikced_by == ["to_be_picked"] * N
    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "to_be_picked") == 0
    assert sum(s.count for s in await q.queue_size() if s.entrypoint == "not_picked") == N


async def test_pick_set_queue_manager_id(
    apgdriver: db.Driver,
    N: int = 100,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver), resources={"test": "pick_qm_id"})
    qmids = set[uuid.UUID]()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        assert job.queue_manager_id is not None
        assert qm.resources["test"] == "pick_qm_id"
        qmids.add(job.queue_manager_id)

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async def waiter() -> None:
        while sum(x.count for x in await q.queue_size()):
            await asyncio.sleep(0.01)
        qm.shutdown.set()

    await asyncio.gather(
        qm.run(dequeue_timeout=timedelta(seconds=0.01)),
        waiter(),
    )

    assert len(qmids) == 1


@pytest.mark.parametrize("N", (1, 10, 100))
async def test_drain_mode(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver))
    jobs = list[Job]()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        jobs.append(job)

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await qm.run(mode=QueueExecutionMode.drain)

    assert len(jobs) == N


@pytest.mark.parametrize("N", (1, 10, 100))
async def test_traceback_log(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver))

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        raise ValueError(f"Test error {job.id}")

    jids = await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await qm.run(mode=QueueExecutionMode.drain)

    logs = await q.queue_log()
    assert sum(log.status == "exception" for log in logs) == N
    assert sum(log.traceback is not None for log in logs if log.status == "exception") == N
    assert sum(log.job_id in jids and log.status == "exception" for log in logs) == N


@pytest.mark.parametrize("N", (100, 200))
@pytest.mark.parametrize("max_concurrent_tasks", (40, 80))
async def test_max_concurrent_tasks(
    apgdriver: db.Driver,
    N: int,
    max_concurrent_tasks: int,
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(Queries(apgdriver))
    picked_jobs = list[Log]()

    async def log_sampler() -> None:
        # Poll until the manager has ramped to its concurrency ceiling instead of
        # sampling once at a fixed delay, which races the ramp-up under load. The
        # global limit caps picks, so the count settles at exactly the ceiling.
        async with async_timeout.timeout(5):
            while True:
                logs = [log for log in await q.queue_log() if log.status == "picked"]
                if len(logs) >= max_concurrent_tasks:
                    picked_jobs.extend(logs)
                    break
                await asyncio.sleep(0.05)
        qm.shutdown.set()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        await qm.shutdown.wait()

    await q.enqueue(["fetch"] * N, [None] * N, [0] * N)

    async with async_timeout.timeout(10):
        await asyncio.gather(
            qm.run(max_concurrent_tasks=max_concurrent_tasks),
            log_sampler(),
        )

    assert len(picked_jobs) == max_concurrent_tasks


async def wait_until_listening(qm: QueueManager, timeout_seconds: float = 10.0) -> None:
    """Block until *qm*'s NOTIFY listener answers a health-check round trip."""
    async with async_timeout.timeout(timeout_seconds):
        while True:
            try:
                await qm.listener_healthy(timeout=timedelta(seconds=0.5))
                return
            except FailingListenerError:
                await asyncio.sleep(0.05)


async def test_notification_burst_coalesces_dequeue_cycles(
    apgdriver: db.Driver,
) -> None:
    # A burst of N single-statement inserts emits N notifications. The buffered
    # backlog must collapse into a few dequeue cycles instead of one wake ->
    # dequeue round trip per notification (the thundering-herd replay).
    q = Queries(apgdriver)
    qm = QueueManager(q)
    dequeue_calls = 0
    original_dequeue = q.dequeue

    async def counting_dequeue(*args: object, **kwargs: object) -> list[Job]:
        nonlocal dequeue_calls
        dequeue_calls += 1
        # Slow the query down so the whole burst lands while one dequeue is in
        # flight; a loop that replays the backlog one event at a time would
        # need ~N cycles to consume it, a coalescing loop needs a couple.
        await asyncio.sleep(0.1)
        return await original_dequeue(*args, **kwargs)  # type: ignore[arg-type]

    q.dequeue = counting_dequeue  # type: ignore[method-assign]

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        pass

    async def burst_then_shutdown() -> None:
        await wait_until_listening(qm)
        # Deferred far into the future: notifications fire, nothing is
        # eligible, so every dequeue in this test returns empty.
        for _ in range(20):
            await q.enqueue("fetch", None, execute_after=timedelta(seconds=30))
        await asyncio.sleep(1.5)
        qm.shutdown.set()

    async with async_timeout.timeout(10):
        await asyncio.gather(
            qm.run(dequeue_timeout=timedelta(seconds=5)),
            burst_then_shutdown(),
        )

    assert dequeue_calls <= 5, f"burst was not coalesced: {dequeue_calls} dequeue calls"


async def test_dequeue_jitter_delays_notification_wake(
    apgdriver: db.Driver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pgqueuer.core.qm.random.uniform", lambda a, b: b)

    q = Queries(apgdriver)
    qm = QueueManager(q)
    loop = asyncio.get_running_loop()
    picked_at = list[float]()

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        picked_at.append(loop.time())

    enqueued_at: float = 0.0

    async def enqueue_then_shutdown() -> None:
        nonlocal enqueued_at
        await wait_until_listening(qm)
        # Let the manager finish its startup dequeue and park in the event wait;
        # only notification-driven wakes are jittered.
        await asyncio.sleep(0.2)
        enqueued_at = loop.time()
        await q.enqueue("fetch", None)
        async with async_timeout.timeout(5):
            while not picked_at:
                await asyncio.sleep(0.01)
        qm.shutdown.set()

    async with async_timeout.timeout(15):
        await asyncio.gather(
            qm.run(
                dequeue_timeout=timedelta(seconds=5),
                dequeue_jitter=timedelta(seconds=0.3),
            ),
            enqueue_then_shutdown(),
        )

    assert picked_at[0] - enqueued_at >= 0.25


async def test_shutdown_during_dequeue_jitter(
    apgdriver: db.Driver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pgqueuer.core.qm.random.uniform", lambda a, b: b)

    q = Queries(apgdriver)
    qm = QueueManager(q)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        pass

    async def trigger_then_shutdown() -> None:
        await wait_until_listening(qm)
        await asyncio.sleep(0.2)
        # Wakes the manager into a 10 s jitter sleep; shutdown must interrupt it.
        await q.enqueue("fetch", None)
        await asyncio.sleep(0.3)
        qm.shutdown.set()

    async with async_timeout.timeout(5):
        await asyncio.gather(
            qm.run(
                dequeue_timeout=timedelta(seconds=30),
                dequeue_jitter=timedelta(seconds=10),
            ),
            trigger_then_shutdown(),
        )


async def test_completion_self_wake_not_jittered(
    apgdriver: db.Driver,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pgqueuer.core.qm.random.uniform", lambda a, b: b)

    q = Queries(apgdriver)
    qm = QueueManager(q)

    @qm.entrypoint("fetch")
    async def fetch(job: Job) -> None:
        # Keep the task alive past the drain check so the manager parks in the
        # event wait and gets woken by the completion-log flush self-wake.
        await asyncio.sleep(0.05)

    # Enqueued before run: picked by the startup dequeue, not a notification.
    # The only notification-queue event in this run is the local self-wake
    # emitted after the completion-log flush; jittering it would stall this
    # drain run for the full 10 s and trip the timeout.
    await q.enqueue("fetch", None)

    async with async_timeout.timeout(5):
        await qm.run(
            dequeue_timeout=timedelta(seconds=30),
            mode=QueueExecutionMode.drain,
            dequeue_jitter=timedelta(seconds=10),
        )
