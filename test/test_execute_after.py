from __future__ import annotations

import asyncio
import time
import uuid
from datetime import timedelta

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.db import Driver
from pgqueuer.domain.models import Job
from pgqueuer.queries import EntrypointExecutionParameter, Queries


async def test_execute_after_default_is_now(apgdriver: Driver) -> None:
    await Queries(apgdriver).enqueue("foo", None, 0, None)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
                global_concurrency_limit=1000,
            )
        )
        == 1
    )

    await Queries(apgdriver).enqueue("foo", None, 0)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
                global_concurrency_limit=1000,
            )
        )
        == 1
    )


async def test_execute_after_zero(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=0)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
                global_concurrency_limit=1000,
            )
        )
        == 1
    )


async def test_execute_after_negative(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=-10)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    assert (
        len(
            await Queries(apgdriver).dequeue(
                10,
                {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
                uuid.uuid4(),
                global_concurrency_limit=1000,
            )
        )
        == 1
    )


async def test_execute_after_1_second(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=1)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    before = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
        global_concurrency_limit=1000,
    )
    assert len(before) == 0

    await asyncio.sleep(execute_after.total_seconds())
    after = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
        global_concurrency_limit=1000,
    )
    assert len(after) == 1


async def test_execute_after_updated_gt_execute_after(apgdriver: Driver) -> None:
    execute_after = timedelta(seconds=1)
    await Queries(apgdriver).enqueue("foo", None, 0, execute_after)
    await asyncio.sleep(execute_after.total_seconds())
    after = await Queries(apgdriver).dequeue(
        10,
        {"foo": EntrypointExecutionParameter(timedelta(seconds=60), False, 0)},
        uuid.uuid4(),
        global_concurrency_limit=1000,
    )
    assert len(after) == 1
    assert all(x.updated > x.execute_after for x in after)


async def test_next_deferred_eta_returns_none_when_empty(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    eta = await q.next_deferred_eta(["foo"])
    assert eta is None


async def test_next_deferred_eta_returns_none_for_ready_jobs(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    await q.enqueue("foo", None, 0)
    eta = await q.next_deferred_eta(["foo"])
    assert eta is None


async def test_next_deferred_eta_returns_timedelta(apgdriver: Driver) -> None:
    q = Queries(apgdriver)
    await q.enqueue("foo", None, 0, timedelta(seconds=10))
    eta = await q.next_deferred_eta(["foo"])
    assert eta is not None
    assert eta.total_seconds() > 5


async def test_next_deferred_eta_inmemory(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None, 0, timedelta(seconds=10))
    eta = await queries.next_deferred_eta(["ep"])
    assert eta is not None
    assert eta.total_seconds() > 5

    # Unrelated entrypoint should return None
    eta2 = await queries.next_deferred_eta(["other"])
    assert eta2 is None


async def test_deferred_job_picked_up_promptly() -> None:
    """A deferred job should be picked up close to its execute_after time,
    not delayed by a full dequeue_timeout."""
    pq = PgQueuer.in_memory()
    processed: list[float] = []

    @pq.entrypoint("deferred")
    async def handler(job: Job) -> None:
        processed.append(time.monotonic())

    defer_seconds = 1.0
    await pq.qm.queries.enqueue("deferred", None, 0, timedelta(seconds=defer_seconds))

    t0 = time.monotonic()

    async def stop_after_processed() -> None:
        while not processed:
            await asyncio.sleep(0.05)
        pq.qm.shutdown.set()

    await asyncio.gather(
        pq.qm.run(
            dequeue_timeout=timedelta(seconds=30),
            batch_size=10,
            max_concurrent_tasks=100,
        ),
        stop_after_processed(),
    )

    elapsed = processed[0] - t0
    # Should be processed near the defer time (~1s), not near dequeue_timeout (30s).
    # Allow generous slack for CI but assert it's well under the dequeue_timeout.
    assert elapsed < defer_seconds + 3.0, (
        f"Deferred job took {elapsed:.1f}s, expected ~{defer_seconds}s"
    )
