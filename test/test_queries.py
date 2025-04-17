import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest

from pgqueuer import db, errors, models, queries


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(apgdriver: db.Driver, N: int) -> None:
    q = queries.Queries(apgdriver)

    assert sum(x.count for x in await q.queue_size()) == 0

    for _ in range(N):
        await q.enqueue("placeholder", None)

    assert sum(x.count for x in await q.queue_size()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_next_jobs(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)

    await q.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    seen = list[int]()
    while jobs := await q.dequeue(
        batch_size=10,
        entrypoints={
            "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 0)
        },
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in jobs:
            payoad = job.payload
            assert payoad is not None
            seen.append(int(payoad))
            await q.log_jobs([(job, "successful", None)])

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 64))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    apgdriver: db.Driver,
    N: int,
    concurrency: int,
) -> None:
    q = queries.Queries(apgdriver)

    await q.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    seen = list[int]()

    async def consumer() -> None:
        while jobs := await q.dequeue(
            entrypoints={
                "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 1)
            },
            batch_size=10,
            queue_manager_id=uuid.uuid4(),
            global_concurrency_limit=1000,
        ):
            for job in jobs:
                payload = job.payload
                assert payload is not None
                seen.append(int(payload))
                await q.log_jobs([(job, "successful", None)])

    await asyncio.wait_for(
        asyncio.gather(*[consumer() for _ in range(concurrency)]),
        10,
    )

    assert sorted(seen) == list(range(N))


async def test_queries_clear(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await q.clear_queue()
    assert sum(x.count for x in await q.queue_size()) == 0

    await q.enqueue("placeholder", None)
    assert sum(x.count for x in await q.queue_size()) == 1

    await q.clear_queue()
    assert sum(x.count for x in await q.queue_size()) == 0


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_move_job_log(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)

    await q.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    while jobs := await q.dequeue(
        batch_size=10,
        entrypoints={
            "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 0)
        },
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in jobs:
            await q.log_jobs([(job, "successful", None)])

    assert sum(x.status == "successful" for x in await q.queue_log()) == N


@pytest.mark.parametrize("N", (1, 2, 5))
async def test_clear_queue(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)

    # Test delete all by listing all
    await q.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue([f"placeholder{n}" for n in range(N)])
    assert sum(x.count for x in await q.queue_size()) == 0

    # Test delete all by None
    await q.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue(None)
    assert sum(x.count for x in await q.queue_size()) == 0
    assert sum(x.status == "deleted" for x in await q.queue_log()) == N
    assert sum(x.count for x in await q.log_statistics(tail=None) if x.status == "deleted") == N
    assert sum(x.status == "deleted" for x in await q.queue_log()) == N

    # Test delete one(1).
    await q.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue("placeholder0")
    assert sum(x.count for x in await q.queue_size()) == N - 1
    assert sum(x.status == "deleted" for x in await q.queue_log()) == N + 1


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queue_priority(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)
    jobs = list[models.Job]()

    await q.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        list(range(N)),
    )

    while next_jobs := await q.dequeue(
        entrypoints={
            "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 0)
        },
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in next_jobs:
            jobs.append(job)
            await q.log_jobs([(job, "successful", None)])

    assert jobs == sorted(jobs, key=lambda x: x.priority, reverse=True)


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queue_retry_timer(
    apgdriver: db.Driver,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.1),
) -> None:
    q = queries.Queries(apgdriver)
    jobs = list[models.Job]()

    await q.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        list(range(N)),
    )

    # Pick all jobs, and mark then as "in progress"
    while _ := await q.dequeue(
        batch_size=10,
        entrypoints={
            "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 0)
        },
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        ...

    assert (
        len(
            await q.dequeue(
                batch_size=10,
                entrypoints={
                    "placeholder": queries.EntrypointExecutionParameter(timedelta(days=1), False, 0)
                },
                queue_manager_id=uuid.uuid4(),
                global_concurrency_limit=1000,
            ),
        )
        == 0
    )

    # Sim. slow entrypoint function.
    await asyncio.sleep(retry_timer.total_seconds())

    # Re-fetch, should get the same number of jobs as queued (N).
    while next_jobs := await q.dequeue(
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(retry_timer, False, 0)},
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        jobs.extend(next_jobs)

    assert len(jobs) == N


@pytest.mark.parametrize("N", (1, 3, 15))
async def test_queue_log_queued_picked_successful(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)

    # Test delete all by listing all
    await q.enqueue(
        ["test_queue_log_queued_picked_successful"] * N,
        [None] * N,
        [0] * N,
    )
    assert sum(x.status == "queued" for x in await q.queue_log()) == N

    # Pick all jobs
    picked_jobs = []
    queue_manager_id = uuid.uuid4()
    while jobs := await q.dequeue(
        batch_size=10,
        entrypoints={
            "test_queue_log_queued_picked_successful": queries.EntrypointExecutionParameter(
                timedelta(days=1), False, 0
            )
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
    ):
        picked_jobs.extend(jobs)

    assert sum(x.status == "picked" for x in await q.queue_log()) == N

    for job in picked_jobs:
        await q.log_jobs([(job, "successful", None)])

    assert sum(x.status == "successful" for x in await q.queue_log()) == N


@pytest.mark.parametrize("N", (1, 3, 15))
async def test_queue_log_queued_picked_exception(
    apgdriver: db.Driver,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)

    # Test delete all by listing all
    await q.enqueue(
        ["test_queue_log_queued_picked_exception"] * N,
        [None] * N,
        [0] * N,
    )
    assert sum(x.status == "queued" for x in await q.queue_log()) == N

    # Pick all jobs
    picked_jobs = []
    queue_manager_id = uuid.uuid4()
    while jobs := await q.dequeue(
        batch_size=10,
        entrypoints={
            "test_queue_log_queued_picked_exception": queries.EntrypointExecutionParameter(
                timedelta(days=1), False, 0
            )
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
    ):
        picked_jobs.extend(jobs)

    assert sum(x.status == "picked" for x in await q.queue_log()) == N

    for job in picked_jobs:
        await q.log_jobs([(job, "exception", None)])

    assert sum(x.status == "exception" for x in await q.queue_log()) == N


async def test_queue_log_queued_dedupe_key_raises(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    aq = queries.Queries(apgdriver)

    await aq.enqueue(
        "test_queue_log_queued_dedupe_key_raises",
        None,
        dedupe_key="test_queue_log_queued_dedupe_key_raises",
    )
    assert sum(x.count for x in await aq.queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError):
        await aq.enqueue(
            "test_queue_log_queued_dedupe_key_raises",
            None,
            dedupe_key="test_queue_log_queued_dedupe_key_raises",
        )
    assert sum(x.count for x in await aq.queue_size()) == 1

    sq = queries.SyncQueries(pgdriver)
    with pytest.raises(errors.DuplicateJobError):
        sq.enqueue(
            "test_queue_log_queued_dedupe_key_raises",
            None,
            dedupe_key="test_queue_log_queued_dedupe_key_raises",
        )

    assert sum(x.count for x in sq.queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_array(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    aq = queries.Queries(apgdriver)

    await aq.enqueue(
        ["test_queue_log_queued_dedupe_key_raises_array"],
        [None],
        [0],
        dedupe_key=["test_queue_log_queued_dedupe_key_raises_array"],
    )
    assert sum(x.count for x in await aq.queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError):
        await aq.enqueue(
            ["test_queue_log_queued_dedupe_key_raises_array"],
            [None],
            [0],
            dedupe_key=["test_queue_log_queued_dedupe_key_raises_array"],
        )

    assert sum(x.count for x in await aq.queue_size()) == 1

    sq = queries.SyncQueries(pgdriver)
    with pytest.raises(errors.DuplicateJobError):
        sq.enqueue(
            ["test_queue_log_queued_dedupe_key_raises_array"],
            [None],
            [0],
            dedupe_key=["test_queue_log_queued_dedupe_key_raises_array"],
        )

    assert sum(x.count for x in sq.queue_size()) == 1


@pytest.mark.parametrize("batch_size", (2, 10))
@pytest.mark.parametrize("loop_size", (2, 10))
async def test_queue_log_queued_dedupe_key_raises_concurrent(
    batch_size: int,
    loop_size: int,
    apgdriver: db.Driver,
) -> None:
    async with asyncpg.create_pool() as pool:
        for _ in range(loop_size):
            with pytest.raises(errors.DuplicateJobError):
                await asyncio.gather(
                    *[
                        queries.Queries(db.AsyncpgPoolDriver(pool)).enqueue(
                            "test_queue_log_queued_dedupe_key_raises_concurrent",
                            None,
                            dedupe_key="test_queue_log_queued_dedupe_key_raises_concurrent",
                        )
                        for _ in range(batch_size)
                    ],
                )
            assert sum(x.count for x in await queries.Queries(apgdriver).queue_size()) == 1

    assert sum(x.count for x in await queries.Queries(apgdriver).queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_contains_dedupe_key(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    dedupe_key = "test_queue_log_queued_dedupe_key_raises_contains_dedupe_key"
    await queries.Queries(apgdriver).enqueue("...", None, dedupe_key=dedupe_key)
    assert sum(x.count for x in await queries.Queries(apgdriver).queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError) as raised:
        await queries.Queries(apgdriver).enqueue("...", None, dedupe_key=dedupe_key)
    assert dedupe_key in raised.value.dedupe_key

    with pytest.raises(errors.DuplicateJobError) as raised:
        queries.SyncQueries(pgdriver).enqueue("...", None, dedupe_key=dedupe_key)
    assert dedupe_key in raised.value.dedupe_key
