import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import psycopg
import pytest

from pgqueuer import db, errors, models, queries
from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.ports.repository import QueueRepositoryPort

# ---------------------------------------------------------------------------
# Parametrized tests (run against both postgres and inmemory)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(queries: QueueRepositoryPort, N: int) -> None:
    assert sum(x.count for x in await queries.queue_size()) == 0

    for _ in range(N):
        await queries.enqueue("placeholder", None)

    assert sum(x.count for x in await queries.queue_size()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_next_jobs(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    await queries.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    seen = list[int]()
    while jobs := await queries.dequeue(
        batch_size=10,
        entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in jobs:
            payoad = job.payload
            assert payoad is not None
            seen.append(int(payoad))
            await queries.log_jobs([(job, "successful", None)])

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 64))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    queries: QueueRepositoryPort,
    N: int,
    concurrency: int,
) -> None:
    await queries.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    seen = list[int]()

    async def consumer() -> None:
        while jobs := await queries.dequeue(
            entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 1)},
            batch_size=10,
            queue_manager_id=uuid.uuid4(),
            global_concurrency_limit=1000,
        ):
            for job in jobs:
                payload = job.payload
                assert payload is not None
                seen.append(int(payload))
                await queries.log_jobs([(job, "successful", None)])

    await asyncio.wait_for(
        asyncio.gather(*[consumer() for _ in range(concurrency)]),
        10,
    )

    assert sorted(seen) == list(range(N))


async def test_queries_clear(queries: QueueRepositoryPort) -> None:
    await queries.clear_queue()
    assert sum(x.count for x in await queries.queue_size()) == 0

    await queries.enqueue("placeholder", None)
    assert sum(x.count for x in await queries.queue_size()) == 1

    await queries.clear_queue()
    assert sum(x.count for x in await queries.queue_size()) == 0


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_move_job_log(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    await queries.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    while jobs := await queries.dequeue(
        batch_size=10,
        entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in jobs:
            await queries.log_jobs([(job, "successful", None)])

    assert sum(x.status == "successful" for x in await queries.queue_log()) == N


@pytest.mark.parametrize("N", (1, 2, 5))
async def test_clear_queue(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    # Test delete all by listing all
    await queries.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await queries.queue_size())
    assert sum(x.count for x in await queries.queue_size()) == N
    await queries.clear_queue([f"placeholder{n}" for n in range(N)])
    assert sum(x.count for x in await queries.queue_size()) == 0

    # Test delete all by None
    await queries.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await queries.queue_size())
    assert sum(x.count for x in await queries.queue_size()) == N
    await queries.clear_queue(None)
    assert sum(x.count for x in await queries.queue_size()) == 0
    assert sum(x.status == "deleted" for x in await queries.queue_log()) == N
    assert (
        sum(x.count for x in await queries.log_statistics(tail=None) if x.status == "deleted") == N
    )
    assert sum(x.status == "deleted" for x in await queries.queue_log()) == N

    # Test delete one(1).
    await queries.enqueue(
        [f"placeholder{n}" for n in range(N)],
        [None] * N,
        [0] * N,
    )

    assert all(x.count == 1 for x in await queries.queue_size())
    assert sum(x.count for x in await queries.queue_size()) == N
    await queries.clear_queue("placeholder0")
    assert sum(x.count for x in await queries.queue_size()) == N - 1
    assert sum(x.status == "deleted" for x in await queries.queue_log()) == N + 1


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queue_priority(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    jobs = list[models.Job]()

    await queries.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        list(range(N)),
    )

    while next_jobs := await queries.dequeue(
        entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        for job in next_jobs:
            jobs.append(job)
            await queries.log_jobs([(job, "successful", None)])

    assert jobs == sorted(jobs, key=lambda x: x.priority, reverse=True)


@pytest.mark.parametrize("N", (1, 3, 15))
async def test_queue_log_queued_picked_successful(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    # Test delete all by listing all
    await queries.enqueue(
        ["test_queue_log_queued_picked_successful"] * N,
        [None] * N,
        [0] * N,
    )
    assert sum(x.status == "queued" for x in await queries.queue_log()) == N

    # Pick all jobs
    picked_jobs = []
    queue_manager_id = uuid.uuid4()
    while jobs := await queries.dequeue(
        batch_size=10,
        entrypoints={
            "test_queue_log_queued_picked_successful": EntrypointExecutionParameter(
                timedelta(days=1), False, 0
            )
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
    ):
        picked_jobs.extend(jobs)

    assert sum(x.status == "picked" for x in await queries.queue_log()) == N

    for job in picked_jobs:
        await queries.log_jobs([(job, "successful", None)])

    assert sum(x.status == "successful" for x in await queries.queue_log()) == N


@pytest.mark.parametrize("N", (1, 3, 15))
async def test_queue_log_queued_picked_exception(
    queries: QueueRepositoryPort,
    N: int,
) -> None:
    # Test delete all by listing all
    await queries.enqueue(
        ["test_queue_log_queued_picked_exception"] * N,
        [None] * N,
        [0] * N,
    )
    assert sum(x.status == "queued" for x in await queries.queue_log()) == N

    # Pick all jobs
    picked_jobs = []
    queue_manager_id = uuid.uuid4()
    while jobs := await queries.dequeue(
        batch_size=10,
        entrypoints={
            "test_queue_log_queued_picked_exception": EntrypointExecutionParameter(
                timedelta(days=1), False, 0
            )
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
    ):
        picked_jobs.extend(jobs)

    assert sum(x.status == "picked" for x in await queries.queue_log()) == N

    for job in picked_jobs:
        await queries.log_jobs([(job, "exception", None)])

    assert sum(x.status == "exception" for x in await queries.queue_log()) == N


async def test_queue_log_queued_dedupe_key_raises(
    queries: QueueRepositoryPort,
) -> None:
    await queries.enqueue(
        "test_queue_log_queued_dedupe_key_raises",
        None,
        dedupe_key="test_queue_log_queued_dedupe_key_raises",
    )
    assert sum(x.count for x in await queries.queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError):
        await queries.enqueue(
            "test_queue_log_queued_dedupe_key_raises",
            None,
            dedupe_key="test_queue_log_queued_dedupe_key_raises",
        )
    assert sum(x.count for x in await queries.queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_array(
    queries: QueueRepositoryPort,
) -> None:
    await queries.enqueue(
        ["test_queue_log_queued_dedupe_key_raises_array"],
        [None],
        [0],
        dedupe_key=["test_queue_log_queued_dedupe_key_raises_array"],
    )
    assert sum(x.count for x in await queries.queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError):
        await queries.enqueue(
            ["test_queue_log_queued_dedupe_key_raises_array"],
            [None],
            [0],
            dedupe_key=["test_queue_log_queued_dedupe_key_raises_array"],
        )

    assert sum(x.count for x in await queries.queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_contains_dedupe_key(
    queries: QueueRepositoryPort,
) -> None:
    dedupe_key = "test_queue_log_queued_dedupe_key_raises_contains_dedupe_key"
    await queries.enqueue("...", None, dedupe_key=dedupe_key)
    assert sum(x.count for x in await queries.queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError) as raised:
        await queries.enqueue("...", None, dedupe_key=dedupe_key)
    assert dedupe_key in raised.value.dedupe_key


@pytest.mark.parametrize("tail", (None, 10))
@pytest.mark.parametrize("last", (None, timedelta(minutes=5)))
@pytest.mark.parametrize("N", (2, 10))
async def test_log_statistics(
    queries: QueueRepositoryPort,
    tail: int | None,
    last: timedelta | None,
    N: int,
) -> None:
    # Enqueue jobs
    await queries.enqueue(["placeholder"] * N, [None] * N, [0] * N)

    # Log jobs
    jobs = await queries.dequeue(
        entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        batch_size=N,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    )
    assert len(jobs) == N
    for job in jobs:
        await queries.log_jobs([(job, "successful", None)])

    # Fetch statistics
    stats = await queries.log_statistics(tail=tail, last=last)

    assert sum(x.count for x in stats if x.status == "queued") == N
    assert sum(x.count for x in stats if x.status == "picked") == N
    assert sum(x.count for x in stats if x.status == "successful") == N
    assert sum(x.count for x in stats) == 3 * N


async def test_enqueue_with_headers(queries: QueueRepositoryPort) -> None:
    headers = {"trace": "abc"}
    await queries.enqueue("header_task", None, headers=headers)

    jobs = await queries.dequeue(
        entrypoints={"header_task": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        batch_size=1,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    )

    assert len(jobs) == 1
    assert jobs[0].headers == headers
    await queries.log_jobs([(jobs[0], "successful", None)])


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queue_retry_timer(
    queries: QueueRepositoryPort,
    N: int,
    retry_timer: timedelta = timedelta(seconds=0.1),
) -> None:
    jobs = list[models.Job]()

    await queries.enqueue(
        ["placeholder"] * N,
        [f"{n}".encode() for n in range(N)],
        list(range(N)),
    )

    # Pick all jobs, and mark then as "in progress"
    while _ := await queries.dequeue(
        batch_size=10,
        entrypoints={"placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        ...

    assert (
        len(
            await queries.dequeue(
                batch_size=10,
                entrypoints={
                    "placeholder": EntrypointExecutionParameter(timedelta(days=1), False, 0)
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
    while next_jobs := await queries.dequeue(
        entrypoints={"placeholder": EntrypointExecutionParameter(retry_timer, False, 0)},
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
    ):
        jobs.extend(next_jobs)

    assert len(jobs) == N


# ---------------------------------------------------------------------------
# PostgreSQL-only tests (require driver / DSN / raw SQL / SyncQueries)
# ---------------------------------------------------------------------------


async def test_queue_log_fetches_inserted_rows(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await q.clear_queue_log()

    created = datetime.now(timezone.utc)
    entries = [
        {
            "created": created,
            "job_id": 1,
            "status": "queued",
            "priority": 5,
            "entrypoint": "inserted-entrypoint",
            "traceback": None,
            "aggregated": False,
        },
        {
            "created": created + timedelta(seconds=1),
            "job_id": 2,
            "status": "successful",
            "priority": 7,
            "entrypoint": "inserted-entrypoint",
            "traceback": None,
            "aggregated": False,
        },
    ]

    insert_log_sql = f"""
        INSERT INTO {q.qbq.settings.queue_table_log} (
            created,
            job_id,
            status,
            priority,
            entrypoint,
            traceback,
            aggregated
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    """

    for entry in entries:
        await q.driver.execute(
            insert_log_sql,
            entry["created"],
            entry["job_id"],
            entry["status"],
            entry["priority"],
            entry["entrypoint"],
            entry["traceback"],
            entry["aggregated"],
        )

    logs = await q.queue_log()

    assert sorted(logs, key=lambda log: log.job_id) == sorted(
        [models.Log(**entry) for entry in entries], key=lambda log: log.job_id
    )


async def test_queue_log_queued_dedupe_key_raises_sync(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    aq = queries.Queries(apgdriver)

    await aq.enqueue(
        "test_queue_log_queued_dedupe_key_raises_sync",
        None,
        dedupe_key="test_queue_log_queued_dedupe_key_raises_sync",
    )
    assert sum(x.count for x in await aq.queue_size()) == 1

    sq = queries.SyncQueries(pgdriver)
    with pytest.raises(errors.DuplicateJobError):
        sq.enqueue(
            "test_queue_log_queued_dedupe_key_raises_sync",
            None,
            dedupe_key="test_queue_log_queued_dedupe_key_raises_sync",
        )

    assert sum(x.count for x in sq.queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_array_sync(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    aq = queries.Queries(apgdriver)

    await aq.enqueue(
        ["test_queue_log_queued_dedupe_key_raises_array_sync"],
        [None],
        [0],
        dedupe_key=["test_queue_log_queued_dedupe_key_raises_array_sync"],
    )
    assert sum(x.count for x in await aq.queue_size()) == 1

    sq = queries.SyncQueries(pgdriver)
    with pytest.raises(errors.DuplicateJobError):
        sq.enqueue(
            ["test_queue_log_queued_dedupe_key_raises_array_sync"],
            [None],
            [0],
            dedupe_key=["test_queue_log_queued_dedupe_key_raises_array_sync"],
        )

    assert sum(x.count for x in sq.queue_size()) == 1


async def test_queue_log_queued_dedupe_key_raises_contains_dedupe_key_sync(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    dedupe_key = "test_queue_log_queued_dedupe_key_raises_contains_dedupe_key_sync"
    await queries.Queries(apgdriver).enqueue("...", None, dedupe_key=dedupe_key)
    assert sum(x.count for x in await queries.Queries(apgdriver).queue_size()) == 1

    with pytest.raises(errors.DuplicateJobError) as raised:
        queries.SyncQueries(pgdriver).enqueue("...", None, dedupe_key=dedupe_key)
    assert dedupe_key in raised.value.dedupe_key


@pytest.mark.parametrize("batch_size", (2, 10))
@pytest.mark.parametrize("loop_size", (2, 10))
async def test_queue_log_queued_dedupe_key_raises_concurrent(
    dsn: str,
    batch_size: int,
    loop_size: int,
    apgdriver: db.Driver,
) -> None:
    async with asyncpg.create_pool(dsn=dsn) as pool:
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


async def test_queries_from_asyncpg_connection(dsn: str) -> None:
    """Test creating Queries from an asyncpg connection."""
    connection = await asyncpg.connect(dsn=dsn)
    try:
        q = queries.Queries.from_asyncpg_connection(connection)

        # Verify the instance is properly configured
        assert isinstance(q, queries.Queries)
        assert isinstance(q.driver, db.AsyncpgDriver)

        # Test that it can be used to enqueue jobs
        await q.enqueue("test_job", b"payload")
        queue_sizes = await q.queue_size()
        assert sum(x.count for x in queue_sizes) > 0
    finally:
        await connection.close()


async def test_queries_from_asyncpg_pool(dsn: str) -> None:
    """Test creating Queries from an asyncpg connection pool."""
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=5)
    try:
        q = queries.Queries.from_asyncpg_pool(pool)

        # Verify the instance is properly configured
        assert isinstance(q, queries.Queries)
        assert isinstance(q.driver, db.AsyncpgPoolDriver)

        # Test that it can be used to enqueue jobs
        await q.enqueue("test_job", b"payload")
        queue_sizes = await q.queue_size()
        assert sum(x.count for x in queue_sizes) > 0
    finally:
        await pool.close()


async def test_queries_from_psycopg_connection(dsn: str) -> None:
    """Test creating Queries from a psycopg async connection."""
    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        q = queries.Queries.from_psycopg_connection(connection)

        # Verify the instance is properly configured
        assert isinstance(q, queries.Queries)
        assert isinstance(q.driver, db.PsycopgDriver)

        # Test that it can be used to enqueue jobs
        await q.enqueue("test_job", b"payload")
        queue_sizes = await q.queue_size()
        assert sum(x.count for x in queue_sizes) > 0
    finally:
        await connection.close()
