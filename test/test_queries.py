import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import psycopg
import pytest

from pgqueuer import db, errors, models, queries
from pgqueuer.domain.settings import DBSettings


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
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
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
            entrypoints={"placeholder": queries.EntrypointExecutionParameter(1)},
            batch_size=10,
            queue_manager_id=uuid.uuid4(),
            global_concurrency_limit=1000,
            heartbeat_timeout=timedelta(seconds=30),
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
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
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
    assert sum(x.count for x in await q.log_statistics(limit=None) if x.status == "deleted") == N
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
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    ):
        for job in next_jobs:
            jobs.append(job)
            await q.log_jobs([(job, "successful", None)])

    assert jobs == sorted(jobs, key=lambda x: x.priority, reverse=True)


@pytest.mark.parametrize("batch_size", (1, 3, 10))
async def test_queue_priority_across_entrypoints(
    apgdriver: db.Driver,
    batch_size: int,
) -> None:
    """Draining several entrypoints yields one global priority DESC, id ASC order (#668)."""
    q = queries.Queries(apgdriver)

    entrypoints = ["alpha", "beta", "gamma"]
    # Tie priorities across entrypoints so the id tiebreak spans entrypoints.
    enqueue_ep = [entrypoints[n % len(entrypoints)] for n in range(60)]
    enqueue_prio = [n % 5 for n in range(60)]

    await q.enqueue(
        enqueue_ep,
        [f"{n}".encode() for n in range(60)],
        enqueue_prio,
    )

    drained = list[models.Job]()
    while next_jobs := await q.dequeue(
        entrypoints={ep: queries.EntrypointExecutionParameter(0) for ep in entrypoints},
        batch_size=batch_size,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    ):
        assert next_jobs == sorted(next_jobs, key=lambda j: (-j.priority, j.id))
        for job in next_jobs:
            drained.append(job)
            await q.log_jobs([(job, "successful", None)])

    assert len(drained) == 60
    assert len({job.id for job in drained}) == 60
    assert drained == sorted(drained, key=lambda j: (-j.priority, j.id))


@pytest.mark.parametrize("batch_size", (1, 5, 7))
async def test_dequeue_caps_at_batch_size_across_entrypoints(
    apgdriver: db.Driver,
    batch_size: int,
) -> None:
    """A single dequeue returns at most batch_size jobs total across all entrypoints (#668)."""
    q = queries.Queries(apgdriver)

    entrypoints = ["a", "b", "c", "d"]
    per_ep = 5
    enqueue_ep = [ep for ep in entrypoints for _ in range(per_ep)]
    total = len(enqueue_ep)

    await q.enqueue(enqueue_ep, [None] * total, [0] * total)

    picked = await q.dequeue(
        batch_size=batch_size,
        entrypoints={ep: queries.EntrypointExecutionParameter(0) for ep in entrypoints},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    )

    assert len(picked) == min(batch_size, total)


async def test_has_queued_work_existence_contract(apgdriver: db.Driver) -> None:
    """queued_work reports positive when work exists, zero otherwise, ignoring picked rows."""
    q = queries.Queries(apgdriver)

    assert await q.queued_work(["foo"]) == 0

    await q.enqueue(["foo"] * 3, [None] * 3, [0] * 3)

    assert await q.queued_work(["foo"]) > 0
    assert await q.queued_work(["bar"]) == 0

    picked = await q.dequeue(
        batch_size=10,
        entrypoints={"foo": queries.EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(picked) == 3
    assert await q.queued_work(["foo"]) == 0


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
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    ):
        ...

    assert (
        len(
            await q.dequeue(
                batch_size=10,
                entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
                queue_manager_id=uuid.uuid4(),
                global_concurrency_limit=1000,
                heartbeat_timeout=timedelta(seconds=30),
            ),
        )
        == 0
    )

    # Sim. slow entrypoint function.
    await asyncio.sleep(retry_timer.total_seconds())

    # Re-fetch, should get the same number of jobs as queued (N).
    while next_jobs := await q.dequeue(
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        batch_size=10,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=retry_timer,
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
            "test_queue_log_queued_picked_successful": queries.EntrypointExecutionParameter(0)
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    ):
        picked_jobs.extend(jobs)

    assert sum(x.status == "picked" for x in await q.queue_log()) == N

    for job in picked_jobs:
        await q.log_jobs([(job, "successful", None)])

    assert sum(x.status == "successful" for x in await q.queue_log()) == N


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
            "test_queue_log_queued_picked_exception": queries.EntrypointExecutionParameter(0)
        },
        queue_manager_id=queue_manager_id,
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
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


async def test_enqueue_on_conflict_skip_single(
    apgdriver: db.Driver,
    pgdriver: db.SyncDriver,
) -> None:
    aq = queries.Queries(apgdriver)

    (first,) = await aq.enqueue("ep", None, dedupe_key="k", on_conflict="skip")
    assert first is not None
    assert sum(x.count for x in await aq.queue_size()) == 1

    assert await aq.enqueue("ep", None, dedupe_key="k", on_conflict="skip") == [None]
    assert sum(x.count for x in await aq.queue_size()) == 1

    sq = queries.SyncQueries(pgdriver)
    assert sq.enqueue("ep", None, dedupe_key="k", on_conflict="skip") == [None]
    assert sum(x.count for x in sq.queue_size()) == 1


async def fetch_jobs_by_id(driver: db.Driver, ids: list[models.JobId | None]) -> dict:
    sql = f"SELECT id, entrypoint, payload FROM {DBSettings().queue_table} WHERE id = ANY($1)"
    return {r["id"]: r for r in await driver.fetch(sql, [x for x in ids if x is not None])}


async def test_enqueue_on_conflict_skip_batch_preserves_shape(apgdriver: db.Driver) -> None:
    """Skip mode returns one entry per input; ids land on the matching rows."""
    aq = queries.Queries(apgdriver)

    await aq.enqueue("existing", None, dedupe_key="b")

    ids = await aq.enqueue(
        ["ep_0", "ep_1", "ep_2"],
        [b"0", b"1", b"2"],
        [0, 0, 0],
        dedupe_key=["a", "b", "c"],
        on_conflict="skip",
    )
    assert [x is None for x in ids] == [False, True, False]

    rows = await fetch_jobs_by_id(apgdriver, ids)
    for position in (0, 2):
        assert rows[ids[position]]["entrypoint"] == f"ep_{position}"
        assert rows[ids[position]]["payload"] == f"{position}".encode()


async def test_enqueue_on_conflict_skip_interleaved_null_keys(apgdriver: db.Driver) -> None:
    aq = queries.Queries(apgdriver)

    await aq.enqueue("existing", None, dedupe_key="dup")

    ids = await aq.enqueue(
        ["ep_0", "ep_1", "ep_2", "ep_3"],
        [b"0", b"1", b"2", b"3"],
        [0, 0, 0, 0],
        dedupe_key=[None, "dup", None, "new"],
        on_conflict="skip",
    )
    assert [x is None for x in ids] == [False, True, False, False]

    rows = await fetch_jobs_by_id(apgdriver, ids)
    for position in (0, 2, 3):
        assert rows[ids[position]]["entrypoint"] == f"ep_{position}"
        assert rows[ids[position]]["payload"] == f"{position}".encode()


async def test_enqueue_on_conflict_skip_within_batch_duplicate(apgdriver: db.Driver) -> None:
    aq = queries.Queries(apgdriver)

    ids = await aq.enqueue(
        ["ep", "ep"],
        [None, None],
        [0, 0],
        dedupe_key=["k", "k"],
        on_conflict="skip",
    )
    assert ids[0] is not None
    assert ids[1] is None
    assert sum(x.count for x in await aq.queue_size()) == 1


async def test_enqueue_on_conflict_skip_no_log_for_skipped(apgdriver: db.Driver) -> None:
    aq = queries.Queries(apgdriver)

    (first,) = await aq.enqueue("ep", None, dedupe_key="k")
    ids = await aq.enqueue(
        ["ep", "ep"],
        [None, None],
        [0, 0],
        dedupe_key=["k", "other"],
        on_conflict="skip",
    )
    assert ids[0] is None
    second = ids[1]
    assert second is not None

    logged_job_ids = [log.job_id for log in await aq.queue_log() if log.status == "queued"]
    assert sorted(logged_job_ids) == sorted([first, second])


async def test_enqueue_on_conflict_skip_all_new_keys(apgdriver: db.Driver) -> None:
    aq = queries.Queries(apgdriver)

    ids = await aq.enqueue(
        ["ep_a", "ep_b"],
        [None, None],
        [0, 0],
        dedupe_key=["a", "b"],
        on_conflict="skip",
    )
    assert all(x is not None for x in ids)
    assert sum(x.count for x in await aq.queue_size()) == 2


@pytest.mark.parametrize("batch_size", (2, 10))
async def test_enqueue_on_conflict_skip_concurrent(
    dsn: str,
    batch_size: int,
    apgdriver: db.Driver,
) -> None:
    async with asyncpg.create_pool(dsn=dsn) as pool:
        results = await asyncio.gather(
            *[
                queries.Queries(db.AsyncpgPoolDriver(pool)).enqueue(
                    "ep",
                    None,
                    dedupe_key="test_enqueue_on_conflict_skip_concurrent",
                    on_conflict="skip",
                )
                for _ in range(batch_size)
            ],
        )

    inserted = [x for (x,) in results if x is not None]
    assert len(inserted) == 1
    assert sum(x.count for x in await queries.Queries(apgdriver).queue_size()) == 1


@pytest.mark.parametrize("limit", (None, 10))
@pytest.mark.parametrize("last", (None, timedelta(minutes=5)))
@pytest.mark.parametrize("N", (2, 10))
async def test_log_statistics(
    apgdriver: db.Driver,
    limit: int | None,
    last: timedelta | None,
    N: int,
) -> None:
    q = queries.Queries(apgdriver)
    # N = 10

    # Enqueue jobs
    await q.enqueue(["placeholder"] * N, [None] * N, [0] * N)

    # Log jobs
    jobs = await q.dequeue(
        entrypoints={"placeholder": queries.EntrypointExecutionParameter(0)},
        batch_size=N,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert len(jobs) == N
    for job in jobs:
        await q.log_jobs([(job, "successful", None)])

    # Fetch statistics
    stats = await q.log_statistics(limit=limit, last=last)

    assert sum(x.count for x in stats if x.status == "queued") == N
    assert sum(x.count for x in stats if x.status == "picked") == N
    assert sum(x.count for x in stats if x.status == "successful") == N
    assert sum(x.count for x in stats) == 3 * N


async def test_enqueue_with_headers(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    headers = {"trace": "abc"}
    await q.enqueue("header_task", None, headers=headers)

    jobs = await q.dequeue(
        entrypoints={"header_task": queries.EntrypointExecutionParameter(0)},
        batch_size=1,
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    )

    assert len(jobs) == 1
    assert jobs[0].headers == headers
    await q.log_jobs([(jobs[0], "successful", None)])


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
