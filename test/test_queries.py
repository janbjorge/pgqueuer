import asyncio

import asyncpg
import pytest
from PgQueuer import models, queries


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(pgpool: asyncpg.Pool, N: int) -> None:
    q = queries.Queries(pgpool)

    assert sum(x.count for x in await q.queue_size()) == 0

    for _ in range(N):
        await q.enqueue("placeholer", None)

    assert sum(x.count for x in await q.queue_size()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_next_jobs(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()
    while job := await q.dequeue():
        payoad = job.payload
        assert payoad is not None
        seen.append(int(payoad))
        await q.log_job(job, "successful")

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 64, 256))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    pgpool: asyncpg.Pool,
    N: int,
    concurrency: int,
) -> None:
    assert pgpool.get_max_size() >= concurrency
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()

    async def consumer() -> None:
        while job := await q.dequeue():
            payload = job.payload
            assert payload is not None
            seen.append(int(payload))
            await q.log_job(job, "successful")

    await asyncio.wait_for(
        asyncio.gather(*[consumer() for _ in range(concurrency)]),
        10,
    )

    assert sorted(seen) == list(range(N))


async def test_queries_clear(pgpool: asyncpg.Pool) -> None:
    q = queries.Queries(pgpool)
    await q.clear_queue()
    assert sum(x.count for x in await q.queue_size()) == 0

    await q.enqueue("placeholer", None)
    assert sum(x.count for x in await q.queue_size()) == 1

    await q.clear_queue()
    assert sum(x.count for x in await q.queue_size()) == 0


@pytest.mark.parametrize("N", (1, 2, 64, 256))
async def test_move_job_log(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    while next_job := await q.dequeue():
        await q.log_job(next_job, status="successful")

    assert sum(x.count for x in await q.log_size()) == N


@pytest.mark.parametrize("N", (1, 2, 5))
async def test_clear_queue(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)

    # Test delete all by listing all
    for n in range(N):
        await q.enqueue(f"placeholer{n}", None)

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue([f"placeholer{n}" for n in range(N)])
    assert sum(x.count for x in await q.queue_size()) == 0

    # Test delete all by None
    for n in range(N):
        await q.enqueue(f"placeholer{n}", None)

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue(None)
    assert sum(x.count for x in await q.queue_size()) == 0

    # Test delete one(1).
    for n in range(N):
        await q.enqueue(f"placeholer{n}", None)

    assert all(x.count == 1 for x in await q.queue_size())
    assert sum(x.count for x in await q.queue_size()) == N
    await q.clear_queue("placeholer0")
    assert sum(x.count for x in await q.queue_size()) == N - 1


@pytest.mark.parametrize("N", (1, 2, 64, 256))
async def test_queue_priority(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)
    jobs = list[models.Job]()

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode(), priority=n)

    while next_job := await q.dequeue():
        jobs.append(next_job)
        await q.log_job(next_job, status="successful")

    assert jobs == sorted(jobs, key=lambda x: x.priority, reverse=True)
