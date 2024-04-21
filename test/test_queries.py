import asyncio

import asyncpg
import pytest
from PgQueuer import queries


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(pgpool: asyncpg.Pool, N: int) -> None:
    q = queries.PgQueuerQueries(pgpool)

    assert sum((await q.qsize()).values()) == 0

    for _ in range(N):
        await q.enqueue("placeholer", None)

    assert sum((await q.qsize()).values()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_next_jobs(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.PgQueuerQueries(pgpool)
    ql = queries.PgQueuerLogQueries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()
    while (next_jobs := await q.dequeue()).root:
        for job in next_jobs.root:
            payoad = job.payload
            assert payoad is not None
            seen.append(int(payoad))
            await ql.move_job_log(job, "successful")

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 64, 256))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    pgpool: asyncpg.Pool,
    N: int,
    concurrency: int,
) -> None:
    assert pgpool.get_max_size() >= concurrency
    q = queries.PgQueuerQueries(pgpool)
    ql = queries.PgQueuerLogQueries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()

    async def consumer() -> None:
        while len(seen) < N:
            jobs = (await q.dequeue()).root
            for job in jobs:
                payload = job.payload
                assert payload is not None
                seen.append(int(payload))
                await ql.move_job_log(job, "successful")

    await asyncio.wait_for(
        asyncio.gather(*[consumer() for _ in range(concurrency)]),
        10,
    )

    assert sorted(seen) == list(range(N))


async def test_queries_clear(pgpool: asyncpg.Pool) -> None:
    q = queries.PgQueuerQueries(pgpool)
    await q.clear()
    assert sum((await q.qsize()).values()) == 0

    await q.enqueue("placeholer", None)
    assert sum((await q.qsize()).values()) == 1

    await q.clear()
    assert sum((await q.qsize()).values()) == 0


@pytest.mark.parametrize("N", (1, 2, 64, 256))
async def test_move_job_log(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.PgQueuerQueries(pgpool)
    ql = queries.PgQueuerLogQueries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    while (next_jobs := await q.dequeue()).root:
        for job in next_jobs.root:
            await ql.move_job_log(job, status="successful")

    assert await ql.qsize() == {("successful", "placeholer", 0): N}
