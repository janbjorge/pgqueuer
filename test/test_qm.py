import asyncio

import asyncpg
import pytest
from PgQueuer.qm import QueueManager
from PgQueuer.queries import PgQueuerQueries


@pytest.mark.parametrize("N", (1, 2, 32, 500))
async def test_job_queing(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    c = QueueManager(pgpool)
    seen = list[int]()

    @c.entrypoint("fetch")
    async def fetch(context: bytes | None) -> None:
        if context is None:
            c.alive = False
            return
        assert context
        seen.append(int(context))

    for n in range(N):
        await c.q.enqueue("fetch", f"{n}".encode())

    # Stop flag
    await c.q.enqueue("fetch", None)

    await asyncio.wait_for(c.run(), timeout=1)
    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 32, 512))
@pytest.mark.parametrize("concurrency", (1, 2, 3, 4))
async def test_job_fetch(
    pgpool: asyncpg.Pool,
    N: int,
    concurrency: int,
) -> None:
    q = PgQueuerQueries(pgpool)
    qmpool = [QueueManager(pgpool) for _ in range(concurrency)]
    seen = list[int]()

    for qm in qmpool:

        @qm.entrypoint("fetch")
        async def fetch(context: bytes | None) -> None:
            if context is None:
                for qm in qmpool:
                    qm.alive = False
                return
            assert context
            seen.append(int(context))

    for n in range(N):
        await q.enqueue("fetch", f"{n}".encode())

    # Stop flag
    await q.enqueue("fetch", None)

    await asyncio.wait_for(
        asyncio.gather(*[qm.run() for qm in qmpool]),
        timeout=10,
    )
    assert sorted(seen) == list(range(N))
