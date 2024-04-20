import asyncio

import asyncpg
import pytest
from PgQueuer import qm


@pytest.mark.parametrize("N", (1, 2, 32, 500))
async def test_job_queing(pgpool: asyncpg.Pool, N: int) -> None:
    c = qm.QueueManager(pgpool)
    seen = list[int]()

    @c.entrypoint("fetch")
    async def fetch(context: bytes | None) -> None:
        if context == f"{N}".encode():
            c.alive = False
        assert context
        seen.append(int(context))

    for n in range(1, N+1):
        await c.q.put("fetch", f"{n}".encode())

    await asyncio.wait_for(c.run(), timeout=1)
    assert sorted(seen) == list(range(1, N+1))
