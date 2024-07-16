from __future__ import annotations

import asyncio
import sys

import asyncpg
from PgQueuer.db import AsyncpgDriver
from PgQueuer.qm import QueueManager


async def main(N: int) -> None:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)
    await qm.queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(N)],
        [0] * N,
    )


if __name__ == "__main__":
    N = 1_000 if len(sys.argv) < 1 else int(sys.argv[1])
    asyncio.run(main(N))
