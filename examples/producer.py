from __future__ import annotations

import sys

import asyncpg
import uvloop

from pgqueuer.queries import Queries


async def main(count: int) -> None:
    connection = await asyncpg.connect()
    queries = Queries.from_asyncpg_connection(connection)
    await queries.enqueue(
        ["fetch"] * count,
        [f"this is from me: {n}".encode() for n in range(1, count + 1)],
        [0] * count,
    )


if __name__ == "__main__":
    count = 1_000 if len(sys.argv) == 1 else int(sys.argv[1])
    uvloop.run(main(count))
