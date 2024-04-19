import asyncio
import time

import asyncpg

from src import qm, queries


async def main() -> None:
    pool = await asyncpg.create_pool(min_size=2)
    c = qm.QueueManager(pool, queries.Queries(pool))

    N = 10_000
    N += 1

    for n in range(N):
        await c.q.put("fetch", f"this is from me: {n}".encode())

    # Use function name if alias is unset.
    @c.entrypoint("fetch")
    async def fetch(context: bytes) -> None:
        print(context)

    t0 = time.perf_counter()
    await c.run()
    print("RPS", N / (time.perf_counter() - t0))


if __name__ == "__main__":
    asyncio.run(main())
