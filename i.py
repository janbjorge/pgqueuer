import asyncpg
import asyncio

from src import queries


async def main() -> None:
    pool = await asyncpg.create_pool(min_size=2)
    q = queries.Queries(pool)
    await q.install()


asyncio.run(main())
