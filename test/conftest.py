import asyncio
from typing import AsyncGenerator

import asyncpg
import psycopg
import pytest

from pgqueuer.db import AsyncpgDriver, Driver, PsycopgDriver, dsn
from pgqueuer.queries import Queries


@pytest.fixture(scope="function")
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn())
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close()


@pytest.fixture(scope="function")
async def psydriver() -> AsyncGenerator[PsycopgDriver, None]:
    async with await psycopg.AsyncConnection.connect(
        conninfo=dsn(),
        autocommit=True,
    ) as conn:
        yield PsycopgDriver(conn)


@pytest.fixture(scope="function", autouse=True)
async def trucate_tables(apgdriver: Driver) -> None:
    await asyncio.gather(
        Queries(apgdriver).clear_log(),
        Queries(apgdriver).clear_queue(),
    )
