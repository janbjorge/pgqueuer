import asyncio
from typing import AsyncGenerator

import asyncpg
import pytest
import uvloop

from pgqueuer.db import AsyncpgDriver, Driver, dsn
from pgqueuer.queries import Queries


@pytest.fixture(scope="session", autouse=True)
def event_loop_policy() -> uvloop.EventLoopPolicy:
    return uvloop.EventLoopPolicy()


@pytest.fixture(scope="function")
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn())
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close()


@pytest.fixture(scope="function", autouse=True)
async def truncate_tables(apgdriver: Driver) -> None:
    await asyncio.gather(
        Queries(apgdriver).clear_queue_log(),
        Queries(apgdriver).clear_queue(),
        Queries(apgdriver).clear_schedule(),
        Queries(apgdriver).clear_statistics_log(),
    )
