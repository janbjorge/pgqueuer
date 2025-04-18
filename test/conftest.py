import asyncio
from typing import AsyncGenerator

import asyncpg
import psycopg
import pytest
import uvloop

from pgqueuer.db import AsyncpgDriver, SyncPsycopgDriver, dsn
from pgqueuer.queries import Queries


@pytest.fixture(scope="session", autouse=True)
def event_loop_policy() -> uvloop.EventLoopPolicy:
    return uvloop.EventLoopPolicy()


async def clear_all(driver: AsyncpgDriver) -> None:
    await asyncio.gather(
        Queries(driver).clear_queue_log(),
        Queries(driver).clear_queue(),
        Queries(driver).clear_schedule(),
        Queries(driver).clear_statistics_log(),
    )


@pytest.fixture(scope="function")
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn())
    driver = AsyncpgDriver(conn)
    await clear_all(driver)
    try:
        yield driver
    finally:
        await conn.close()


@pytest.fixture(scope="function")
async def pgdriver(apgdriver: AsyncpgDriver) -> AsyncGenerator[SyncPsycopgDriver, None]:
    conn = psycopg.connect(dsn(), autocommit=True)
    try:
        yield SyncPsycopgDriver(conn)
    finally:
        conn.close()
