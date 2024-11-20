import asyncio
from typing import AsyncGenerator

import asyncpg
import pytest

from pgqueuer.db import AsyncpgDriver, Driver, dsn
from pgqueuer.queries import Queries


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
        Queries(apgdriver).clear_log(),
        Queries(apgdriver).clear_queue(),
        Queries(apgdriver).clear_schedule(),
    )
