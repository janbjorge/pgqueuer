from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import pytest
from PgQueuer.db import AsyncpgDriver, Driver, dsn


@pytest.fixture(scope="function")
async def pgdriver() -> AsyncGenerator[Driver, None]:
    database = "tmp_test_db"
    conn_a = await asyncpg.connect(dsn=dsn())
    async with create_test_database(database, conn_a):
        conn_b = await asyncpg.connect(dsn=dsn(database=database))
        yield AsyncpgDriver(conn_b)
        await conn_b.close()
    await conn_a.close()


@asynccontextmanager
async def create_test_database(
    tmptestdb: str,
    connection: asyncpg.Connection,
) -> AsyncGenerator[None, None]:
    try:
        await connection.execute(f"DROP DATABASE IF EXISTS {tmptestdb} WITH (FORCE);")
        await connection.execute(f"CREATE DATABASE {tmptestdb} TEMPLATE testdb;")
        yield
    finally:
        await connection.execute(f"DROP DATABASE {tmptestdb} WITH (FORCE);")
