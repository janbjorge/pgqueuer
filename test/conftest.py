from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import pytest

from pgqueuer.db import AsyncpgDriver, Driver, dsn


@pytest.fixture(scope="function")
async def pgdriver() -> AsyncGenerator[Driver, None]:
    database_name = "tmp_test_db"
    async with create_test_database(database_name):
        conn_b = await asyncpg.connect(dsn=dsn(database=database_name))
        yield AsyncpgDriver(conn_b)
        await conn_b.close()


@asynccontextmanager
async def create_test_database(name: str) -> AsyncGenerator[None, None]:
    connection = await asyncpg.connect(dsn=dsn())
    try:
        await connection.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE);")
        await connection.execute(f"CREATE DATABASE {name} TEMPLATE testdb;")
        yield
    finally:
        await connection.execute(f"DROP DATABASE {name} WITH (FORCE);")
        await connection.close()
