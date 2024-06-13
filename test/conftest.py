import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import pytest
from PgQueuer.db import AsyncpgDriver, Driver


def dsn(
    host: str = "",
    user: str = "",
    pawssword: str = "",
    database: str = "",
    port: str = "",
) -> str:
    host = os.getenv("PGHOST", host or "localhost")
    user = os.getenv("PGUSER", user or "testuser")
    password = os.getenv("PGPASSWORD", pawssword or "testpassword")
    database = os.getenv("PGDATABASE", database or "testdb")
    port = os.getenv("PGPORT", port or "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@pytest.fixture(scope="function")
async def pgdriver() -> AsyncGenerator[Driver, None]:
    database = "tmp_test_db"
    conn_a = await asyncpg.connect(dsn=dsn(database="postgres"))
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
