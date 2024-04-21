import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import pytest


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
async def pgpool() -> AsyncGenerator[asyncpg.Pool, None]:
    database = "tmp_test_db"
    async with (
        asyncpg.create_pool(dsn=dsn(database="postgres")) as p1,
        create_test_database(database, p1),
        asyncpg.create_pool(dsn(database=database), max_size=40) as p2,
    ):
        yield p2


@asynccontextmanager
async def create_test_database(
    tmptestdb: str,
    pool: asyncpg.Pool,
) -> AsyncGenerator[None, None]:
    try:
        await pool.execute(f"DROP DATABASE IF EXISTS {tmptestdb} WITH (FORCE);")
        await pool.execute(f"CREATE DATABASE {tmptestdb} TEMPLATE testdb;")
        yield
    finally:
        await pool.execute(f"DROP DATABASE {tmptestdb} WITH (FORCE);")
