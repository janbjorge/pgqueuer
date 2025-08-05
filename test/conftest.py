from __future__ import annotations

import asyncio
import os
import uuid
from typing import AsyncGenerator
from urllib.parse import urlparse, urlunparse

import asyncpg
import psycopg
import pytest
import pytest_asyncio

try:  # pragma: no cover - uvloop not installed on Windows
    import uvloop
except ModuleNotFoundError:
    uvloop = None  # type: ignore[assignment]

from testcontainers.postgres import PostgresContainer

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, SyncPsycopgDriver
from pgqueuer.queries import Queries


@pytest.fixture(scope="session", autouse=True)
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    """Provide uvloop if available; fallback to default policy."""
    return asyncio.DefaultEventLoopPolicy() if uvloop is None else uvloop.EventLoopPolicy()


@pytest.fixture(scope="session")
async def postgres_container() -> AsyncGenerator[PostgresContainer, None]:
    postgres_version = os.environ.get("POSTGRES_VERSION", "16")

    # https://postgresqlco.nf/doc/en/param/vacuum_buffer_usage_limit/
    # Assume worst case for backwards compatibility
    commands = [
        "postgres",
        "-c",
        "fsync=off",
        "-c",
        "full_page_writes=off",
        "-c",
        "jit=off",
        "-c",
        "synchronous_commit=off",
        "-c",
        "checkpoint_timeout=30min",
        "-c",
        "max_wal_size=4GB",
        "-c",
        "checkpoint_completion_target=0.9",
        "-c",
        "shared_buffers=256MB",
        "-c",
        "autovacuum_naptime=5s",
    ] + (["-c", "vacuum_buffer_usage_limit=8MB"] if int(postgres_version) >= 16 else [])

    container = (
        PostgresContainer(f"postgres:{postgres_version}", driver=None)
        .with_command(commands)
        .with_kwargs(tmpfs={"/var/lib/pg/data": "rw"})
        .with_envs(PGDATA="/var/lib/pg/data")
        .with_env("POSTGRES_USER", "test")
        .with_env("POSTGRES_PASSWORD", "test")
    )

    with container as running:
        yield running


@pytest.fixture(scope="session")
async def install_pgq(postgres_container: PostgresContainer) -> None:
    async with asyncpg.create_pool(dsn=postgres_container.get_connection_url()) as pool:
        await Queries(AsyncpgPoolDriver(pool)).install()


def build_dsn_for(base_url: str, path: str) -> str:
    """Build a DSN for the specified path."""
    return urlunparse(urlparse(base_url)._replace(path=path))


@pytest_asyncio.fixture()
async def dsn(
    postgres_container: PostgresContainer, install_pgq: None
) -> AsyncGenerator[str, None]:
    test_db_name = f"test_{uuid.uuid4().hex}"

    async with asyncpg.create_pool(
        dsn=build_dsn_for(
            postgres_container.get_connection_url(),
            path="/postgres",
        )
    ) as admin_pool:
        # Create a short-lived test db from the template dbname on the container
        await admin_pool.execute(
            f'CREATE DATABASE "{test_db_name}" TEMPLATE "{postgres_container.dbname}"'
        )

        yield build_dsn_for(
            postgres_container.get_connection_url(),
            path=f"/{test_db_name}",
        )

        # Clean up the test db
        await admin_pool.execute(f'DROP DATABASE IF EXISTS "{test_db_name}" WITH (FORCE)')


@pytest_asyncio.fixture(scope="function")
async def apgdriver(dsn: str) -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn)
    driver = AsyncpgDriver(conn)
    try:
        yield driver
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="function")
async def pgdriver(dsn: str) -> AsyncGenerator[SyncPsycopgDriver, None]:
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        yield SyncPsycopgDriver(conn)
    finally:
        conn.close()
