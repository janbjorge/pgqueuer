from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any, AsyncGenerator
from urllib.parse import quote, urlparse, urlunparse

import asyncpg
import psycopg
import pytest
import pytest_asyncio

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.queries import Queries

try:  # pragma: no cover - uvloop not installed on Windows
    import uvloop
except ModuleNotFoundError:
    uvloop = None  # type: ignore[assignment]

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pgqueuer.db import SyncPsycopgDriver


class PGQueuerPostgresContainer(DockerContainer):
    """Postgres container with modern wait strategy support
    (libpq env defaults: PGUSER, PGPASSWORD, PGDATABASE, PGPORT)."""

    def __init__(
        self,
        image: str = "postgres:latest",
        port: int = 5432,
        username: str | None = None,
        password: str | None = None,
        dbname: str | None = None,
        *,
        driver: str | None = "psycopg2",
        **kwargs: Any,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.port = port or int(os.environ.get("PGPORT", "5432"))
        self.username = username or os.environ.get("PGUSER", "test")
        self.password = password or os.environ.get("PGPASSWORD", "test")
        self.dbname = dbname or os.environ.get("PGDATABASE", "test")
        self.driver_suffix = f"+{driver}" if driver else ""

        self.with_exposed_ports(port)
        self.with_env("POSTGRES_USER", self.username)
        self.with_env("POSTGRES_PASSWORD", self.password)
        self.with_env("POSTGRES_DB", self.dbname)
        self.waiting_for(LogMessageWaitStrategy("database system is ready to accept connections"))

    def get_connection_url(self, host: str | None = None) -> str:
        if self._container is None:
            msg = "container has not been started"
            raise RuntimeError(msg)

        host = host or self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        quoted_password = quote(self.password, safe=" +")
        return (
            f"postgresql{self.driver_suffix}://{self.username}:{quoted_password}"
            f"@{host}:{port}/{self.dbname}"
        )


@pytest.fixture(scope="session", autouse=True)
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    """Provide uvloop if available; fallback to default policy."""
    return asyncio.DefaultEventLoopPolicy() if uvloop is None else uvloop.EventLoopPolicy()


@pytest.fixture(scope="session")
async def postgres_container() -> AsyncGenerator[str, None]:
    if external_postgres_dsn := os.environ.get("EXTERNAL_POSTGRES_DSN"):
        yield external_postgres_dsn
        return

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
        PGQueuerPostgresContainer(f"postgres:{postgres_version}", driver=None)
        .with_command(commands)
        .with_kwargs(tmpfs={"/var/lib/pg/data": "rw"})
        .with_envs(PGDATA="/var/lib/pg/data")
    )

    with container as running:
        yield running.get_connection_url()


@pytest.fixture(scope="session")
async def migrated_db(postgres_container: str) -> AsyncGenerator[str, None]:
    """Ensure the database is migrated before running tests."""

    parent = f"parent_{uuid.uuid4().hex}"

    async with asyncpg.create_pool(dsn=build_dsn_for(postgres_container, "/postgres")) as pool:
        await pool.execute(f"CREATE DATABASE {parent}")

    async with asyncpg.create_pool(dsn=build_dsn_for(postgres_container, f"/{parent}")) as pool:
        await Queries(AsyncpgPoolDriver(pool)).install()

    yield build_dsn_for(postgres_container, f"/{parent}")


def build_dsn_for(base_url: str, path: str) -> str:
    """Build a DSN for the specified path."""
    return urlunparse(urlparse(base_url)._replace(path=path))


@pytest_asyncio.fixture(scope="function")
async def dsn(migrated_db: str) -> AsyncGenerator[str, None]:
    parent = urlparse(migrated_db).path.strip("/")
    child = f"test_{uuid.uuid4().hex}"

    async with asyncpg.create_pool(dsn=build_dsn_for(migrated_db, path="/postgres")) as pool:
        # Create a short-lived test db from the template dbname on the container
        await pool.execute(f"CREATE DATABASE {child} TEMPLATE {parent}")

        yield build_dsn_for(migrated_db, path=f"/{child}")

        # Clean up the test db
        await pool.execute(f'DROP DATABASE IF EXISTS "{child}" WITH (FORCE)')


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
