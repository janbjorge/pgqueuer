import asyncio
import asyncio.selector_events
from contextlib import asynccontextmanager
from typing import AsyncContextManager, AsyncGenerator, Callable, Generator

import asyncpg
import psycopg
import pytest
from conftest import dsn
from PgQueuer.db import AsyncpgDriver, Driver, PsycopgDriver


@asynccontextmanager
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn())
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close()


@asynccontextmanager
async def psydriver() -> AsyncGenerator[PsycopgDriver, None]:
    async with await psycopg.AsyncConnection.connect(
        conninfo=dsn(),
        autocommit=True,
    ) as conn:
        yield PsycopgDriver(conn)


def drivers() -> (
    Generator[
        Callable[..., AsyncContextManager[Driver]],
        None,
        None,
    ]
):
    yield apgdriver
    yield psydriver


async def notify(
    driver: Driver,
    channel: str,
    payload: str,
) -> None:
    if isinstance(driver, AsyncpgDriver):
        query = "SELECT pg_notify($1, $2);"
    elif isinstance(driver, PsycopgDriver):
        query = "SELECT pg_notify(%s, %s);"
    else:
        raise NotImplementedError(driver)

    await driver.execute(query, channel, payload)


@pytest.mark.parametrize("driver", drivers())
async def test_fetchval(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        assert (await d.fetchval("SELECT 1 as one, 2 as two")) == 1


@pytest.mark.parametrize("driver", drivers())
async def test_fetch(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        assert list(await d.fetch("SELECT 1 as one, 2 as two")) == [(1, 2)]


@pytest.mark.parametrize("driver", drivers())
async def test_execute(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        assert isinstance(await d.execute("SELECT 1 as one, 2 as two;"), str)


@pytest.mark.parametrize("driver", drivers())
async def test_notify(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    event = asyncio.Future[str | bytearray | bytes]()

    async with driver() as d:
        name = d.__class__.__name__.lower()
        payload = f"hello_from_{name}"
        channel = f"test_notify_{name}"

        await d.add_listener(channel, event.set_result)

        # Seems psycopg does not pick up on
        # notifiys sent from its current connection.
        # Workaround by using asyncpg.
        async with apgdriver() as ad:
            await notify(ad, channel, payload)

        assert await asyncio.wait_for(event, timeout=1) == payload
