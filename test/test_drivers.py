import asyncio
import re
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable, Generator

import asyncpg
import psycopg
import pytest
from conftest import dsn
from PgQueuer.db import AsyncpgDriver, Driver, PsycopgDriver
from PgQueuer.queries import QueryBuilder


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


@pytest.mark.parametrize("driver", drivers())
@pytest.mark.parametrize(
    "query",
    (
        QueryBuilder().create_delete_from_log_query,
        QueryBuilder().create_delete_from_queue_query,
        QueryBuilder().create_dequeue_query,
        QueryBuilder().create_enqueue_query,
        QueryBuilder().create_has_column_query,
        QueryBuilder().create_log_job_query,
        QueryBuilder().create_log_statistics_query,
        QueryBuilder().create_queue_size_query,
        QueryBuilder().create_truncate_log_query,
        QueryBuilder().create_truncate_queue_query,
    ),
)
async def test_valid_query_syntax(
    query: Callable[..., str],
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        if isinstance(d, AsyncpgDriver):
            with suppress(asyncpg.exceptions.UndefinedParameterError):
                await d.execute(query())

        elif isinstance(d, PsycopgDriver):
            try:
                await d.execute(query())
            except psycopg.errors.ProgrammingError as exc:
                assert (
                    re.match(
                        r"the query has \d+ placeholders but \d+ parameters were passed",  # noqa
                        str(exc),
                    )
                    is not None
                )
        else:
            raise NotADirectoryError(d)
