import asyncio
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable, Generator

import asyncpg
import psycopg
import pytest
from conftest import dsn

from pgqueuer.db import AsyncpgDriver, Driver, PsycopgDriver
from pgqueuer.helpers import perf_counter_dt
from pgqueuer.listeners import initialize_notice_event_listener
from pgqueuer.models import PGChannel, TableChangedEvent
from pgqueuer.queries import QueryBuilder


@asynccontextmanager
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    conn = await asyncpg.connect(dsn=dsn(), timeout=5)
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close(timeout=5)


@asynccontextmanager
async def psydriver() -> AsyncGenerator[PsycopgDriver, None]:
    conn = await psycopg.AsyncConnection.connect(
        conninfo=dsn(),
        autocommit=True,
    )
    try:
        yield PsycopgDriver(conn)
    finally:
        await conn.close()


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
    query = "SELECT pg_notify($1, $2);"
    await driver.execute(query, channel, payload)


@pytest.mark.parametrize("driver", drivers())
async def test_fetch(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        assert list(await d.fetch("SELECT 1 as one, 2 as two")) == [{"one": 1, "two": 2}]


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
        async with driver() as ad:
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
                assert "query parameter missing" in str(exc)
        else:
            raise NotADirectoryError(d)


@pytest.mark.parametrize("driver", drivers())
async def test_event_listener(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
        name = d.__class__.__name__.lower()
        channel = PGChannel(f"test_event_listener_{name}")
        payload = TableChangedEvent(
            channel=channel,
            operation="update",
            sent_at=perf_counter_dt(),
            table="foo",
            type="table_changed_event",
        )

        listener = await initialize_notice_event_listener(
            d,
            channel,
            {},
            {},
        )

        # Seems psycopg does not pick up on
        # notifiys sent from its current connection.
        # Workaround by using asyncpg.
        async with driver() as dd:
            await notify(dd, channel, payload.model_dump_json())

        assert (await asyncio.wait_for(listener.get(), timeout=1)) == payload
