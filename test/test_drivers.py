import asyncio
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable

import asyncpg
import psycopg
import pytest
from conftest import dsn

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, Driver, PsycopgDriver
from pgqueuer.helpers import perf_counter_dt
from pgqueuer.listeners import initialize_notice_event_listener
from pgqueuer.models import PGChannel, TableChangedEvent
from pgqueuer.queries import DBSettings, QueryBuilder


@asynccontextmanager
async def asyncpg_connect() -> AsyncGenerator[asyncpg.Connection, None]:
    conn = await asyncpg.connect(dsn=dsn())
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def apgdriver() -> AsyncGenerator[AsyncpgDriver, None]:
    async with (
        asyncpg_connect() as conn,
        AsyncpgDriver(conn) as x,
    ):
        yield x


@asynccontextmanager
async def apgpooldriver() -> AsyncGenerator[AsyncpgPoolDriver, None]:
    async with (
        asyncpg.create_pool(dsn=dsn()) as pool,
        AsyncpgPoolDriver(pool) as x,
    ):
        yield x


@asynccontextmanager
async def psydriver() -> AsyncGenerator[PsycopgDriver, None]:
    async with (
        await psycopg.AsyncConnection.connect(
            conninfo=dsn(),
            autocommit=True,
        ) as conn,
        PsycopgDriver(conn) as x,
    ):
        yield x


def drivers() -> tuple[Callable[..., AsyncContextManager[Driver]], ...]:
    return (
        apgdriver,
        psydriver,
        apgpooldriver,
    )


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
            await ad.execute(
                QueryBuilder(DBSettings(channel=channel)).create_notify_query(),
                payload,
            )

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
        if isinstance(d, AsyncpgDriver | AsyncpgPoolDriver):
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
            await dd.execute(
                QueryBuilder(DBSettings(channel=channel)).create_notify_query(),
                payload.model_dump_json(),
            )

        assert (await asyncio.wait_for(listener.get(), timeout=1)) == payload
