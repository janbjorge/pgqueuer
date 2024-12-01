import asyncio
import functools
import inspect
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable

import asyncpg
import psycopg
import pytest
from conftest import dsn

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, Driver, PsycopgDriver
from pgqueuer.helpers import utc_now
from pgqueuer.listeners import (
    PGNoticeEventListener,
    handle_event_type,
    initialize_notice_event_listener,
)
from pgqueuer.models import TableChangedEvent
from pgqueuer.qb import (
    DBSettings,
    QueryBuilderEnvironment,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
)
from pgqueuer.types import PGChannel


def get_user_defined_functions(klass: object) -> list[str]:
    return [
        name
        for name, _ in inspect.getmembers(klass, inspect.isfunction)
        if not name.startswith("__")
    ]


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
                QueryQueueBuilder(
                    DBSettings(channel=channel),
                ).create_notify_query(),
                payload,
            )

        assert await asyncio.wait_for(event, timeout=1) == payload


@pytest.mark.parametrize("driver", drivers())
@pytest.mark.parametrize(
    "query, name",
    (
        [
            (getattr(QueryQueueBuilder(), name), name)
            for name in get_user_defined_functions(QueryQueueBuilder)
        ]
        + [
            (getattr(QueryBuilderEnvironment(), name), name)
            for name in get_user_defined_functions(QueryBuilderEnvironment)
        ]
        + [
            (getattr(QuerySchedulerBuilder(), name), name)
            for name in get_user_defined_functions(QuerySchedulerBuilder)
        ]
    ),
)
async def test_valid_query_syntax(
    query: Callable[..., str],
    name: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    if name == "create_install_query":
        pytest.skip()

    sql = query()
    sql = sql if isinstance(sql, str) else f"\n{'-'*50}\n".join(x for x in sql)
    assert isinstance(sql, str)

    def rolledback(sql: str) -> str:
        return f"BEGIN; {sql}; ROLLBACK;"

    async with driver() as d:
        if isinstance(d, AsyncpgDriver | AsyncpgPoolDriver):
            with suppress(asyncpg.exceptions.UndefinedParameterError):
                await d.execute(rolledback(sql))

        elif isinstance(d, PsycopgDriver):
            try:
                await d.execute(rolledback(sql))
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
            sent_at=utc_now(),
            table="foo",
            type="table_changed_event",
        )

        listener = PGNoticeEventListener()
        await initialize_notice_event_listener(
            d,
            channel,
            functools.partial(
                handle_event_type,
                notice_event_queue=listener,
                statistics={},
                canceled={},
            ),
        )

        # Seems psycopg does not pick up on
        # notifiys sent from its current connection.
        # Workaround by using asyncpg.
        async with driver() as dd:
            await dd.execute(
                QueryQueueBuilder(
                    DBSettings(channel=channel),
                ).create_notify_query(),
                payload.model_dump_json(),
            )

        assert (await asyncio.wait_for(listener.get(), timeout=1)) == payload
