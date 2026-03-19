import asyncio
import inspect
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable

import asyncpg
import psycopg
import pytest

from pgqueuer.db import (
    AsyncpgDriver,
    AsyncpgPoolDriver,
    Driver,
    PsycopgDriver,
    SyncDriver,
    SyncPsycopgDriver,
)
from pgqueuer.helpers import utc_now
from pgqueuer.listeners import (
    PGNoticeEventListener,
    default_event_router,
    initialize_notice_event_listener,
)
from pgqueuer.models import TableChangedEvent
from pgqueuer.qb import (
    DBSettings,
    QueryBuilderEnvironment,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
)
from pgqueuer.types import Channel


def get_user_defined_functions(klass: object) -> list[str]:
    return [
        name
        for name, _ in inspect.getmembers(klass, inspect.isfunction)
        if not name.startswith("__")
    ]


@asynccontextmanager
async def asyncpg_connect(dsn: str) -> AsyncGenerator[asyncpg.Connection, None]:
    conn = await asyncpg.connect(dsn=dsn)
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def apgdriver(dsn: str) -> AsyncGenerator[AsyncpgDriver, None]:
    async with (
        asyncpg_connect(dsn) as conn,
        AsyncpgDriver(conn) as x,
    ):
        yield x


@asynccontextmanager
async def apgpooldriver(dsn: str) -> AsyncGenerator[AsyncpgPoolDriver, None]:
    async with (
        asyncpg.create_pool(dsn=dsn) as pool,
        AsyncpgPoolDriver(pool) as x,
    ):
        yield x


@asynccontextmanager
async def psydriver(dsn: str) -> AsyncGenerator[PsycopgDriver, None]:
    async with (
        await psycopg.AsyncConnection.connect(
            conninfo=dsn,
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
    dsn: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        assert list(await d.fetch("SELECT 1 as one, 2 as two")) == [{"one": 1, "two": 2}]


@pytest.mark.parametrize("driver", drivers())
async def test_execute(
    dsn: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        assert isinstance(await d.execute("SELECT 1 as one, 2 as two;"), str)


@pytest.mark.parametrize("driver", drivers())
async def test_notify(
    dsn: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    event = asyncio.Future[str | bytearray | bytes]()

    async with driver(dsn) as d:
        name = d.__class__.__name__.lower()
        payload = f"hello_from_{name}"
        channel = f"test_notify_{name}"

        await d.add_listener(channel, event.set_result)

        # Send from a separate connection to avoid self-notify edge cases.
        async with driver(dsn) as ad:
            await ad.execute(
                QueryQueueBuilder(
                    DBSettings(channel=channel),
                ).build_notify_query(),
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
    dsn: str,
    query: Callable[..., str],
    name: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    if name == "build_install_query":
        pytest.skip()

    sql = query()
    sql = sql if isinstance(sql, str) else f"\n{'-' * 50}\n".join(x for x in sql)
    assert isinstance(sql, str)

    def rolledback(sql: str) -> str:
        return f"BEGIN; {sql}; ROLLBACK;"

    async with driver(dsn) as d:
        if isinstance(d, AsyncpgDriver | AsyncpgPoolDriver):
            with suppress(asyncpg.exceptions.UndefinedParameterError):
                await d.execute(rolledback(sql))

        elif isinstance(d, PsycopgDriver):
            with suppress(psycopg.errors.UndefinedParameter):
                await d.execute(rolledback(sql))
        else:
            raise NotADirectoryError(d)


@pytest.mark.parametrize("driver", drivers())
async def test_event_listener(
    dsn: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        name = d.__class__.__name__.lower()
        channel = Channel(f"test_event_listener_{name}")
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
            default_event_router(
                notice_event_queue=listener,
                statistics={},
                canceled={},
                pending_health_check={},
            ),
        )

        # Send from a separate connection to avoid self-notify edge cases.
        async with driver(dsn) as dd:
            await dd.execute(
                QueryQueueBuilder(DBSettings(channel=channel)).build_notify_query(),
                payload.model_dump_json(),
            )

        assert (await asyncio.wait_for(listener.get(), timeout=1)) == payload


@pytest.mark.parametrize("driver", drivers())
async def test_recovery_after_failed_sql(
    dsn: str,
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        with pytest.raises(Exception):
            await d.execute("SELECT 1 WHERE")

        result = await d.fetch("SELECT 1 as one")
        assert result == [{"one": 1}]

        with pytest.raises(Exception):
            await d.fetch("SELECT 1 WHERE")

        result = await d.fetch("SELECT 2 as two")
        assert result == [{"two": 2}]


async def test_recovery_after_failed_sql_sync(
    pgdriver: SyncDriver,
) -> None:
    with pytest.raises(Exception):
        pgdriver.fetch("SELECT 1 WHERE")

    result = pgdriver.fetch("SELECT 1 as one")
    assert result == [{"one": 1}]


async def test_no_autocommit_raises(dsn: str) -> None:
    with pytest.raises(RuntimeError):
        SyncPsycopgDriver(psycopg.connect(dsn))

    with pytest.raises(RuntimeError):
        async with await psycopg.AsyncConnection.connect(conninfo=dsn) as conn:
            PsycopgDriver(conn)
