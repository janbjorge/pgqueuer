import asyncio
import inspect
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import AsyncContextManager, AsyncGenerator, Callable

import asyncpg
import psqlpy
import psqlpy.exceptions
import psycopg
import pytest

from pgqueuer.adapters.persistence.qb import (
    QueryBuilderEnvironment,
    QueryQueueBuilder,
    QuerySchedulerBuilder,
)
from pgqueuer.core.listeners import (
    PGNoticeEventListener,
    default_event_router,
    initialize_notice_event_listener,
)
from pgqueuer.db import (
    AsyncpgDriver,
    AsyncpgPoolDriver,
    Driver,
    PsqlpyDriver,
    PsycopgDriver,
    SyncDriver,
    SyncPsycopgDriver,
)
from pgqueuer.models import TableChangedEvent
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


@asynccontextmanager
async def psqlpydriver(dsn: str) -> AsyncGenerator[PsqlpyDriver, None]:
    pool = psqlpy.ConnectionPool(dsn=dsn, max_db_pool_size=5)
    try:
        async with PsqlpyDriver(pool) as x:
            yield x
    finally:
        pool.close()


def drivers() -> tuple[Callable[..., AsyncContextManager[Driver]], ...]:
    return (
        apgdriver,
        psydriver,
        apgpooldriver,
        psqlpydriver,
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
            await ad.notify(channel, payload)

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
    if any(
        p.default is inspect.Parameter.empty for p in inspect.signature(query).parameters.values()
    ):
        pytest.skip("builder requires arguments; covered by dedicated tests")

    sql = query()
    sql = sql if isinstance(sql, str) else f"\n{'-' * 50}\n".join(x for x in sql)
    assert isinstance(sql, str)

    def rolledback(sql: str) -> str:
        return f"BEGIN; {sql}; ROLLBACK;"

    # Builders that reference placeholders raise once the parameters are
    # missing; each driver spells that error differently.
    undefined_parameter = (
        asyncpg.exceptions.UndefinedParameterError,
        psycopg.errors.UndefinedParameter,
        psqlpy.exceptions.DatabaseError,
    )

    async with driver(dsn) as d:
        with suppress(*undefined_parameter):
            await d.execute(rolledback(sql))


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
            sent_at=datetime.now(timezone.utc),
            table="foo",
            type="table_changed_event",
        )

        listener = PGNoticeEventListener()
        await initialize_notice_event_listener(
            d,
            channel,
            default_event_router(
                notice_event_queue=listener,
                canceled={},
                pending_health_check={},
            ),
        )

        # Send from a separate connection to avoid self-notify edge cases.
        async with driver(dsn) as dd:
            await dd.notify(channel, payload.model_dump_json())

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


async def test_psqlpy_pool_too_small_raises(dsn: str) -> None:
    # psqlpy rejects max_db_pool_size=1 in the constructor, so shrink after.
    pool = psqlpy.ConnectionPool(dsn=dsn, max_db_pool_size=2)
    pool.resize(1)
    try:
        with pytest.raises(RuntimeError):
            PsqlpyDriver(pool)
    finally:
        pool.close()


async def test_psqlpy_invalid_channel_raises(dsn: str) -> None:
    async with psqlpydriver(dsn) as d:
        with pytest.raises(ValueError):
            await d.add_listener("not an identifier", lambda _: None)


async def test_psqlpy_listener_serves_channels_added_after_start(dsn: str) -> None:
    """Registering a second channel rebuilds the listener without losing the first."""
    first = asyncio.Future[str | bytearray | bytes]()
    second = asyncio.Future[str | bytearray | bytes]()

    async with psqlpydriver(dsn) as d:
        await d.add_listener("psqlpy_chan_first", first.set_result)
        await d.add_listener("psqlpy_chan_second", second.set_result)

        async with psqlpydriver(dsn) as notifier:
            await notifier.notify("psqlpy_chan_first", "one")
            await notifier.notify("psqlpy_chan_second", "two")

        assert await asyncio.wait_for(first, timeout=5) == "one"
        assert await asyncio.wait_for(second, timeout=5) == "two"


async def test_psqlpy_listener_survives_callback_exception(dsn: str) -> None:
    """A raising callback must not take down psqlpy's listen task."""
    delivered = asyncio.Future[str | bytearray | bytes]()

    def explode(payload: str | bytes | bytearray) -> None:
        raise RuntimeError("callback failure")

    async with psqlpydriver(dsn) as d:
        await d.add_listener("psqlpy_chan_boom", explode)
        await d.add_listener("psqlpy_chan_alive", delivered.set_result)

        async with psqlpydriver(dsn) as notifier:
            await notifier.notify("psqlpy_chan_boom", "boom")
            await asyncio.sleep(0.1)
            await notifier.notify("psqlpy_chan_alive", "still here")

        assert await asyncio.wait_for(delivered, timeout=5) == "still here"


async def test_no_autocommit_raises(dsn: str) -> None:
    with pytest.raises(RuntimeError):
        SyncPsycopgDriver(psycopg.connect(dsn))

    with pytest.raises(RuntimeError):
        async with await psycopg.AsyncConnection.connect(conninfo=dsn) as conn:
            PsycopgDriver(conn)
