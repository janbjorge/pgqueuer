import asyncio
import inspect
from contextlib import asynccontextmanager, suppress
from typing import AsyncContextManager, AsyncGenerator, Callable

import asyncpg
import psycopg
import pytest
from conftest import dsn

from pgqueuer.db import (
    AsyncpgDriver,
    AsyncpgPoolDriver,
    Driver,
    PsycopgDriver,
    SyncDriver,
    SyncPsycopgDriver,
    _named_parameter,
    _replace_dollar_named_parameter,
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

        # Seems psycopg does not pick up on
        # notifiys sent from its current connection.
        # Workaround by using asyncpg.
        async with driver() as dd:
            await dd.execute(
                QueryQueueBuilder(DBSettings(channel=channel)).build_notify_query(),
                payload.model_dump_json(),
            )

        assert (await asyncio.wait_for(listener.get(), timeout=1)) == payload


def test_named_parameter_empty() -> None:
    """Test with no arguments."""
    args = ()
    assert _named_parameter(args) == {}


def test_named_parameter_single() -> None:
    """Test with a single argument."""
    args = (42,)
    expected = {"parameter_1": 42}
    assert _named_parameter(args) == expected


def test_named_parameter_multiple() -> None:
    """Test with multiple arguments."""
    args = (42, "test", 3.14)
    expected = {"parameter_1": 42, "parameter_2": "test", "parameter_3": 3.14}
    assert _named_parameter(args) == expected


def test_named_parameter_with_none() -> None:
    """Test with None as an argument."""
    args = (None, "test")
    expected = {"parameter_1": None, "parameter_2": "test"}
    assert _named_parameter(args) == expected


def test_named_parameter_with_special_characters() -> None:
    """Test with special characters in arguments."""
    args = ("@#$", "test", "123")
    expected = {"parameter_1": "@#$", "parameter_2": "test", "parameter_3": "123"}
    assert _named_parameter(args) == expected


def test_named_parameter_with_mixed_types() -> None:
    """Test with mixed types of arguments."""
    args = (42, "test", 3.14, None, True)
    expected = {
        "parameter_1": 42,
        "parameter_2": "test",
        "parameter_3": 3.14,
        "parameter_4": None,
        "parameter_5": True,
    }
    assert _named_parameter(args) == expected


def test_replace_dollar_named_parameter_no_dollars() -> None:
    """Test with a query that has no dollar parameters."""
    query = "SELECT * FROM table WHERE column = 'value';"
    expected = query
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_single() -> None:
    """Test with a query that has a single dollar parameter."""
    query = "SELECT * FROM table WHERE column = $1;"
    expected = "SELECT * FROM table WHERE column = %(parameter_1)s;"
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_multiple() -> None:
    """Test with a query that has multiple dollar parameters."""
    query = "SELECT * FROM table WHERE column1 = $1 AND column2 = $2;"
    expected = "SELECT * FROM table WHERE column1 = %(parameter_1)s AND column2 = %(parameter_2)s;"
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_repeated() -> None:
    """Test with a query that has repeated dollar parameters."""
    query = "SELECT * FROM table WHERE column1 = $1 OR column1 = $1;"
    expected = "SELECT * FROM table WHERE column1 = %(parameter_1)s OR column1 = %(parameter_1)s;"
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_non_sequential() -> None:
    """Test with a query that has non-sequential dollar parameters."""
    query = "SELECT * FROM table WHERE column1 = $1 AND column2 = $3;"
    expected = "SELECT * FROM table WHERE column1 = %(parameter_1)s AND column2 = %(parameter_3)s;"
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_mixed() -> None:
    """Test with a query that has mixed dollar parameters and other text."""
    query = "SELECT $1, column FROM table WHERE column2 = $2 AND column3 = 'value';"
    expected = "SELECT %(parameter_1)s, column FROM table WHERE column2 = %(parameter_2)s AND column3 = 'value';"  # noqa: E501
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_edge_case() -> None:
    """Test with a query that has dollar parameters at the edges."""
    query = "$1 SELECT * FROM table WHERE column = $2;"
    expected = "%(parameter_1)s SELECT * FROM table WHERE column = %(parameter_2)s;"
    assert _replace_dollar_named_parameter(query) == expected


def test_replace_dollar_named_parameter_large_numbers() -> None:
    """Test with a query that has large numbered dollar parameters."""
    query = "SELECT * FROM table WHERE column1 = $10 AND column2 = $20;"
    expected = (
        "SELECT * FROM table WHERE column1 = %(parameter_10)s AND column2 = %(parameter_20)s;"
    )
    assert _replace_dollar_named_parameter(query) == expected


@pytest.mark.parametrize("driver", drivers())
async def test_recovery_after_failed_sql(
    driver: Callable[..., AsyncContextManager[Driver]],
) -> None:
    async with driver() as d:
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


async def test_no_autocommit_raises() -> None:
    with pytest.raises(RuntimeError):
        SyncPsycopgDriver(psycopg.connect(dsn()))

    with pytest.raises(RuntimeError):
        async with await psycopg.AsyncConnection.connect(conninfo=dsn()) as conn:
            PsycopgDriver(conn)
