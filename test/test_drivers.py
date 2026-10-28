import asyncio
import inspect
import uuid
from collections.abc import AsyncGenerator, Callable, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta

import asyncpg
import psycopg
import pytest

from pgqueuer.adapters.persistence.qb import (
    DBSettings,
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
    PsycopgDriver,
    SyncDriver,
    SyncPsycopgDriver,
)
from pgqueuer.models import TableChangedEvent, TracebackRecord
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries
from pgqueuer.types import Channel, QueueEntrypoint, QueueManagerId


def get_user_defined_functions(klass: object) -> list[str]:
    return [
        name
        for name, _ in inspect.getmembers(klass, inspect.isfunction)
        if name.startswith("build_")
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


def drivers() -> tuple[Callable[..., AbstractAsyncContextManager[Driver]], ...]:
    return (
        apgdriver,
        psydriver,
        apgpooldriver,
    )


@pytest.mark.parametrize("driver", drivers())
async def test_fetch(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        assert list(await d.fetch("SELECT 1 as one, 2 as two")) == [{"one": 1, "two": 2}]


@pytest.mark.parametrize("driver", drivers())
async def test_execute(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        assert isinstance(await d.execute("SELECT 1 as one, 2 as two;"), str)


@pytest.mark.parametrize("driver", drivers())
async def test_notify(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
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
@pytest.mark.parametrize("channel", ["MyChan", "my-app.events", "'; DROP TABLE x --"])
async def test_notify_channel_needs_quoting(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
    channel: str,
) -> None:
    """LISTEN must quote the channel so it matches pg_notify's string literal (#947).

    Unquoted, MyChan folds to mychan and never receives, and the other two were
    rejected by the psycopg driver outright.
    """
    event = asyncio.Future[str | bytearray | bytes]()

    async with driver(dsn) as d:
        await d.add_listener(channel, event.set_result)
        async with driver(dsn) as ad:
            await ad.notify(channel, "hello")
        assert await asyncio.wait_for(event, timeout=1) == "hello"


@pytest.mark.parametrize("driver", drivers())
@pytest.mark.parametrize(
    "query, name",
    (
        [
            (getattr(QueryQueueBuilder(DBSettings()), name), name)
            for name in get_user_defined_functions(QueryQueueBuilder)
        ]
        + [
            (getattr(QueryBuilderEnvironment(DBSettings()), name), name)
            for name in get_user_defined_functions(QueryBuilderEnvironment)
        ]
        + [
            (getattr(QuerySchedulerBuilder(DBSettings()), name), name)
            for name in get_user_defined_functions(QuerySchedulerBuilder)
        ]
    ),
)
async def test_valid_query_syntax(
    dsn: str,
    query: Callable[..., str | Iterable[str]],
    name: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    if name == "build_install_query":
        pytest.skip()
    if any(
        p.default is inspect.Parameter.empty for p in inspect.signature(query).parameters.values()
    ):
        pytest.skip("builder requires arguments; covered by dedicated tests")

    sql = query()
    sql = sql if isinstance(sql, str) else f"\n{'-' * 50}\n".join(sql)
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
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        name = d.__class__.__name__.lower()
        channel = Channel(f"test_event_listener_{name}")
        payload = TableChangedEvent(
            channel=channel,
            operation="update",
            sent_at=datetime.now(UTC),
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
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    async with driver(dsn) as d:
        with pytest.raises(Exception, match="syntax error"):
            await d.execute("SELECT 1 WHERE")

        result = await d.fetch("SELECT 1 as one")
        assert result == [{"one": 1}]

        with pytest.raises(Exception, match="syntax error"):
            await d.fetch("SELECT 1 WHERE")

        result = await d.fetch("SELECT 2 as two")
        assert result == [{"two": 2}]


@pytest.mark.parametrize("driver", drivers())
async def test_log_rows_with_a_traceback_read_back(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    """Every log read parses the traceback column, whichever form the driver returns jsonb in."""
    async with driver(dsn) as d:
        q = Queries(d)
        await q.clear_queue()
        await q.clear_queue_log()
        await q.enqueue("traceback_ep", None)
        (job,) = await q.dequeue(
            batch_size=1,
            entrypoints={QueueEntrypoint("traceback_ep"): EntrypointExecutionParameter(0)},
            queue_manager_id=QueueManagerId(uuid.uuid4()),
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )
        record = TracebackRecord.from_exception(ValueError("boom"), job_id=job.id)
        await q.log_jobs([(job, "exception", record)])

        (from_queue_log,) = [log for log in await q.queue_log() if log.traceback is not None]
        (from_exception_logs,) = await q.exception_logs()
        (from_history,) = [
            log for log in await q.job_log_history(job.id) if log.traceback is not None
        ]
        assert from_queue_log.traceback == from_exception_logs.traceback == record
        assert from_history.traceback == record


@pytest.mark.parametrize("driver", drivers())
async def test_a_nul_in_an_exception_message_is_logged(
    dsn: str,
    driver: Callable[..., AbstractAsyncContextManager[Driver]],
) -> None:
    """jsonb refuses a NUL, so the traceback record spells it out instead of failing the write."""
    async with driver(dsn) as d:
        q = Queries(d)
        await q.clear_queue()
        await q.clear_queue_log()
        await q.enqueue(["nul_ep", "nul_ep", "nul_ep"], [None, None, None], [0, 0, 0])
        jobs = await q.dequeue(
            batch_size=3,
            entrypoints={QueueEntrypoint("nul_ep"): EntrypointExecutionParameter(0)},
            queue_manager_id=QueueManagerId(uuid.uuid4()),
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )
        failed, ok, retried = jobs
        context: dict[str, object] = {"reason": "bad\x00line", "nested": ["a\x00b"]}
        # one batch: the record with a NUL must not take the other job's end down with it
        record = TracebackRecord.from_exception(ValueError("a\x00b"), failed.id)
        await q.log_jobs([(failed, "exception", record), (ok, "successful", None)])
        await q.retry_job(
            retried,
            timedelta(0),
            TracebackRecord.from_exception(ValueError("c\x00d"), retried.id, context),
        )

        logs = await q.queue_log()
        assert (ok.id, "successful") in {(log.job_id, log.status) for log in logs}
        logged = {log.job_id: log.traceback for log in logs if log.traceback}
        assert logged[failed.id] is not None
        assert logged[failed.id].exception_message == "a\\x00b"
        assert logged[retried.id] is not None
        assert logged[retried.id].exception_message == "c\\x00d"
        assert logged[retried.id].additional_context == {
            "reason": "bad\\x00line",
            "nested": ["a\\x00b"],
        }


async def test_recovery_after_failed_sql_sync(
    pgdriver: SyncDriver,
) -> None:
    with pytest.raises(Exception, match="syntax error"):
        pgdriver.fetch("SELECT 1 WHERE")

    result = pgdriver.fetch("SELECT 1 as one")
    assert result == [{"one": 1}]


async def test_no_autocommit_raises(dsn: str) -> None:
    with pytest.raises(RuntimeError):
        SyncPsycopgDriver(psycopg.connect(dsn))

    with pytest.raises(RuntimeError):
        async with await psycopg.AsyncConnection.connect(conninfo=dsn) as conn:
            PsycopgDriver(conn)


async def test_a_one_connection_pool_queries_but_cannot_listen(dsn: str) -> None:
    """A listener holds a connection for good, so only listening needs a second one."""
    async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1) as pool:
        driver = AsyncpgPoolDriver(pool)
        assert await driver.fetch("SELECT 1 AS one") == [{"one": 1}]

        with pytest.raises(RuntimeError, match="Pool max size"):
            await driver.add_listener(Channel("ch_one_connection"), lambda _: None)
