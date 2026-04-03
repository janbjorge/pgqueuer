import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from pgqueuer import db
from pgqueuer.applications import PgQueuer
from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, Driver, PsycopgDriver
from pgqueuer.models import CronExpressionEntrypoint, Job, Schedule
from pgqueuer.qb import DBSettings
from test.helpers import wait_until_empty_queue


async def test_pgqueuer_delegates_to_queue_manager(apgdriver: db.Driver) -> None:
    """Smoke test: PgQueuer wiring delegates entrypoint registration and job
    processing to the underlying QueueManager correctly."""
    pgq = PgQueuer(apgdriver)
    seen = list[int]()
    N = 5

    @pgq.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        assert context.payload is not None
        seen.append(int(context.payload))

    await pgq.qm.queries.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await asyncio.gather(
        pgq.run(),
        wait_until_empty_queue(pgq.qm.queries, [pgq]),
    )

    assert seen == list(range(N))


async def inspect_schedule(connection: Driver) -> list[Schedule]:
    query = f"SELECT * FROM {DBSettings().schedules_table} ORDER BY id"
    return [Schedule.model_validate(dict(x)) for x in await connection.fetch(query)]


@pytest.fixture
async def scheduler(apgdriver: AsyncpgDriver) -> PgQueuer:
    return PgQueuer(apgdriver)


async def shutdown_scheduler_after(pgq: PgQueuer, delay: timedelta = timedelta(seconds=1)) -> None:
    await asyncio.sleep(delay.total_seconds())
    pgq.shutdown.set()


async def test_scheduler_register(scheduler: PgQueuer) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    scheduler.schedule("sample_task", "1 * * * *")(sample_task)
    assert len(scheduler.sm.registry) == 1
    itr = iter(scheduler.sm.registry.keys())
    key = next(itr)
    assert key.entrypoint == "sample_task"
    assert scheduler.sm.registry[key].parameters.expression == "1 * * * *"

    scheduler.schedule("sample_task", "2 * * * *")(sample_task)
    assert len(scheduler.sm.registry) == 2
    itr = iter(scheduler.sm.registry.keys())
    key = next(itr)
    key = next(itr)
    assert key.entrypoint == "sample_task"
    assert scheduler.sm.registry[key].parameters.expression == "2 * * * *"


async def test_scheduler_register_raises_invalid_expression(scheduler: PgQueuer) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    with pytest.raises(ValueError):
        scheduler.schedule("sample_task", "bla * * * *")(sample_task)


async def test_scheduler_runs_tasks(scheduler: PgQueuer, mocker: Mock) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )
    executed = False

    async def sample_task(schedule: Schedule) -> None:
        nonlocal executed
        executed = True

    scheduler.schedule("sample_task", "* * * * *")(sample_task)

    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_scheduler_after(scheduler),
        ],
    )

    assert executed


async def test_heartbeat_updates(scheduler: PgQueuer, mocker: Mock) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )

    async def sample_task(schedule: Schedule) -> None: ...

    scheduler.schedule("sample_task", "* * * * *")(sample_task)

    before = await inspect_schedule(scheduler.connection)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_scheduler_after(scheduler, timedelta(seconds=1)),
        ],
    )
    after = await inspect_schedule(scheduler.connection)

    assert all(a.heartbeat > b.heartbeat for a, b in zip(after, before))


async def test_schedule_storage_and_retrieval(
    scheduler: PgQueuer,
    mocker: Mock,
) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )
    expression = "* * * * *"
    entrypoint = "db_task"
    received: CronExpressionEntrypoint | None = None

    async def db_task(schedule: Schedule) -> None:
        nonlocal received
        received = CronExpressionEntrypoint(
            entrypoint=schedule.entrypoint,
            expression=schedule.expression,
        )

    scheduler.schedule(entrypoint, expression)(db_task)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_scheduler_after(scheduler, timedelta(seconds=2)),
        ],
    )

    assert received is not None
    assert received.entrypoint == entrypoint
    assert received.expression == expression


async def test_pgqueuer_from_asyncpg_connection(dsn: str) -> None:
    """Test creating PgQueuer from an asyncpg connection."""
    import asyncpg

    connection = await asyncpg.connect(dsn=dsn)
    try:
        pgq = PgQueuer.from_asyncpg_connection(connection)

        # Verify the instance is properly configured
        assert isinstance(pgq, PgQueuer)
        assert isinstance(pgq.connection, AsyncpgDriver)
        assert pgq.qm is not None
        assert pgq.sm is not None
        assert pgq.resources == {}

        # Test that it can be used to enqueue and process jobs
        seen = list[int]()

        @pgq.entrypoint("test_job")
        async def test_job(job: Job) -> None:
            assert job.payload is not None
            seen.append(int(job.payload))

        await pgq.qm.queries.enqueue(
            ["test_job"] * 3,
            [f"{n}".encode() for n in range(3)],
            [0] * 3,
        )

        await asyncio.gather(
            pgq.run(),
            wait_until_empty_queue(pgq.qm.queries, [pgq]),
        )

        assert seen == list(range(3))
    finally:
        await connection.close()


async def test_pgqueuer_from_asyncpg_connection_with_resources(dsn: str) -> None:
    """Test creating PgQueuer from an asyncpg connection with custom resources."""
    import asyncpg

    connection = await asyncpg.connect(dsn=dsn)
    try:
        custom_resources = {"shared_cache": {}, "counter": 0}
        pgq = PgQueuer.from_asyncpg_connection(
            connection,
            resources=custom_resources,
        )

        # Verify resources are passed through
        assert pgq.resources is custom_resources
        assert pgq.resources["shared_cache"] == {}
        assert pgq.resources["counter"] == 0
        assert pgq.qm.resources is custom_resources
    finally:
        await connection.close()


async def test_pgqueuer_from_asyncpg_pool(dsn: str) -> None:
    """Test creating PgQueuer from an asyncpg connection pool."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=5)
    try:
        pgq = PgQueuer.from_asyncpg_pool(pool)

        # Verify the instance is properly configured
        assert isinstance(pgq, PgQueuer)
        assert isinstance(pgq.connection, AsyncpgPoolDriver)
        assert pgq.qm is not None
        assert pgq.sm is not None
        assert pgq.resources == {}

        # Test that it can be used to enqueue and process jobs
        seen = list[int]()

        @pgq.entrypoint("pool_job")
        async def pool_job(job: Job) -> None:
            assert job.payload is not None
            seen.append(int(job.payload))

        await pgq.qm.queries.enqueue(
            ["pool_job"] * 3,
            [f"{n}".encode() for n in range(3)],
            [0] * 3,
        )

        await asyncio.gather(
            pgq.run(),
            wait_until_empty_queue(pgq.qm.queries, [pgq]),
        )

        assert sorted(seen) == list(range(3))
    finally:
        await pool.close()


async def test_pgqueuer_from_asyncpg_pool_with_resources(dsn: str) -> None:
    """Test creating PgQueuer from an asyncpg pool with custom resources."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=5)
    try:
        custom_resources = {"db_pool": pool, "request_count": 0}
        pgq = PgQueuer.from_asyncpg_pool(
            pool,
            resources=custom_resources,
        )

        # Verify resources are passed through
        assert pgq.resources is custom_resources
        assert pgq.resources["db_pool"] is pool
        assert pgq.resources["request_count"] == 0
    finally:
        await pool.close()


async def test_pgqueuer_from_psycopg_connection(dsn: str) -> None:
    """Test creating PgQueuer from a psycopg async connection."""
    import psycopg

    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        pgq = PgQueuer.from_psycopg_connection(connection)

        # Verify the instance is properly configured
        assert isinstance(pgq, PgQueuer)
        assert isinstance(pgq.connection, PsycopgDriver)
        assert pgq.qm is not None
        assert pgq.sm is not None
        assert pgq.resources == {}

        # Test that it can be used to enqueue and process jobs
        seen = list[int]()

        @pgq.entrypoint("psycopg_job")
        async def psycopg_job(job: Job) -> None:
            assert job.payload is not None
            seen.append(int(job.payload))

        await pgq.qm.queries.enqueue(
            ["psycopg_job"] * 3,
            [f"{n}".encode() for n in range(3)],
            [0] * 3,
        )

        await asyncio.gather(
            pgq.run(),
            wait_until_empty_queue(pgq.qm.queries, [pgq]),
        )

        assert seen == list(range(3))
    finally:
        await connection.close()


async def test_pgqueuer_from_psycopg_connection_with_resources(dsn: str) -> None:
    """Test creating PgQueuer from a psycopg connection with custom resources."""
    import psycopg

    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    try:
        custom_resources = {"psycopg_conn": connection, "transaction_count": 0}
        pgq = PgQueuer.from_psycopg_connection(
            connection,
            resources=custom_resources,
        )

        # Verify resources are passed through
        assert pgq.resources is custom_resources
        assert pgq.resources["psycopg_conn"] is connection
        assert pgq.resources["transaction_count"] == 0
        assert pgq.qm.resources is custom_resources
    finally:
        await connection.close()
