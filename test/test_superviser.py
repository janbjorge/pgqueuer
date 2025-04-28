from __future__ import annotations

import asyncio
import signal
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import ANY, MagicMock, patch

import async_timeout
import pytest

from pgqueuer import AsyncpgDriver, PgQueuer, QueueManager, SchedulerManager, supervisor
from pgqueuer.errors import FailingListenerError
from pgqueuer.models import HealthCheckEvent
from pgqueuer.types import QueueExecutionMode


@pytest.fixture(scope="function")
async def queue_manager(apgdriver: AsyncpgDriver) -> QueueManager:
    """Fixture to instantiate QueueManager."""
    return QueueManager(connection=apgdriver)


@pytest.fixture(scope="function")
async def scheduler_manager(apgdriver: AsyncpgDriver) -> SchedulerManager:
    """Fixture to instantiate SchedulerManager."""
    return SchedulerManager(connection=apgdriver)


@pytest.fixture(scope="function")
async def pg_queuer(apgdriver: AsyncpgDriver) -> PgQueuer:
    """Fixture to instantiate PgQueuer."""
    return PgQueuer(connection=apgdriver)


@pytest.fixture(scope="function")
def shutdown_event() -> asyncio.Event:
    """Fixture to create a shutdown asyncio.Event."""
    return asyncio.Event()


async def test_setup_shutdown_handlers_queue_manager(
    queue_manager: QueueManager,
    shutdown_event: asyncio.Event,
) -> None:
    manager = supervisor.setup_shutdown_handlers(queue_manager, shutdown_event)
    assert manager.shutdown is shutdown_event


async def test_setup_shutdown_handlers_scheduler_manager(
    scheduler_manager: SchedulerManager,
    shutdown_event: asyncio.Event,
) -> None:
    manager = supervisor.setup_shutdown_handlers(scheduler_manager, shutdown_event)
    assert manager.shutdown is shutdown_event


async def test_setup_shutdown_handlers_pg_queuer(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    manager = supervisor.setup_shutdown_handlers(pg_queuer, shutdown_event)
    assert manager.shutdown is shutdown_event
    assert pg_queuer.qm.shutdown is shutdown_event
    assert pg_queuer.sm.shutdown is shutdown_event


async def test_setup_shutdown_handlers_invalid_manager(
    shutdown_event: asyncio.Event,
) -> None:
    class InvalidManager:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported instance type: .*InvalidManager.*"):
        supervisor.setup_shutdown_handlers(InvalidManager(), shutdown_event)  # type: ignore[arg-type]


async def test_setup_signal_handlers(
    shutdown_event: asyncio.Event,
) -> None:
    with patch("pgqueuer.supervisor.asyncio.get_event_loop") as mock_get_event_loop:
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        supervisor.setup_signal_handlers(shutdown_event)

        mock_loop.add_signal_handler.assert_any_call(signal.SIGINT, ANY, signal.SIGINT)
        mock_loop.add_signal_handler.assert_any_call(signal.SIGTERM, ANY, signal.SIGTERM)


async def test_runit_normal_operation(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    @asynccontextmanager
    async def foo():  # type: ignore
        yield pg_queuer

    # Run runit in the background
    runit_task = asyncio.create_task(
        supervisor.runit(
            factory=foo,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=2),
            restart_on_failure=False,
            shutdown=shutdown_event,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )

    # Allow some time for runit to start
    await asyncio.sleep(0.1)

    # Signal shutdown
    shutdown_event.set()

    # Allow some time for runit to handle shutdown
    await asyncio.sleep(0.2)

    # Ensure runit_task is completed
    async with async_timeout.timeout(1):
        await runit_task
    assert runit_task.done()


async def test_runit_restart_on_failure(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    calls = 0

    async def failing_run(*args, **kwargs):  # type: ignore
        nonlocal calls
        calls += 1
        raise Exception("Simulated failure")

    pg_queuer.run = failing_run  # type: ignore

    async def foo():  # type: ignore
        return pg_queuer

    # Run runit in the background
    runit_task = asyncio.create_task(
        supervisor.runit(
            factory=foo,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0.05),
            restart_on_failure=True,
            shutdown=shutdown_event,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )

    # Allow some time for runit to attempt running and failing
    await asyncio.sleep(0.2)

    # Signal shutdown to stop the runit loop
    shutdown_event.set()

    # Ensure runit_task is completed
    async with async_timeout.timeout(1):
        await runit_task
    assert runit_task.done()
    assert calls > 1


async def test_runit_no_restart_on_failure(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    async def failing_run(*args, **kwargs):  # type: ignore
        raise Exception("Simulated failure")

    pg_queuer.run = failing_run  # type: ignore

    @asynccontextmanager
    async def foo():  # type: ignore
        yield pg_queuer

    with pytest.raises(Exception, match="Simulated failure"):
        await supervisor.runit(
            factory=foo,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=2),
            restart_on_failure=False,
            shutdown=shutdown_event,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )


async def test_runit_negative_restart_delay(shutdown_event: asyncio.Event) -> None:
    with pytest.raises(ValueError, match="'restart_delay' must be >= 0"):
        await supervisor.runit(
            factory=...,  # type: ignore
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=-1),
            restart_on_failure=True,
            shutdown=shutdown_event,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )


async def test_run_manager_invalid_manager() -> None:
    class InvalidManager:
        pass

    with pytest.raises(NotImplementedError, match=r"Unsupported instance type: .*InvalidManager.*"):
        await supervisor.run_manager(
            InvalidManager(),  # type: ignore
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )


async def test_run_max_concurrent_tasks_twice_batch_size(
    queue_manager: QueueManager,
) -> None:
    with pytest.raises(
        RuntimeError,
        match=r"max_concurrent_tasks must be at least twice the batch size",
    ):
        await supervisor.run_manager(
            queue_manager,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=10,
            shutdown_on_listener_failure=False,
        )


async def test_shutdown_on_listener_failure(queue_manager: QueueManager) -> None:
    with pytest.raises(FailingListenerError):
        await queue_manager.listener_healthy(timedelta(seconds=0))

    async def mocked_listener_healthy(timeout: timedelta) -> HealthCheckEvent:
        await asyncio.sleep(0.1)
        raise RuntimeError("Mocked")

    queue_manager.listener_healthy = mocked_listener_healthy  # type: ignore
    with pytest.raises(RuntimeError):
        await supervisor.run_manager(
            queue_manager,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=True,
        )
