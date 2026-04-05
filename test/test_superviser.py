from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import asynccontextmanager
from datetime import timedelta
from functools import partial
from unittest.mock import ANY, MagicMock, patch

import async_timeout
import pytest

from pgqueuer import AsyncpgDriver, PgQueuer, QueueManager, SchedulerManager, supervisor
from pgqueuer.errors import FailingListenerError
from pgqueuer.models import HealthCheckEvent
from pgqueuer.types import QueueExecutionMode


@pytest.fixture(scope="function")
async def queue_manager(apgdriver: AsyncpgDriver) -> QueueManager:
    return QueueManager(connection=apgdriver)


@pytest.fixture(scope="function")
async def scheduler_manager(apgdriver: AsyncpgDriver) -> SchedulerManager:
    return SchedulerManager(connection=apgdriver)


@pytest.fixture(scope="function")
async def pg_queuer(apgdriver: AsyncpgDriver) -> PgQueuer:
    return PgQueuer(connection=apgdriver)


@pytest.fixture(scope="function")
def shutdown_event() -> asyncio.Event:
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
    with patch("pgqueuer.adapters.cli.supervisor.asyncio.get_event_loop") as mock_get_event_loop:
        mock_loop = MagicMock()
        mock_get_event_loop.return_value = mock_loop

        supervisor.setup_signal_handlers(shutdown_event)

        mock_loop.add_signal_handler.assert_any_call(signal.SIGINT, ANY, signal.SIGINT)
        mock_loop.add_signal_handler.assert_any_call(signal.SIGTERM, ANY, signal.SIGTERM)


async def test_run_with_partial_function(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    @asynccontextmanager
    async def factory(arg):  # type: ignore
        yield pg_queuer

    task = asyncio.create_task(
        supervisor.run(
            partial(factory, arg=42),
            dequeue_timeout=1.0,
            batch_size=10,
            shutdown=shutdown_event,
        )
    )

    await asyncio.sleep(0.1)
    shutdown_event.set()

    async with async_timeout.timeout(2):
        await task
    assert task.done()


async def test_run_no_restart_on_failure(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    async def failing_run(*args, **kwargs):  # type: ignore
        raise Exception("Simulated failure")

    pg_queuer.run = failing_run  # type: ignore

    @asynccontextmanager
    async def factory():  # type: ignore
        yield pg_queuer

    with pytest.raises(Exception, match="Simulated failure"):
        await supervisor.run(
            factory,
            dequeue_timeout=1.0,
            batch_size=10,
            shutdown=shutdown_event,
        )


async def test_run_negative_restart_delay(shutdown_event: asyncio.Event) -> None:
    with pytest.raises(ValueError, match="'restart_delay' must be >= 0"):
        await supervisor.run(
            lambda: ...,  # type: ignore
            restart_delay=-1.0,
            shutdown=shutdown_event,
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


async def test_fallback_when_add_signal_handler_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    # Simulate Windows/Proactor behavior
    def boom(*_: object) -> None:
        raise NotImplementedError

    monkeypatch.setattr(loop, "add_signal_handler", boom)

    caplog.set_level(logging.WARNING)

    supervisor.setup_signal_handlers(asyncio.Event())
    assert "not supported on this platform" in caplog.text.lower()
