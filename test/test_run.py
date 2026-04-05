from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager
from unittest.mock import patch

import async_timeout
import pytest

from pgqueuer import AsyncpgDriver, PgQueuer, run
from pgqueuer.core.logconfig import LogLevel
from pgqueuer.types import QueueExecutionMode


@pytest.fixture(scope="function")
async def pg_queuer(apgdriver: AsyncpgDriver) -> PgQueuer:
    return PgQueuer(connection=apgdriver)


@pytest.fixture(scope="function")
def shutdown_event() -> asyncio.Event:
    return asyncio.Event()


async def test_run_with_callable_factory(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    @asynccontextmanager
    async def factory():  # type: ignore
        yield pg_queuer

    task = asyncio.create_task(
        run(
            factory,
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


async def test_run_with_string_factory(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    @asynccontextmanager
    async def factory():  # type: ignore
        yield pg_queuer

    with patch(
        "pgqueuer.adapters.cli.supervisor.factories.load_factory",
        return_value=factory,
    ) as mock_load:
        task = asyncio.create_task(
            run(
                "mymodule:myfactory",
                dequeue_timeout=1.0,
                batch_size=10,
                shutdown=shutdown_event,
            )
        )

        await asyncio.sleep(0.1)
        shutdown_event.set()

        async with async_timeout.timeout(2):
            await task

        mock_load.assert_called_once_with("mymodule:myfactory")


async def test_run_custom_shutdown_event(
    pg_queuer: PgQueuer,
) -> None:
    custom_shutdown = asyncio.Event()

    @asynccontextmanager
    async def factory():  # type: ignore
        yield pg_queuer

    task = asyncio.create_task(
        run(
            factory,
            dequeue_timeout=1.0,
            batch_size=10,
            shutdown=custom_shutdown,
        )
    )

    await asyncio.sleep(0.1)
    assert not task.done()

    custom_shutdown.set()

    async with async_timeout.timeout(2):
        await task
    assert task.done()


async def test_run_log_level_setup(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    @asynccontextmanager
    async def factory():  # type: ignore
        yield pg_queuer

    with patch(
        "pgqueuer.adapters.cli.supervisor.logconfig.setup_fancy_logger"
    ) as mock_logger:
        shutdown_event.set()
        await run(
            factory,
            log_level=LogLevel.DEBUG,
            shutdown=shutdown_event,
        )
        mock_logger.assert_called_once_with(LogLevel.DEBUG)


async def test_run_restart_on_failure(
    pg_queuer: PgQueuer,
    shutdown_event: asyncio.Event,
) -> None:
    calls = 0

    async def failing_run(*args, **kwargs):  # type: ignore
        nonlocal calls
        calls += 1
        raise Exception("Simulated failure")

    pg_queuer.run = failing_run  # type: ignore

    async def factory():  # type: ignore
        return pg_queuer

    task = asyncio.create_task(
        run(
            factory,
            dequeue_timeout=1.0,
            batch_size=10,
            restart_delay=0.05,
            restart_on_failure=True,
            shutdown=shutdown_event,
        )
    )

    await asyncio.sleep(0.2)
    shutdown_event.set()

    async with async_timeout.timeout(2):
        await task
    assert task.done()
    assert calls > 1


def test_run_defaults_match_cli() -> None:
    sig = inspect.signature(run)

    assert sig.parameters["dequeue_timeout"].default == 30.0
    assert sig.parameters["batch_size"].default == 10
    assert sig.parameters["restart_delay"].default == 5.0
    assert sig.parameters["restart_on_failure"].default is False
    assert sig.parameters["log_level"].default == LogLevel.INFO
    assert sig.parameters["mode"].default == QueueExecutionMode.continuous
    assert sig.parameters["max_concurrent_tasks"].default is None
    assert sig.parameters["shutdown_on_listener_failure"].default is False
    assert sig.parameters["shutdown"].default is None
