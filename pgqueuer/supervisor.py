"""
This module provides functionality to dynamically load and run queue management components.
It includes the ability to load a factory function for creating instances of
QueueManager, Scheduler and PgQueuer, manage their lifecycle, and handle graceful shutdowns.
The module is designed to support asynchronous queue processing and scheduling
using configurable factory paths.
"""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress
from datetime import timedelta
from typing import AsyncContextManager, Awaitable, Callable, ContextManager, TypeAlias

from . import applications, factories, logconfig, qm, sm, types

Manager: TypeAlias = qm.QueueManager | sm.SchedulerManager | applications.PgQueuer
ManagerFactory: TypeAlias = Callable[
    [],
    Awaitable[Manager] | AsyncContextManager[Manager] | ContextManager[Manager],
]


def setup_shutdown_handlers(manager: Manager, shutdown: asyncio.Event) -> Manager:
    """
    Create and configure a queue management instance.

    Args:
        factory_fn (FactoryType): The factory function to create the instance.
        shutdown (asyncio.Event): The event used to signal shutdown.

    Returns:
        qm.QueueManager | sm.SchedulerManager | applications.PgQueuer:
            The configured instance.

    Raises:
        Exception: If an error occurs during instance creation or configuration.
    """

    if isinstance(manager, qm.QueueManager | sm.SchedulerManager):
        manager.shutdown = shutdown
    elif isinstance(manager, applications.PgQueuer):
        manager.shutdown = shutdown
        manager.qm.shutdown = shutdown
        manager.sm.shutdown = shutdown
    else:
        raise NotImplementedError(
            f"Unsupported instance type: {type(manager).__name__}. This instance is "
            "not recognized as a valid QueueManager, SchedulerManager, or PgQueuer."
        )
    return manager


def setup_signal_handlers(shutdown: asyncio.Event) -> None:
    """
    Setup signal handlers for clean shutdown on SIGINT or SIGTERM.

    Args:
        shutdown: Event to signal shutdown.
    """

    def set_shutdown(signum: int) -> None:
        logconfig.logger.info("Signal %d received, shutting down.", signum)
        shutdown.set()

    # Adding signal handlers ensures the application can gracefully
    # handle shutdown signals (SIGINT, SIGTERM).The try/except block is
    # necessary because some platforms, like Windows, do not support adding async signal handlers.
    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, set_shutdown, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, set_shutdown, signal.SIGTERM)
    except NotImplementedError:
        logconfig.logger.warning(
            "Async signal handlers are not supported on this platform; "
            "KeyboardInterrupt will still stop the worker."
        )


async def runit(
    factory: ManagerFactory,
    dequeue_timeout: timedelta,
    batch_size: int,
    restart_delay: timedelta,
    restart_on_failure: bool,
    shutdown: asyncio.Event,
    mode: types.QueueExecutionMode,
    max_concurrent_tasks: int | None,
    shutdown_on_listener_failure: bool,
) -> None:
    """
    Supervise and manage the lifecycle of a queue management instance.

    Args:
        factory (FactoryType): Factory function or path to create an instance.
        dequeue_timeout (timedelta): Timeout duration for dequeuing jobs.
        batch_size (int): Number of jobs to process in each batch.
        restart_delay (timedelta): Delay before restarting on failure.
        restart_on_failure (bool): Whether to restart after a failure.
        shutdown (asyncio.Event): The event to signal shutdown.

    Raises:
        ValueError: If restart_delay is negative.
    """
    if restart_delay < timedelta(0):
        raise ValueError(f"'restart_delay' must be >= 0. Got {restart_delay!r}")

    setup_signal_handlers(shutdown)

    while not shutdown.is_set():
        try:
            async with factories.run_factory(factory()) as manager:
                setup_shutdown_handlers(manager, shutdown)
                await run_manager(
                    manager,
                    dequeue_timeout,
                    batch_size,
                    mode,
                    max_concurrent_tasks,
                    shutdown_on_listener_failure,
                )
        except Exception as exc:
            if not restart_on_failure:
                raise
            logconfig.logger.exception(
                "Error during instance execution.",
                exc_info=exc,
            )

        if not shutdown.is_set():
            await await_shutdown_or_timeout(shutdown, restart_delay)


async def run_manager(
    manager: Manager,
    dequeue_timeout: timedelta,
    batch_size: int,
    mode: types.QueueExecutionMode,
    max_concurrent_tasks: int | None,
    shutdown_on_listener_failure: bool,
) -> None:
    """
    Run a queue management instance.

    Args:
        manager: The instance to run (QueueManager, SchedulerManager, or PgQueuer).
        dequeue_timeout: Timeout duration for dequeuing jobs.
        batch_size: Number of jobs to process per batch.

    Raises:
        NotImplementedError: If the instance type is unsupported.
    """
    logconfig.logger.debug("Running: %s", type(manager).__name__)
    if isinstance(manager, qm.QueueManager):
        await manager.run(
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
        )
    elif isinstance(manager, sm.SchedulerManager):
        await manager.run()
    elif isinstance(manager, applications.PgQueuer):
        await manager.run(
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
        )
    else:
        raise NotImplementedError(f"Unsupported instance type: {type(manager)}")


async def await_shutdown_or_timeout(
    shutdown: asyncio.Event,
    restart_delay: timedelta,
) -> None:
    """
    Wait for shutdown or until ``restart_delay`` elapses.

    Args:
        shutdown: Event indicating shutdown.
        restart_delay: Delay duration before restarting.
    """

    logconfig.logger.info("Waiting %r before restarting.", restart_delay)

    with suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(shutdown.wait(), timeout=restart_delay.total_seconds())

    if not shutdown.is_set():
        logconfig.logger.info("Attempting to restart...")
