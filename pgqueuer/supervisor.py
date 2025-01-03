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
from datetime import timedelta
from typing import Awaitable, Callable, TypeAlias

from pgqueuer.factories import load_factory

from . import applications, logconfig, qm, sm

Manager: TypeAlias = qm.QueueManager | sm.SchedulerManager | applications.QueueManager
ManagerFactory: TypeAlias = Callable[[], Awaitable[Manager]]


def load_manager_factory(factory_path: str) -> ManagerFactory:
    """
    Load factory function from a given module path or factory-style path.

    Args:
        factory_path (str): Module path to the factory function or factory-style path.

    Returns:
        A callable that creates an instance of QueueManager, SchedulerManager, or PgQueuer.
    """
    return load_factory(factory_path)


async def runit(
    factory: ManagerFactory,
    dequeue_timeout: timedelta,
    batch_size: int,
    restart_delay: timedelta,
    restart_on_failure: bool,
    shutdown: asyncio.Event,
) -> None:
    """
    Run and supervise a queue management instance with restart logic.

    Args:
        factory: Factory function or path to create an instance.
        dequeue_timeout: Timeout duration for dequeuing jobs.
        batch_size: Number of jobs to process in each batch.
        restart_delay: Delay before restarting on failure.
        restart_on_failure: Whether to restart after a failure.

    Raises:
        ValueError: If restart_delay is negative.
    """
    t0 = timedelta(seconds=0)
    if restart_delay < t0:
        raise ValueError(f"'restart_delay' must be >= {t0}. Got {restart_delay!r}")

    setup_signal_handlers(shutdown)

    while not shutdown.is_set():
        try:
            manager = await factory()
            logconfig.logger.info("Instance created: %s", type(manager).__name__)

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

        except Exception as exc:
            if not restart_on_failure:
                raise
            logconfig.logger.exception(
                "Error creating or configuring instance.",
                exc_info=exc,
            )
            await await_shutdown_or_timeout(shutdown, restart_delay)
            continue

        try:
            await run_manager(manager, dequeue_timeout, batch_size)
        except Exception as exc:
            if not restart_on_failure:
                raise
            logconfig.logger.exception(
                "Error during instance execution.",
                exc_info=exc,
            )

        if not shutdown.is_set():
            await await_shutdown_or_timeout(shutdown, restart_delay)


def setup_signal_handlers(shutdown: asyncio.Event) -> None:
    """
    Setup signal handlers for clean shutdown on SIGINT or SIGTERM.

    Args:
        shutdown: Event to signal shutdown.
    """

    def set_shutdown(signum: int) -> None:
        logconfig.logger.info("Signal %d received, shutting down.", signum)
        shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, set_shutdown, signal.SIGINT)
    loop.add_signal_handler(signal.SIGTERM, set_shutdown, signal.SIGTERM)


async def run_manager(
    mananger: Manager,
    dequeue_timeout: timedelta,
    batch_size: int,
) -> None:
    """
    Run a queue management instance.

    Args:
        instance: The instance to run (QueueManager, SchedulerManager, or PgQueuer).
        dequeue_timeout: Timeout duration for dequeuing jobs.
        batch_size: Number of jobs to process per batch.

    Raises:
        NotImplementedError: If the instance type is unsupported.
    """
    logconfig.logger.debug("Running: %s", type(mananger).__name__)
    if isinstance(mananger, qm.QueueManager):
        await mananger.run(dequeue_timeout=dequeue_timeout, batch_size=batch_size)
    elif isinstance(mananger, sm.SchedulerManager):
        await mananger.run()
    elif isinstance(mananger, applications.PgQueuer):
        await mananger.run(dequeue_timeout=dequeue_timeout, batch_size=batch_size)
    else:
        raise NotImplementedError(f"Unsupported instance type: {type(mananger)}")


async def await_shutdown_or_timeout(
    shutdown: asyncio.Event,
    restart_delay: timedelta,
) -> None:
    """
    Wait for a shutdown event or timeout after an exception.

    Args:
        shutdown: Event indicating shutdown.
        restart_delay: Delay duration before restarting.
        exc: The exception that triggered the wait.
    """

    logconfig.logger.info("Waiting %r before restarting.", restart_delay)

    _, pending = await asyncio.wait(
        (
            asyncio.create_task(asyncio.sleep(restart_delay.total_seconds())),
            asyncio.create_task(shutdown.wait()),
        ),
        return_when=asyncio.FIRST_COMPLETED,
    )

    for not_done in pending:
        not_done.cancel()

    if not shutdown.is_set():
        logconfig.logger.info("Attempting to restart...")
