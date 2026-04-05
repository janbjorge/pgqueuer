"""
Supervisor for PgQueuer managers.

Provides :func:`run` to start a QueueManager, SchedulerManager, or PgQueuer
with signal handling, graceful shutdown, and optional restart-on-failure.
"""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress
from datetime import timedelta
from typing import AsyncContextManager, Awaitable, Callable, ContextManager, TypeAlias

from pgqueuer.adapters.cli import factories
from pgqueuer.core import applications, logconfig, qm, sm
from pgqueuer.domain import types

Manager: TypeAlias = qm.QueueManager | sm.SchedulerManager | applications.PgQueuer
ManagerFactory: TypeAlias = Callable[
    [],
    Awaitable[Manager] | AsyncContextManager[Manager] | ContextManager[Manager],
]


def setup_shutdown_handlers(manager: Manager, shutdown: asyncio.Event) -> Manager:
    """Wire *shutdown* into *manager* so that setting the event stops processing."""
    if not isinstance(manager, (qm.QueueManager, sm.SchedulerManager, applications.PgQueuer)):
        raise NotImplementedError(
            f"Unsupported instance type: {type(manager).__name__}. This instance is "
            "not recognized as a valid QueueManager, SchedulerManager, or PgQueuer."
        )
    manager.shutdown = shutdown
    if isinstance(manager, applications.PgQueuer):
        manager.qm.shutdown = shutdown
        manager.sm.shutdown = shutdown
    return manager


def setup_signal_handlers(shutdown: asyncio.Event) -> None:
    """Register SIGINT/SIGTERM handlers that set *shutdown*."""

    def set_shutdown(signum: int) -> None:
        logconfig.logger.info("Signal %d received, shutting down.", signum)
        shutdown.set()

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, set_shutdown, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, set_shutdown, signal.SIGTERM)
    except NotImplementedError:
        logconfig.logger.warning(
            "Async signal handlers are not supported on this platform; "
            "KeyboardInterrupt will still stop the worker."
        )


async def run_manager(
    manager: Manager,
    dequeue_timeout: timedelta,
    batch_size: int,
    mode: types.QueueExecutionMode,
    max_concurrent_tasks: int | None,
    shutdown_on_listener_failure: bool,
) -> None:
    """Dispatch ``manager.run()`` with the right arguments for its type."""
    logconfig.logger.debug("Running: %s", type(manager).__name__)
    if isinstance(manager, sm.SchedulerManager):
        await manager.run()
    elif isinstance(manager, (qm.QueueManager, applications.PgQueuer)):
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
    """Wait for *shutdown* or until *restart_delay* elapses."""
    logconfig.logger.info("Waiting %r before restarting.", restart_delay)

    with suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(shutdown.wait(), timeout=restart_delay.total_seconds())

    if not shutdown.is_set():
        logconfig.logger.info("Attempting to restart...")


async def run(
    factory: str | ManagerFactory,
    *,
    dequeue_timeout: float = 30.0,
    batch_size: int = 10,
    restart_delay: float = 5.0,
    restart_on_failure: bool = False,
    log_level: logconfig.LogLevel = logconfig.LogLevel.INFO,
    mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous,
    max_concurrent_tasks: int | None = None,
    shutdown_on_listener_failure: bool = False,
    shutdown: asyncio.Event | None = None,
) -> None:
    """
    Start a PgQueuer manager.

    This is the programmatic equivalent of the ``pgq run`` CLI command.
    It resolves the factory, sets up logging and signal handlers, and runs
    the supervisor loop (with optional restart-on-failure).

    Args:
        factory: ``"module:function"`` string *or* a callable
            :data:`ManagerFactory`.
        dequeue_timeout: Max seconds to wait for new jobs per dequeue cycle.
        batch_size: Number of jobs to pull from the queue at once.
        restart_delay: Seconds to wait before restarting after a failure.
        restart_on_failure: Whether to automatically restart on failure.
        log_level: PgQueuer log level.
        mode: Queue execution mode (``continuous`` or ``drain``).
        max_concurrent_tasks: Global upper limit on concurrent tasks.
        shutdown_on_listener_failure: Shut down if the NOTIFY listener fails.
        shutdown: Optional :class:`asyncio.Event` to control shutdown
            externally.  A new event is created when ``None``.
    """
    if restart_delay < 0:
        raise ValueError(f"'restart_delay' must be >= 0. Got {restart_delay!r}")

    logconfig.setup_fancy_logger(log_level)

    resolved: ManagerFactory = (
        factories.load_factory(factory) if isinstance(factory, str) else factory
    )
    shutdown_event = shutdown or asyncio.Event()
    dequeue_timeout_td = timedelta(seconds=dequeue_timeout)
    restart_delay_td = timedelta(seconds=restart_delay if restart_on_failure else 0)

    setup_signal_handlers(shutdown_event)

    while not shutdown_event.is_set():
        try:
            async with factories.run_factory(resolved()) as manager:
                setup_shutdown_handlers(manager, shutdown_event)
                await run_manager(
                    manager,
                    dequeue_timeout_td,
                    batch_size,
                    mode,
                    max_concurrent_tasks,
                    shutdown_on_listener_failure,
                )
        except Exception as exc:
            if not restart_on_failure:
                raise
            logconfig.logger.exception("Error during instance execution.", exc_info=exc)

        if not shutdown_event.is_set():
            await await_shutdown_or_timeout(shutdown_event, restart_delay_td)
