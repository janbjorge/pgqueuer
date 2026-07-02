from __future__ import annotations

import asyncio
import signal
from contextlib import AbstractAsyncContextManager, suppress
from datetime import timedelta
from typing import Callable, TypeAlias

from pgqueuer.adapters.cli import factories
from pgqueuer.core import applications, logconfig, qm, sm
from pgqueuer.domain import types

Manager: TypeAlias = qm.QueueManager | sm.SchedulerManager | applications.PgQueuer
ManagerFactory: TypeAlias = Callable[[], AbstractAsyncContextManager[Manager]]


def setup_shutdown_handlers(manager: Manager, shutdown: asyncio.Event) -> Manager:
    """Wire *shutdown* into *manager* so cancellation propagates to inner managers."""

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
    """Set *shutdown* on SIGINT/SIGTERM; silently skip on platforms without async signal support."""

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
    heartbeat_timeout: timedelta = timedelta(seconds=30),
) -> None:
    """Supervise a manager lifecycle; optionally restart after *restart_delay* on failure.

    Raises ValueError when ``restart_delay`` is negative.
    """
    if restart_delay < timedelta(0):
        raise ValueError(f"'restart_delay' must be >= 0. Got {restart_delay!r}")

    setup_signal_handlers(shutdown)

    while not shutdown.is_set():
        try:
            async with factories.validate_factory_result(factory()) as manager:
                setup_shutdown_handlers(manager, shutdown)
                await run_manager(
                    manager,
                    dequeue_timeout,
                    batch_size,
                    mode,
                    max_concurrent_tasks,
                    shutdown_on_listener_failure,
                    heartbeat_timeout,
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
    heartbeat_timeout: timedelta = timedelta(seconds=30),
) -> None:
    """Dispatch ``run()`` on the concrete manager type.

    Raises NotImplementedError if *manager* is not a recognised manager class.
    """
    logconfig.logger.debug("Running: %s", type(manager).__name__)
    if isinstance(manager, qm.QueueManager):
        await manager.run(
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
            heartbeat_timeout=heartbeat_timeout,
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
            heartbeat_timeout=heartbeat_timeout,
        )
    else:
        raise NotImplementedError(f"Unsupported instance type: {type(manager)}")


async def await_shutdown_or_timeout(
    shutdown: asyncio.Event,
    restart_delay: timedelta,
) -> None:
    """Wait for *shutdown* or up to *restart_delay*, whichever comes first."""
    logconfig.logger.info("Waiting %r before restarting.", restart_delay)

    with suppress(TimeoutError, asyncio.TimeoutError):
        await asyncio.wait_for(shutdown.wait(), timeout=restart_delay.total_seconds())

    if not shutdown.is_set():
        logconfig.logger.info("Attempting to restart...")
