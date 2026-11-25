from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import ExitStack, asynccontextmanager, contextmanager, nullcontext, suppress
from datetime import timedelta
from typing import Protocol, TypeAlias, runtime_checkable

from pgqueuer.adapters.cli import factories
from pgqueuer.core import applications, buffers, logconfig, qm, sm, tm
from pgqueuer.domain import types

Manager: TypeAlias = qm.QueueManager | sm.SchedulerManager | applications.PgQueuer
ManagerFactory: TypeAlias = Callable[[], object]


def setup_shutdown_handlers(manager: object, shutdown: asyncio.Event) -> Manager:
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


@contextmanager
def setup_signal_handlers(shutdown: asyncio.Event) -> Iterator[None]:
    """Set *shutdown* on SIGINT/SIGTERM until exit. Do nothing where async signals fail."""

    def set_shutdown(signum: int) -> None:
        logconfig.logger.info("Signal %d received, shutting down.", signum)
        # The default action handles the next signal, so a second signal stops the process.
        loop.remove_signal_handler(signum)
        shutdown.set()

    loop = asyncio.get_event_loop()
    with ExitStack() as handlers:
        try:
            loop.add_signal_handler(signal.SIGINT, set_shutdown, signal.SIGINT)
            handlers.callback(loop.remove_signal_handler, signal.SIGINT)
            loop.add_signal_handler(signal.SIGTERM, set_shutdown, signal.SIGTERM)
            handlers.callback(loop.remove_signal_handler, signal.SIGTERM)
        except NotImplementedError:
            logconfig.logger.warning(
                "Async signal handlers are not supported on this platform; "
                "KeyboardInterrupt will still stop the worker."
            )
        yield


async def forward_shutdown(source: asyncio.Event, target: asyncio.Event) -> None:
    """Set *target* once *source* is set so a process signal stops the current cycle."""
    await source.wait()
    target.set()


@runtime_checkable
class CarriesUnwrittenJobLogs(Protocol):
    """A manager that can still hold job ends from a failed flush."""

    unwritten_job_logs: list[buffers.JobLogEntry]

    async def write_job_ends(self, logs: list[buffers.JobLogEntry]) -> None: ...


class UnwrittenJobLogs:
    """Job ends a failed flush left in this process.

    ``around`` writes them before the body runs, then keeps whatever that
    run could not write. A failed write, or a manager with no queue, leaves
    the held ends in place.
    """

    def __init__(self) -> None:
        self.entries: list[buffers.JobLogEntry] = []

    @asynccontextmanager
    async def around(self, manager: object) -> AsyncIterator[None]:
        carrier = manager if isinstance(manager, CarriesUnwrittenJobLogs) else None
        if self.entries and carrier is not None:
            await carrier.write_job_ends(self.entries)
            self.entries = []
        try:
            yield
        finally:
            if carrier is not None and carrier.unwritten_job_logs:
                self.entries = list(carrier.unwritten_job_logs)


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

    Each cycle gets its own shutdown event so a manager that sets ``shutdown`` on
    exit (e.g. ``PgQueuer.run``'s ``finally``) cannot latch the process-level
    event and defeat ``restart_on_failure``. Process signals still stop the
    current cycle via :func:`forward_shutdown`.

    Raises ValueError when ``restart_delay`` is negative.
    """
    if restart_delay < timedelta(0):
        raise ValueError(f"'restart_delay' must be >= 0. Got {restart_delay!r}")

    unwritten = UnwrittenJobLogs()
    while not shutdown.is_set():
        cycle_shutdown = asyncio.Event()
        forward_task = asyncio.create_task(forward_shutdown(shutdown, cycle_shutdown))
        failed = False
        try:
            async with factories.validate_factory_result(factory()) as entered:
                manager = setup_shutdown_handlers(entered, cycle_shutdown)
                async with unwritten.around(manager):
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
            failed = True
            logconfig.logger.exception(
                "Error during instance execution.",
                exc_info=exc,
            )
        finally:
            await tm.cancel_and_wait(forward_task)

        # Clean completion (drain / signal) ends the supervisor; only failures restart.
        if shutdown.is_set() or not failed:
            break

        await await_shutdown_or_timeout(shutdown, restart_delay)


async def run(
    factory: str | ManagerFactory,
    *,
    dequeue_timeout: timedelta = timedelta(seconds=30),
    batch_size: int = 10,
    heartbeat_timeout: timedelta = timedelta(seconds=30),
    mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous,
    max_concurrent_tasks: int | None = None,
    shutdown_on_listener_failure: bool = False,
    restart_on_failure: bool = False,
    restart_delay: timedelta = timedelta(seconds=5),
    shutdown: asyncio.Event | None = None,
) -> None:
    """Run a worker from *factory*, as ``pgq run`` does, without the CLI.

    *factory* is a ``module:attr`` path or the factory itself. Without *shutdown*,
    SIGINT/SIGTERM stop the worker; with it, signal handling is left to the caller.

    Usage example::

        asyncio.run(pgqueuer.run("myapp:create_pgqueuer"))
    """
    owns_signals = shutdown is None
    shutdown = asyncio.Event() if shutdown is None else shutdown
    with setup_signal_handlers(shutdown) if owns_signals else nullcontext():
        await runit(
            factories.load_factory(factory) if isinstance(factory, str) else factory,
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
            restart_delay=restart_delay if restart_on_failure else timedelta(0),
            restart_on_failure=restart_on_failure,
            shutdown=shutdown,
            mode=mode,
            max_concurrent_tasks=max_concurrent_tasks,
            shutdown_on_listener_failure=shutdown_on_listener_failure,
            heartbeat_timeout=heartbeat_timeout,
        )


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

    with suppress(TimeoutError):
        async with asyncio.timeout(restart_delay.total_seconds()):
            await shutdown.wait()

    if not shutdown.is_set():
        logconfig.logger.info("Attempting to restart...")
