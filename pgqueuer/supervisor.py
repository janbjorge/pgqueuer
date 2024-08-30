from __future__ import annotations

import asyncio
import importlib
import signal
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import timedelta
from typing import Awaitable, Callable, TypeAlias

from . import logconfig, qm

QM_FACTORY: TypeAlias = Callable[[], Awaitable[qm.QueueManager]]


def load_queue_manager_factory(factory_path: str) -> QM_FACTORY:
    """
    Load and return the QueueManager factory function from a given module path.
    """
    module_name, factory_name = factory_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


async def __runit(
    factory_fn: str,
    dequeue_timeout: timedelta,
    batch_size: int,
    retry_timer: timedelta | None,
    worker_id: int,
) -> None:
    qm_factory = load_queue_manager_factory(factory_fn)
    qm = await qm_factory()  # Create the QueueManager instance

    def graceful_shutdown(signum: int, frame: object) -> None:
        """
        Handle incoming signals that require a graceful shutdown of the application.
        """
        logconfig.logger.info(
            "Received signal %s, shutting down worker %s",
            signum,
            worker_id,
        )
        qm.alive = False

    # Setup signal handlers for graceful shutdown of the QueueManager.
    signal.signal(signal.SIGINT, graceful_shutdown)  # Handle Ctrl-C
    signal.signal(signal.SIGTERM, graceful_shutdown)  # Handle termination request

    # Start the QueueManager's operation
    await qm.run(
        dequeue_timeout=dequeue_timeout,
        batch_size=batch_size,
        retry_timer=retry_timer,
    )


def _runit(
    factory_fn: str,
    dequeue_timeout: timedelta,
    batch_size: int,
    retry_timer: timedelta | None,
    worker_id: int,
) -> None:
    asyncio.run(
        __runit(
            factory_fn=factory_fn,
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
            retry_timer=retry_timer,
            worker_id=worker_id,
        ),
    )


async def runit(
    factory_fn: str,
    dequeue_timeout: timedelta,
    batch_size: int,
    retry_timer: timedelta | None,
    workers: int,
) -> None:
    """
    Main function to instantiate and manage the lifecycle of a QueueManager.

    Parameters:
        - factory_fn: module and function path to a function returning a QueueManager
        - **kwargs: keyword arguments passed through to QueueManager.run
    """

    def log_unhandled_exception(f: Future) -> None:
        if e := f.exception():
            logconfig.logger.critical(
                "Unhandled exception: worker shutdown!",
                exc_info=e,
            )

    futures = set[Future]()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for wid in range(workers):
            f = pool.submit(
                _runit,
                factory_fn,
                dequeue_timeout,
                batch_size,
                retry_timer,
                wid,
            )
            f.add_done_callback(log_unhandled_exception)
            futures.add(f)
