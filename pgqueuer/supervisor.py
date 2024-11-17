"""
This module provides functionality to dynamically load and run queue management components.

It includes the ability to load a factory function for creating instances of
QueueManager, Scheduler and PgQueuer, manage their lifecycle, and handle graceful shutdowns.
The module is designed to support asynchronous queue processing and scheduling
using configurable factory paths.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import sys
from datetime import timedelta
from typing import Awaitable, Callable

from . import applications, qm, sm


def load_manager_factory(
    factory_path: str,
) -> Callable[
    [],
    Awaitable[qm.QueueManager | sm.SchedulerManager | applications.PgQueuer],
]:
    """
    Load factory function from a given module path.

    Args:
        factory_path (str): Module path to the factory function.

    Returns:
        Callable: Async callable returning a QueueManager,
            SchedulerManager, or PgQueuer instance.
    """
    sys.path.insert(0, os.getcwd())
    module_name, factory_name = factory_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


async def runit(
    factory_fn: str,
    dequeue_timeout: timedelta,
    batch_size: int,
) -> None:
    """
    Run QueueManager, SchedulerManager, or PgQueuer using the factory function.

    Args:
        factory_fn (str): Module path to the factory function.
        dequeue_timeout (timedelta): Timeout for dequeuing jobs.
        batch_size (int): Number of jobs per batch.
        retry_timer (timedelta | None): Retry timer for stalled jobs.
    """
    instance = await load_manager_factory(factory_fn)()

    def set_shutdown(signum: int) -> None:
        """
        Handle shutdown signals.

        Args:
            signum (int): Signal number received.
        """
        print(f"Received signal {signum}, shutting down...", flush=True)
        instance.shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: set_shutdown(signal.SIGINT))
    loop.add_signal_handler(signal.SIGTERM, lambda: set_shutdown(signal.SIGTERM))

    if isinstance(instance, qm.QueueManager):
        await instance.run(
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
        )
    elif isinstance(instance, sm.SchedulerManager):
        await instance.run()
    elif isinstance(instance, applications.PgQueuer):
        await instance.run(
            dequeue_timeout=dequeue_timeout,
            batch_size=batch_size,
        )
    else:
        raise NotImplementedError(instance)
