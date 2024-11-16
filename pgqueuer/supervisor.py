"""
This module provides functionality to dynamically load and run queue management components.

It includes the ability to load a factory function for creating instances of
QueueManager or Scheduler, manage their lifecycle, and handle graceful shutdowns.
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

from . import qm, sm


def load_manager_factory(
    factory_path: str,
) -> Callable[
    [],
    Awaitable[qm.QueueManager | sm.SchedulerManager],
]:
    """
    Load the QueueManager factory function from a given module path.

    Dynamically imports the specified module and retrieves the factory function
    used to create a QueueManager or Scheduler instance. The factory function should be an
    asynchronous callable that returns a QueueManager or Scheduler.

    Args:
        factory_path (str): The full module path to the factory function,
            e.g., 'myapp.create_queue_manager'.

    Returns:
        Callable: A callable that returns an awaitable QueueManager or Scheduler instance.
    """
    sys.path.insert(0, os.getcwd())
    module_name, factory_name = factory_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


async def runit(
    factory_fn: str,
    dequeue_timeout: timedelta,
    batch_size: int,
    retry_timer: timedelta | None,
) -> None:
    """
    Instantiate and run a QueueManager or Scheduler using the provided factory function.

    This function handles the setup and lifecycle management of the QueueManager or Scheduler,
    including signal handling for graceful shutdown. It loads the QueueManager or Scheduler
    factory, creates an instance, sets up signal handlers, and starts the QueueManager's or
    Scheduler's run loop with the specified parameters.

    Args:
        factory_fn (str): The module path to the QueueManager or Scheduler factory function,
            e.g., 'myapp.create_queue_manager'.
        dequeue_timeout (timedelta): The timeout duration for dequeuing jobs.
        batch_size (int): The number of jobs to retrieve in each batch.
        retry_timer (timedelta | None): The duration after which to retry 'picked' jobs
            that may have stalled. If None, retry logic is disabled.
    """
    instance = await load_manager_factory(factory_fn)()

    def set_shutdown(signum: int) -> None:
        """
        Handle incoming signals to perform a graceful shutdown.

        When a termination signal is received (e.g., SIGINT or SIGTERM), this function
        sets the 'shutdown' event in the QueueManager or Scheduler to initiate a controlled shutdown
        process, allowing tasks to complete cleanly.

        Args:
            signum (int): The signal number received.
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
            retry_timer=retry_timer,
        )
    elif isinstance(instance, sm.SchedulerManager):
        await instance.run()
    else:
        raise NotImplementedError(instance)
