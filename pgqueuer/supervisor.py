from __future__ import annotations

import importlib
import signal
from datetime import timedelta
from typing import Awaitable, Callable, TypeAlias

from . import qm

QM_FACTORY: TypeAlias = Callable[[], Awaitable[qm.QueueManager]]


def load_queue_manager_factory(factory_path: str) -> QM_FACTORY:
    """
    Load and return the QueueManager factory function from a given module path.
    """
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
    Main function to instantiate and manage the lifecycle of a QueueManager.

    Parameters:
        - factory_fn: module and function path to a function returning a QueueManager
        - **kwargs: keyword arguments passed through to QueueManager.run
    """
    qm_factory = load_queue_manager_factory(factory_fn)
    qm = await qm_factory()  # Create the QueueManager instance

    def graceful_shutdown(signum: int, frame: object) -> None:
        """
        Handle incoming signals that require a graceful shutdown of the application.
        """
        print(f"Received signal {signum}, shutting down...")
        qm.alive.set()

    # Setup signal handlers for graceful shutdown of the QueueManager.
    signal.signal(signal.SIGINT, graceful_shutdown)  # Handle Ctrl-C
    signal.signal(signal.SIGTERM, graceful_shutdown)  # Handle termination request

    # Start the QueueManager's operation
    await qm.run(
        dequeue_timeout=dequeue_timeout,
        batch_size=batch_size,
        retry_timer=retry_timer,
    )
