from __future__ import annotations

import importlib
import signal
from typing import Awaitable, Callable, TypeAlias

from .qm import QueueManager

QM_FACTORY: TypeAlias = Callable[[], Awaitable[QueueManager]]


def load_queue_manager_factory(factory_path: str) -> QM_FACTORY:
    """
    Load and return the QueueManager factory function from a given module path.
    """
    module_name, factory_name = factory_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, factory_name)


async def runit(factory_fn: str, **kwargs) -> None:
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
        qm.alive = False

    # Setup signal handlers for graceful shutdown of the QueueManager.
    signal.signal(signal.SIGINT, graceful_shutdown)  # Handle Ctrl-C
    signal.signal(signal.SIGTERM, graceful_shutdown)  # Handle termination request

    await qm.run(**kwargs)  # Start the QueueManager's operation
