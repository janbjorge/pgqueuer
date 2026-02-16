"""Backward-compatibility shim. Canonical: pgqueuer.adapters.cli.supervisor"""

from pgqueuer.adapters.cli.supervisor import (
    Manager,
    ManagerFactory,
    await_shutdown_or_timeout,
    run_manager,
    runit,
    setup_shutdown_handlers,
    setup_signal_handlers,
)

__all__ = [
    "Manager",
    "ManagerFactory",
    "await_shutdown_or_timeout",
    "run_manager",
    "runit",
    "setup_shutdown_handlers",
    "setup_signal_handlers",
]
