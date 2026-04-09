"""Backward-compatibility shim. Canonical: pgqueuer.core.executors"""

from pgqueuer.core.executors import (
    AbstractEntrypointExecutor,
    AbstractScheduleExecutor,
    AsyncContextCrontab,
    AsyncContextEntrypoint,
    AsyncCrontab,
    AsyncEntrypoint,
    DatabaseRetryEntrypointExecutor,
    Entrypoint,
    EntrypointExecutor,
    EntrypointExecutorParameters,
    EntrypointTypeVar,
    ScheduleCrontab,
    ScheduleExecutor,
    ScheduleExecutorFactoryParameters,
    is_async_callable,
)

__all__ = [
    "AbstractEntrypointExecutor",
    "AbstractScheduleExecutor",
    "AsyncContextCrontab",
    "AsyncContextEntrypoint",
    "AsyncCrontab",
    "AsyncEntrypoint",
    "DatabaseRetryEntrypointExecutor",
    "Entrypoint",
    "EntrypointExecutor",
    "EntrypointExecutorParameters",
    "EntrypointTypeVar",
    "ScheduleCrontab",
    "ScheduleExecutor",
    "ScheduleExecutorFactoryParameters",
    "is_async_callable",
]
