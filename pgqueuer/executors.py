"""Backward-compatibility shim. Canonical: pgqueuer.core.executors"""

from pgqueuer.core.executors import (
    AbstractEntrypointExecutor,
    AbstractScheduleExecutor,
    AsyncContextEntrypoint,
    AsyncCrontab,
    AsyncEntrypoint,
    DatabaseRetryEntrypointExecutor,
    Entrypoint,
    EntrypointExecutor,
    EntrypointExecutorParameters,
    EntrypointTypeVar,
    RetryWithBackoffEntrypointExecutor,
    ScheduleExecutor,
    ScheduleExecutorFactoryParameters,
    is_async_callable,
)

__all__ = [
    "AbstractEntrypointExecutor",
    "AbstractScheduleExecutor",
    "AsyncContextEntrypoint",
    "AsyncCrontab",
    "AsyncEntrypoint",
    "DatabaseRetryEntrypointExecutor",
    "Entrypoint",
    "EntrypointExecutor",
    "EntrypointExecutorParameters",
    "EntrypointTypeVar",
    "RetryWithBackoffEntrypointExecutor",
    "ScheduleExecutor",
    "ScheduleExecutorFactoryParameters",
    "is_async_callable",
]
