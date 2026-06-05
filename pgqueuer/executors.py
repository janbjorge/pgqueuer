"""Backward-compatibility shim. Canonical: pgqueuer.core.executors"""

from __future__ import annotations

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
    wants_context,
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
    "wants_context",
]
