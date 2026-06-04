from __future__ import annotations

import dataclasses
import functools
import inspect
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, TypeAlias, TypeVar, cast

from croniter import croniter

from pgqueuer.domain import errors, models, types
from pgqueuer.domain.models import utc_now

AsyncEntrypoint: TypeAlias = Callable[[models.Job], Awaitable[None]]
AsyncContextEntrypoint: TypeAlias = Callable[[models.Job, models.Context], Awaitable[None]]
Entrypoint: TypeAlias = AsyncEntrypoint | AsyncContextEntrypoint
EntrypointTypeVar = TypeVar("EntrypointTypeVar", bound=Entrypoint)


AsyncCrontab: TypeAlias = Callable[[models.Schedule], Awaitable[None]]
AsyncContextCrontab: TypeAlias = Callable[
    [models.Schedule, models.ScheduleContext], Awaitable[None]
]
ScheduleCrontab: TypeAlias = AsyncCrontab | AsyncContextCrontab


def is_async_callable(obj: Callable[..., object] | object) -> bool:
    """Return True if *obj* is an async function or async-callable instance."""
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(getattr(obj, "__call__", None))
    )


def wants_context(func: Callable[..., object], context_type: type) -> bool:
    """Return True if func declares a positionally-bindable parameter annotated context_type."""
    # eval_str resolves string/forward-ref annotations (PEP 563) to real types.
    try:
        signature = inspect.signature(func, eval_str=True)
    except (TypeError, ValueError, NameError):
        return False

    positional_kinds = (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    )
    for parameter in signature.parameters.values():
        if parameter.kind not in positional_kinds:
            continue
        if parameter.annotation is context_type:
            return True
    return False


@dataclasses.dataclass
class EntrypointExecutorParameters:
    concurrency_limit: int
    func: Entrypoint
    accepts_context: bool = False
    on_failure: types.OnFailure = "delete"


@dataclasses.dataclass
class AbstractEntrypointExecutor(ABC):
    """Subclass to customise per-job execution."""

    parameters: EntrypointExecutorParameters

    @abstractmethod
    async def execute(self, job: models.Job, context: models.Context) -> None: ...


@dataclasses.dataclass
class EntrypointExecutor(AbstractEntrypointExecutor):
    """Default executor: invokes the registered async entrypoint."""

    def __post_init__(self) -> None:
        if not is_async_callable(cast(Callable[..., object], self.parameters.func)):
            raise TypeError(
                "Entrypoint function must be async (defined with 'async def'). "
                "Sync entrypoints are no longer supported. "
                "Wrap blocking code with asyncio.to_thread(): "
                "async def my_entry(job): await asyncio.to_thread(blocking_fn, job)"
            )

    async def execute(self, job: models.Job, context: models.Context) -> None:
        if self.parameters.accepts_context:
            await cast(AsyncContextEntrypoint, self.parameters.func)(job, context)
        else:
            await cast(AsyncEntrypoint, self.parameters.func)(job)


@dataclasses.dataclass
class DatabaseRetryEntrypointExecutor(EntrypointExecutor):
    """Executor that converts exceptions into database-level retries.

    On failure the job is re-queued via :class:`~pgqueuer.domain.errors.RetryRequested`
    with exponential backoff derived from ``job.attempts``.  After *max_attempts*
    consecutive failures the exception propagates as a terminal failure.
    """

    max_attempts: int = 5
    initial_delay: timedelta = dataclasses.field(default_factory=lambda: timedelta(seconds=1))
    max_delay: timedelta = dataclasses.field(default_factory=lambda: timedelta(minutes=5))
    backoff_multiplier: float = 2.0

    async def execute(self, job: models.Job, context: models.Context) -> None:
        try:
            await super().execute(job, context)
        except errors.RetryRequested:
            raise
        except Exception as e:
            if job.attempts >= self.max_attempts:
                raise
            delay = min(
                self.initial_delay * (self.backoff_multiplier**job.attempts),
                self.max_delay,
            )
            raise errors.RetryRequested(delay=delay, reason=str(e)) from e


@dataclasses.dataclass
class ScheduleExecutorFactoryParameters:
    entrypoint: str
    expression: str
    func: ScheduleCrontab
    clean_old: bool
    accepts_context: bool = False


@dataclasses.dataclass
class AbstractScheduleExecutor(ABC):
    """Subclass to customise per-schedule execution."""

    parameters: ScheduleExecutorFactoryParameters

    @abstractmethod
    async def execute(self, schedule: models.Schedule, context: models.ScheduleContext) -> None: ...

    def get_next(self) -> datetime:
        """Next scheduled fire time in UTC."""
        return datetime.fromtimestamp(
            croniter(self.parameters.expression, start_time=utc_now()).get_next(),
            timezone.utc,
        )

    def next_in(self) -> timedelta:
        """Time remaining until ``get_next()``."""
        return self.get_next() - utc_now()


@dataclasses.dataclass
class ScheduleExecutor(AbstractScheduleExecutor):
    """Default schedule executor: invokes the registered async crontab function."""

    async def execute(self, schedule: models.Schedule, context: models.ScheduleContext) -> None:
        if self.parameters.accepts_context:
            await cast(AsyncContextCrontab, self.parameters.func)(schedule, context)
        else:
            await cast(AsyncCrontab, self.parameters.func)(schedule)
