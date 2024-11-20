# executors.py

from __future__ import annotations

import asyncio
import dataclasses
import functools
import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import (
    Awaitable,
    Callable,
    TypeAlias,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)

import anyio
import anyio.to_thread
from croniter import croniter

from . import db, errors, helpers, models, queries

AsyncEntrypoint: TypeAlias = Callable[[models.Job], Awaitable[None]]
SyncEntrypoint: TypeAlias = Callable[[models.Job], None]
Entrypoint: TypeAlias = AsyncEntrypoint | SyncEntrypoint
EntrypointTypeVar = TypeVar("EntrypointTypeVar", bound=Entrypoint)


AsyncCrontab: TypeAlias = Callable[[models.Schedule], Awaitable[None]]


@overload
def is_async_callable(obj: AsyncEntrypoint) -> TypeGuard[AsyncEntrypoint]: ...


@overload
def is_async_callable(obj: SyncEntrypoint) -> TypeGuard[SyncEntrypoint]: ...


def is_async_callable(obj: object) -> bool:
    """
    Determines whether an object is an asynchronous callable.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (
        callable(obj) and asyncio.iscoroutinefunction(obj.__call__)
    )


######## Entrypoints ########


@dataclasses.dataclass
class EntrypointExecutorParameters:
    channel: models.PGChannel
    concurrency_limit: int
    connection: db.Driver
    func: Entrypoint
    queries: queries.Queries
    requests_per_second: float
    retry_timer: timedelta
    serialized_dispatch: bool
    shutdown: asyncio.Event


@dataclasses.dataclass
class AbstractEntrypointExecutor(ABC):
    """
    Abstract base class for job executors.

    Users can subclass this to create custom job executors.
    """

    parameters: EntrypointExecutorParameters

    @abstractmethod
    async def execute(self, job: models.Job, context: models.Context) -> None:
        """
        Execute the given job.

        Args:
            job (models.Job): The job to execute.
            context (models.Context): The context for the job.
        """


@dataclasses.dataclass
class EntrypointExecutor(AbstractEntrypointExecutor):
    """
    Job executor that wraps an entrypoint function.

    Executes the provided function when processing a job.
    """

    is_async: bool = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.is_async = is_async_callable(self.parameters.func)

    async def execute(self, job: models.Job, context: models.Context) -> None:
        """
        Execute the job using the wrapped function.

        Args:
            job (models.Job): The job to execute.
            context (models.Context): The context for the job.
        """
        if self.is_async:
            await cast(AsyncEntrypoint, self.parameters.func)(job)
        else:
            await anyio.to_thread.run_sync(cast(SyncEntrypoint, self.parameters.func), job)


@dataclasses.dataclass
class RetryWithBackoffEntrypointExecutor(EntrypointExecutor):
    max_attempts: int | None = 5  # maximum retry attempts
    max_delay: float = 10.0  # maximum delay for retry
    max_time: timedelta | None = timedelta(minutes=5)  # maximum time used on retry
    initial_delay: float = 0.1  # base delay for backoff
    backoff_multiplier: float = 2.0  # base for exponential backoff
    jitter: Callable[[], float] = lambda: random.uniform(0, 1)  # jitter callable

    async def execute(self, job: models.Job, context: models.Context) -> None:
        """
        Execute the job with retry logic, using exponential backoff and jitter.

        Args:
            job (models.Job): The job to execute.
            context (models.Context): The context for the job.

        The function retries execution up to `max_attempts` times in case of failure,
        applying exponential backoff with an initial delay (`initial_delay`),
        up to a maximum delay (`max_delay`).
        Jitter is added to the delay to avoid contention.
        """

        attempt = 0

        with helpers.timer() as elapsed:
            while True:
                try:
                    return await super().execute(job, context)
                except Exception as e:
                    attempt += 1
                    if self.max_time and (elp := elapsed()) >= self.max_time:
                        raise errors.MaxTimeExceeded(self.max_time, elp) from e
                    if self.max_attempts and attempt >= self.max_attempts:
                        raise errors.MaxRetriesExceeded(self.max_attempts) from e

                    delay = (
                        self.initial_delay * (self.backoff_multiplier**attempt) / 2
                        + self.jitter() * self.initial_delay / 2
                    )

                    await asyncio.sleep(min(delay, self.max_delay))


######## Schedulers ########


@dataclasses.dataclass
class ScheduleExecutorFactoryParameters:
    connection: db.Driver
    entrypoint: str
    expression: str
    func: AsyncCrontab
    queries: queries.Queries
    shutdown: asyncio.Event


@dataclasses.dataclass
class AbstractScheduleExecutor(ABC):
    """
    Abstract base class for job executors.

    This class provides a blueprint for creating job executors that run according to a schedule.
    Users should subclass this to create custom job executors, defining specific execution logic.
    """

    parameters: ScheduleExecutorFactoryParameters

    @abstractmethod
    async def execute(self, schedule: models.Schedule) -> None:
        """
        Execute the given crontab.

        This method must be implemented by subclasses to define the specific behavior of job
        execution.
        """

    def get_next(self) -> datetime:
        """
        Calculate the next scheduled run time based on the cron expression.

        Returns:
            datetime: The next scheduled datetime in UTC.
        """
        return datetime.fromtimestamp(
            croniter(self.parameters.expression).get_next(),
            timezone.utc,
        )

    def next_in(self) -> timedelta:
        """
        Calculate the time remaining until the next scheduled run.

        Returns:
            timedelta: The time difference between now and the next scheduled run.
        """
        return self.get_next() - helpers.utc_now()


@dataclasses.dataclass
class ScheduleExecutor(AbstractScheduleExecutor):
    """
    Job executor that wraps an entrypoint function.

    This executor runs the provided function according to the defined schedule.
    It is a concrete implementation of AbstractScheduleExecutor.
    """

    async def execute(self, schedule: models.Schedule) -> None:
        """
        Execute the job using the wrapped function.

        This method calls the provided asynchronous function when the job is triggered.
        """
        await self.parameters.func(schedule)
