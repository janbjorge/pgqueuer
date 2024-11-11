# executors.py

from __future__ import annotations

import asyncio
import dataclasses
import functools
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

from . import helpers, models

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


class JobExecutor(ABC):
    """
    Abstract base class for job executors.

    Users can subclass this to create custom job executors.
    """

    def __init__(
        self,
        func: Entrypoint,
        requests_per_second: float,
        retry_timer: timedelta,
        serialized_dispatch: bool,
        concurrency_limit: int,
    ):
        super().__init__()
        self.func = func
        self.requests_per_second = requests_per_second
        self.retry_timer = retry_timer
        self.serialized_dispatch = serialized_dispatch
        self.concurrency_limit = concurrency_limit

    @abstractmethod
    async def execute(self, job: models.Job) -> None:
        """
        Execute the given job.

        Args:
            job (models.Job): The job to execute.
        """


class EntrypointExecutor(JobExecutor):
    """
    Job executor that wraps an entrypoint function.

    Executes the provided function when processing a job.
    """

    def __init__(
        self,
        func: Entrypoint,
        requests_per_second: float = float("inf"),
        retry_timer: timedelta = timedelta(seconds=0),
        serialized_dispatch: bool = False,
        concurrency_limit: int = 0,
    ) -> None:
        self.func = func
        self.requests_per_second = requests_per_second
        self.retry_timer = retry_timer
        self.serialized_dispatch = serialized_dispatch
        self.concurrency_limit = concurrency_limit
        self.is_async = is_async_callable(func)

    async def execute(self, job: models.Job) -> None:
        """
        Execute the job using the wrapped function.

        Args:
            job (models.Job): The job to execute.
        """
        if self.is_async:
            await cast(AsyncEntrypoint, self.func)(job)
        else:
            await anyio.to_thread.run_sync(cast(SyncEntrypoint, self.func), job)


@dataclasses.dataclass
class AbstractScheduleExecutor(ABC):
    """
    Abstract base class for job executors.

    This class provides a blueprint for creating job executors that run according to a schedule.
    Users should subclass this to create custom job executors, defining specific execution logic.
    """

    name: str
    expression: str
    func: AsyncCrontab

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
            croniter(self.expression).get_next(),
            timezone.utc,
        )

    def next_in(self) -> timedelta:
        """
        Calculate the time remaining until the next scheduled run.

        Returns:
            timedelta: The time difference between now and the next scheduled run.
        """
        return self.get_next() - helpers.utc_now()


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
        await self.func(schedule)
