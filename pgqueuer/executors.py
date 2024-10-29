# executors.py

from __future__ import annotations

import asyncio
import functools
from abc import ABC, abstractmethod
from datetime import timedelta
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

from . import models

AsyncEntrypoint: TypeAlias = Callable[[models.Job], Awaitable[None]]
SyncEntrypoint: TypeAlias = Callable[[models.Job], None]
Entrypoint: TypeAlias = AsyncEntrypoint | SyncEntrypoint
EntrypointTypeVar = TypeVar("EntrypointTypeVar", bound=Entrypoint)


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
