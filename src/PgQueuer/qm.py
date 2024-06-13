from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    TypeAlias,
    TypeGuard,
    TypeVar,
    overload,
)

import anyio
import anyio.to_thread

from .buffers import JobBuffer
from .db import Driver
from .listeners import initialize_event_listener
from .logconfig import logger
from .models import Job, PGChannel
from .queries import DBSettings, Queries
from .tm import TaskManager

if TYPE_CHECKING:
    AsyncEntrypoint: TypeAlias = Callable[[Job], Awaitable[None]]
    SyncEntrypoint: TypeAlias = Callable[[Job], None]
    Entrypoint: TypeAlias = AsyncEntrypoint | SyncEntrypoint
    T = TypeVar("T", bound=Entrypoint)


@overload
def is_async_callable(obj: AsyncEntrypoint) -> TypeGuard[AsyncEntrypoint]: ...


@overload
def is_async_callable(obj: SyncEntrypoint) -> TypeGuard[SyncEntrypoint]: ...


def is_async_callable(obj: object) -> bool:
    """
    Determines whether an object is an asynchronous callable.
    This function identifies objects that are either asynchronous coroutine functions
    or have a callable __call__ method that is an asynchronous coroutine function.

    The function handles functools.partial objects by evaluating the
    underlying function. It supports overloads to ensure type-specific behavior
    with AsyncEntrypoint and SyncEntrypoint types.

    Inspired by:
    https://github.com/encode/starlette/blob/9f16bf5c25e126200701f6e04330864f4a91a898/starlette/_utils.py#L38
    """

    while isinstance(obj, functools.partial):
        obj = obj.func

    return asyncio.iscoroutinefunction(obj) or (
        callable(obj) and asyncio.iscoroutinefunction(obj.__call__)
    )


@dataclasses.dataclass
class QueueManager:
    """
    Manages job queues and dispatches jobs to registered entry points,
    handling database connections and events.
    """

    connection: Driver
    channel: PGChannel = dataclasses.field(default=PGChannel(DBSettings().channel))

    alive: bool = dataclasses.field(
        init=False,
        default=True,
    )
    buffer: JobBuffer = dataclasses.field(init=False)
    queries: Queries = dataclasses.field(init=False)
    registry: dict[str, Entrypoint] = dataclasses.field(
        init=False, default_factory=dict
    )

    def __post_init__(self) -> None:
        """
        Initializes database query handlers and validates pool size upon
        instance creation.
        """
        self.queries = Queries(self.connection)
        self.buffer = JobBuffer(
            max_size=10,
            timeout=timedelta(seconds=0.01),
            flush_callback=self.queries.log_jobs,
        )

    def entrypoint(self, name: str) -> Callable[[T], T]:
        """
        Registers a function as an entrypoint for handling specific
        job types. Ensures unique naming in the registry.
        """

        if name in self.registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        def register(func: T) -> T:
            self.registry[name] = func
            return func

        return register

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        retry_timer: timedelta | None = None,
    ) -> None:
        """
        Continuously listens for events and dispatches jobs. Manages connections and
        tasks, logs timeouts, and resets connections upon termination.

        Parameters:
            - dequeue_timeout: The timeout duration for waiting to dequeue jobs.
            Defaults to 30 seconds.
            - batch_size : The number of jobs to retrieve in each batch. Defaults to 10.
            - retry_timer: If specified, selects jobs that have been in 'picked' status
            for longer than the specified retry timer duration. If `None`, the timeout
            job checking is skipped.
        """

        if not (await self.queries.has_updated_column()):
            raise RuntimeError(
                f"The {self.queries.qb.settings.queue_table} table is missing the "
                "updated column, please run 'python3 -m PgQueuer upgrade'"
            )

        self.buffer.max_size = batch_size

        async with TaskManager() as tm:
            tm.add(asyncio.create_task(self.buffer.monitor()))
            listener = await initialize_event_listener(self.connection, self.channel)

            while self.alive:
                while self.alive and (
                    jobs := await self.queries.dequeue(
                        batch_size=batch_size,
                        entrypoints=set(self.registry.keys()),
                        retry_timer=retry_timer,
                    )
                ):
                    for job in jobs:
                        tm.add(asyncio.create_task(self._dispatch(job)))
                        with contextlib.suppress(asyncio.QueueEmpty):
                            listener.get_nowait()

                try:
                    await asyncio.wait_for(
                        listener.get(),
                        timeout=dequeue_timeout.total_seconds(),
                    )
                except asyncio.TimeoutError:
                    logger.debug(
                        "Timeout after %r without receiving an event.",
                        dequeue_timeout,
                    )
            self.buffer.alive = False

    async def _dispatch(self, job: Job) -> None:
        """
        Handles asynchronous job dispatch. Logs exceptions, updates job status,
        and adapts execution method based on whether the job's function is asynchronous.
        """

        logger.debug(
            "Dispatching entrypoint/id: %s/%s",
            job.entrypoint,
            job.id,
        )

        try:
            fn = self.registry[job.entrypoint]
            if is_async_callable(fn):
                await fn(job)
            else:
                await anyio.to_thread.run_sync(fn, job)
        except Exception:
            logger.exception(
                "Exception while processing entrypoint/id: %s/%s",
                job.entrypoint,
                job.id,
            )
            await self.buffer.add_job(job, "exception")
        else:
            logger.debug(
                "Dispatching entrypoint/id: %s/%s - successful",
                job.entrypoint,
                job.id,
            )
            await self.buffer.add_job(job, "successful")
