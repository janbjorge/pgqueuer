from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
from collections import Counter, deque
from datetime import datetime, timedelta
from math import isfinite
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

from .buffers import JobBuffer
from .db import Driver
from .helpers import perf_counter_dt
from .listeners import initialize_notice_event_listener
from .logconfig import logger
from .models import Context, Job, JobId, PGChannel
from .queries import DBSettings, Queries
from .tm import TaskManager

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


class JobExecutor:
    def __init__(
        self,
        func: Entrypoint,
        requests_per_second: float,
    ) -> None:
        self.func = func
        self.requests_per_second = requests_per_second
        self.is_async = is_async_callable(func)

    async def __call__(self, job: Job) -> None:
        if self.is_async:
            await cast(AsyncEntrypoint, self.func)(job)
        else:
            await anyio.to_thread.run_sync(cast(SyncEntrypoint, self.func), job)


@dataclasses.dataclass
class QueueManager:
    """
    Manages job queues and dispatches jobs to registered entry points,
    handling database connections and events.
    """

    connection: Driver
    channel: PGChannel = dataclasses.field(
        default=PGChannel(DBSettings().channel),
    )

    alive: bool = dataclasses.field(
        init=False,
        default=True,
    )
    buffer: JobBuffer = dataclasses.field(init=False)
    queries: Queries = dataclasses.field(init=False)
    registry: dict[str, JobExecutor] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    # dict[entrypoint, [count, timestamp]]
    statistics: dict[str, deque[tuple[int, datetime]]] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    job_context: dict[JobId, Context] = dataclasses.field(
        init=False,
        default_factory=dict,
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

    def get_context(self, job_id: JobId) -> Context:
        """
        Retrieves the cancellation scope for a specific job, allowing the job to be checked
        and managed for cancellation.
        """
        return self.job_context[job_id]

    def entrypoint(
        self,
        name: str,
        requests_per_second: float = float("inf"),
    ) -> Callable[[T], T]:
        """
        Registers a function as an entrypoint for handling specific
        job types. Ensures unique naming in the registry.
        """

        if name in self.registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        if requests_per_second < 0:
            raise ValueError("Rate must be greater or eq. to zero.")

        def register(func: T) -> T:
            self.registry[name] = JobExecutor(
                func=func,
                requests_per_second=requests_per_second,
            )
            self.statistics[name] = deque(maxlen=1_000)
            return func

        return register

    def observed_requests_per_second(
        self,
        entrypoint: str,
        epsilon: timedelta = timedelta(milliseconds=0.01),
    ) -> float:
        samples = self.statistics[entrypoint]
        if not samples:
            return 0.0
        timespan = perf_counter_dt() - min(t for _, t in samples) + epsilon
        requests = sum(c for c, _ in samples)
        return requests / timespan.total_seconds()

    def entrypoints_below_requests_per_second(self) -> set[str]:
        return {
            entrypoint
            for entrypoint, fn in self.registry.items()
            if self.observed_requests_per_second(entrypoint) < fn.requests_per_second
        }

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
                "updated column, please run 'python3 -m pgqueuer upgrade'"
            )

        if not (
            await self.queries.has_user_type(
                "canceled", self.queries.qb.settings.statistics_table_status_type
            )
        ):
            raise RuntimeError(
                f"The {self.queries.qb.settings.statistics_table_status_type} is missing the "
                "'canceled' type, please run 'python3 -m pgqueuer upgrade'"
            )

        self.buffer.max_size = batch_size

        async with TaskManager() as tm:
            tm.add(asyncio.create_task(self.buffer.monitor()))
            notice_event_listener = await initialize_notice_event_listener(
                self.connection,
                self.channel,
                self.statistics,
                self.job_context,
            )

            while self.alive:
                jobs = await self.queries.dequeue(
                    batch_size=batch_size,
                    entrypoints=self.entrypoints_below_requests_per_second(),
                    retry_timer=retry_timer,
                )

                entrypoint_tally = Counter[str]()

                for job in jobs:
                    self.job_context[job.id] = Context(
                        cancellation=anyio.CancelScope(),
                    )
                    tm.add(asyncio.create_task(self._dispatch(job)))
                    entrypoint_tally[job.entrypoint] += 1
                    with contextlib.suppress(asyncio.QueueEmpty):
                        notice_event_listener.get_nowait()

                for entrypoint, count in entrypoint_tally.items():
                    # skip if rate is inf.
                    rps = self.registry[entrypoint].requests_per_second
                    if isfinite(rps):
                        tm.add(
                            asyncio.create_task(
                                self.queries.notify_debounce_event(
                                    entrypoint,
                                    count,
                                )
                            )
                        )

                try:
                    await asyncio.wait_for(
                        notice_event_listener.get(),
                        timeout=dequeue_timeout.total_seconds(),
                    )
                except asyncio.TimeoutError:
                    logger.debug(
                        "Timeout after %r without receiving an event.",
                        dequeue_timeout,
                    )

            self.buffer.alive = False
            self.connection.alive = False

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
            await self.registry[job.entrypoint](job)
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
        finally:
            self.job_context.pop(job.id, None)
