from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import sys
from collections import Counter, deque
from datetime import timedelta
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

from . import buffers, db, helpers, listeners, logconfig, models, queries, tm

AsyncEntrypoint: TypeAlias = Callable[[models.Job], Awaitable[None]]
SyncEntrypoint: TypeAlias = Callable[[models.Job], None]
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

    async def __call__(self, job: models.Job) -> None:
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

    connection: db.Driver
    channel: models.PGChannel = dataclasses.field(
        default=models.PGChannel(queries.DBSettings().channel),
    )

    alive: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    buffer: buffers.JobBuffer = dataclasses.field(init=False)
    queries: queries.Queries = dataclasses.field(init=False)

    # Per entrypoint
    entrypoint_registry: dict[str, JobExecutor] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    entrypoint_statistics: dict[str, models.EntrypointStatistics] = dataclasses.field(
        init=False,
        default_factory=dict,
    )

    # Per job.
    job_context: dict[models.JobId, models.Context] = dataclasses.field(
        init=False,
        default_factory=dict,
    )

    def __post_init__(self) -> None:
        """
        Initializes database query handlers and validates pool size upon
        instance creation.
        """
        self.queries = queries.Queries(self.connection)
        self.buffer = buffers.JobBuffer(
            queries=self.queries,
            max_size=10,
            timeout=timedelta(seconds=0.01),
        )

    def get_context(self, job_id: models.JobId) -> models.Context:
        """
        Retrieves the cancellation scope for a specific job, allowing the job to be checked
        and managed for cancellation.
        """
        return self.job_context[job_id]

    def entrypoint(
        self,
        name: str,
        requests_per_second: float = float("inf"),
        concurrency_limit: int = 0,
    ) -> Callable[[T], T]:
        """
        Registers a function as an entrypoint for handling specific
        job types. Ensures unique naming in the registry.

        Parameters:
            - name: The name used to identify the entrypoint in the database.
            - requests_per_second: The upper limit on the number of jobs that will
                be processed for this entrypoint accross all consumers.
            - concurrency_limit: The upper limit on the number of concurrent jobs
                that can be executed for this entrypoint in any one consumer.
        """

        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        if requests_per_second < 0:
            raise ValueError("Rate must be greater or eq. to zero.")

        if concurrency_limit < 0:
            raise ValueError("Concurrency limit must be greater or eq. to zero.")

        def register(func: T) -> T:
            self.entrypoint_registry[name] = JobExecutor(
                func=func,
                requests_per_second=requests_per_second,
            )
            self.entrypoint_statistics[name] = models.EntrypointStatistics(
                samples=deque(maxlen=1_000),
                concurrency_limiter=asyncio.Semaphore(concurrency_limit or sys.maxsize),
            )
            return func

        return register

    def observed_requests_per_second(
        self,
        entrypoint: str,
        epsilon: timedelta = timedelta(milliseconds=0.01),
    ) -> float:
        samples = self.entrypoint_statistics[entrypoint].samples
        if not samples:
            return 0.0
        timespan = helpers.perf_counter_dt() - min(t for _, t in samples) + epsilon
        requests = sum(c for c, _ in samples)
        return requests / timespan.total_seconds()

    def entrypoints_below_capacity_limits(self) -> set[str]:
        return {
            entrypoint
            for entrypoint, fn in self.entrypoint_registry.items()
            if self.observed_requests_per_second(entrypoint) < fn.requests_per_second
            and not self.entrypoint_statistics[entrypoint].concurrency_limiter.locked()
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

        async with tm.TaskManager() as task_manger:
            task_manger.add(asyncio.create_task(self.buffer.monitor()))
            notice_event_listener = await listeners.initialize_notice_event_listener(
                self.connection,
                self.channel,
                self.entrypoint_statistics,
                self.job_context,
            )

            alive_task = asyncio.create_task(self.alive.wait())

            while not self.alive.is_set():
                jobs = await self.queries.dequeue(
                    batch_size=batch_size,
                    entrypoints=self.entrypoints_below_capacity_limits(),
                    retry_timer=retry_timer,
                )

                entrypoint_tally = Counter[str]()

                for job in jobs:
                    self.job_context[job.id] = models.Context(
                        cancellation=anyio.CancelScope(),
                    )
                    task_manger.add(asyncio.create_task(self._dispatch(job)))
                    entrypoint_tally[job.entrypoint] += 1
                    with contextlib.suppress(asyncio.QueueEmpty):
                        notice_event_listener.get_nowait()

                for entrypoint, count in entrypoint_tally.items():
                    # skip if rate is inf.
                    rps = self.entrypoint_registry[entrypoint].requests_per_second
                    if isfinite(rps):
                        task_manger.add(
                            asyncio.create_task(
                                self.queries.notify_debounce_event(
                                    entrypoint,
                                    count,
                                )
                            )
                        )

                try:
                    event_task = asyncio.create_task(
                        asyncio.wait_for(
                            notice_event_listener.get(),
                            timeout=dequeue_timeout.total_seconds(),
                        )
                    )
                    await asyncio.wait(
                        (alive_task, event_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except asyncio.TimeoutError:
                    logconfig.logger.debug(
                        "Timeout after %r without receiving an event.",
                        dequeue_timeout,
                    )

            self.buffer.alive.set()

            self.connection.alive.set()
            await self.connection.tm.gather_tasks()

        await self.buffer.flush_jobs()

    async def _dispatch(self, job: models.Job) -> None:
        """
        Handles asynchronous job dispatch. Logs exceptions, updates job status,
        and adapts execution method based on whether the job's function is asynchronous.
        """

        logconfig.logger.debug(
            "Dispatching entrypoint/id: %s/%s",
            job.entrypoint,
            job.id,
        )

        async with self.entrypoint_statistics[job.entrypoint].concurrency_limiter:
            try:
                # Run the job unless it has already been cancelled. Check this here because jobs
                # can be cancelled between when they are dequeued and when the acquire the
                # concurrency limit semaphore.
                if not self.get_context(job.id).cancellation.cancel_called:
                    await self.entrypoint_registry[job.entrypoint](job)
            except Exception:
                logconfig.logger.exception(
                    "Exception while processing entrypoint/id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                await self.buffer.add_job(job, "exception")
            else:
                logconfig.logger.debug(
                    "Dispatching entrypoint/id: %s/%s - successful",
                    job.entrypoint,
                    job.id,
                )
                canceled = self.get_context(job.id).cancellation.cancel_called
                await self.buffer.add_job(job, "canceled" if canceled else "successful")
            finally:
                self.job_context.pop(job.id, None)
