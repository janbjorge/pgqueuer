"""
Queue Manager module for handling job queues and dispatching jobs.

This module defines the `QueueManager` class, which manages job queues, dispatches
jobs to registered entrypoints, and handles database connections and events.
It also provides utility functions and types for job execution and rate limiting.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import sys
import uuid
import warnings
from collections import Counter, deque
from datetime import timedelta
from math import isfinite
from typing import AsyncGenerator, Callable

import anyio

from . import (
    buffers,
    db,
    executors,
    heartbeat,
    helpers,
    listeners,
    logconfig,
    models,
    qb,
    queries,
    tm,
)

warnings.simplefilter("default", DeprecationWarning)


@dataclasses.dataclass
class QueueManager:
    """
    Manages job queues and dispatches jobs to registered entrypoints.

    The `QueueManager` handles the lifecycle of jobs, including dequeueing,
    dispatching to entrypoint functions, handling rate limiting, concurrency
    limits, and managing cancellations.

    Attributes:
        connection (db.Driver): The database driver used for database operations.
        channel (models.PGChannel): The PostgreSQL channel for notifications.
        shutdown (asyncio.Event): Event to signal when the QueueManager is shutting down.
        queries (queries.Queries): Instance for executing database queries.
        entrypoint_registry (dict[str, JobExecutor]): Registered job executors.
        entrypoint_statistics (dict[str, models.EntrypointStatistics]): Statistics for entrypoints.
        queue_manager_id (uuid.UUID): Unique identifier for each QueueManager instance.
        job_context (dict[models.JobId, models.Context]): Contexts for jobs,
            including cancellation scopes.
    """

    connection: db.Driver
    channel: models.PGChannel = dataclasses.field(
        default=models.PGChannel(qb.DBSettings().channel),
    )

    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    queries: queries.Queries = dataclasses.field(init=False)

    # Per entrypoint
    entrypoint_registry: dict[str, executors.AbstractEntrypointExecutor] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    entrypoint_statistics: dict[str, models.EntrypointStatistics] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    queue_manager_id: uuid.UUID = dataclasses.field(
        init=False,
        default_factory=uuid.uuid4,
    )

    # Per job.
    job_context: dict[models.JobId, models.Context] = dataclasses.field(
        init=False,
        default_factory=dict,
    )

    @property
    def alive(self) -> asyncio.Event:
        # For backwards compatibility
        warnings.warn(
            "The `alive` property is deprecated and will be removed in a future release. "
            "Please use `shutdown` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.shutdown

    def __post_init__(self) -> None:
        """
        Initialize the QueueManager after dataclass fields have been set.

        Sets up the `queries` instance using the provided database connection.
        """
        self.queries = queries.Queries(self.connection)

    def get_context(self, job_id: models.JobId) -> models.Context:
        """
        Retrieve the context associated with a specific job ID.

        Args:
            job_id (models.JobId): The unique identifier of the job.

        Returns:
            models.Context: The context object associated with the job.
        """
        return self.job_context[job_id]

    def register_executor(
        self,
        name: str,
        executor: executors.AbstractEntrypointExecutor,
    ) -> None:
        """
        Register a job executor with a specific name.

        Args:
            name (str): The name of the entrypoint as referenced in the job queue.
            executor (JobExecutor): The job executor instance.

        Raises:
            RuntimeError: If the entrypoint name is already registered.
        """
        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        self.entrypoint_registry[name] = executor
        self.entrypoint_statistics[name] = models.EntrypointStatistics(
            samples=deque(maxlen=1_000),
            concurrency_limiter=asyncio.Semaphore(
                executor.parameters.concurrency_limit or sys.maxsize
            ),
        )

    def entrypoint(
        self,
        name: str,
        *,
        requests_per_second: float = float("inf"),
        concurrency_limit: int = 0,
        retry_timer: timedelta = timedelta(seconds=0),
        serialized_dispatch: bool = False,
        executor: type[executors.AbstractEntrypointExecutor] | None = None,
        executor_factory: Callable[
            [executors.EntrypointExecutorParameters],
            executors.AbstractEntrypointExecutor,
        ]
        | None = None,
    ) -> Callable[[executors.EntrypointTypeVar], executors.EntrypointTypeVar]:
        """
        Decorator to register an entrypoint for job processing.

        Users can specify a custom executor or use the default EntrypointExecutor.

        Args:
            name (str): The name of the entrypoint as referenced in the job queue.
            requests_per_second (float): Max number of jobs per second to process for
                this entrypoint.
            concurrency_limit (int): Max number of concurrent jobs allowed for this entrypoint.
            retry_timer (timedelta): Duration to wait before retrying 'picked' jobs.
            serialized_dispatch (bool): Whether to serialize dispatching of jobs.
            executor (JobExecutor): Custom executor instance to use.

        Returns:
            Callable[[T], T]: A decorator that registers the function as an entrypoint.

        Raises:
            RuntimeError: If the entrypoint name is already registered.
            ValueError: If `requests_per_second` or `concurrency_limit` are negative.
        """

        if executor is not None:
            warnings.warn(
                "The 'executor' parameter is deprecated and will be removed in a future version. "
                "Please use 'executor_factory' instead for custom executor handling.",
                DeprecationWarning,
                stacklevel=3,
            )

        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        # Check requests_per_second type / value range.
        if not isinstance(requests_per_second, (float, int)):
            raise ValueError("Rate must be float | int.")

        if requests_per_second < 0:
            raise ValueError("Rate must be greater or eq. to zero.")

        # Check concurrency_limit type / value range.
        if not isinstance(concurrency_limit, int):
            raise ValueError("Concurrency limit must be int.")

        if concurrency_limit < 0:
            raise ValueError("Concurrency limit must be greater or eq. to zero.")

        # Check serialized_dispatch type.
        if not isinstance(serialized_dispatch, bool):
            raise ValueError("Serialized dispatch must be boolean")

        executor_factory = executor_factory or executors.EntrypointExecutor

        def register(func: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
            self.register_executor(
                name,
                executor_factory(
                    executors.EntrypointExecutorParameters(
                        connection=self.connection,
                        channel=self.channel,
                        shutdown=self.shutdown,
                        queries=self.queries,
                        func=func,
                        requests_per_second=requests_per_second,
                        retry_timer=retry_timer,
                        serialized_dispatch=serialized_dispatch,
                        concurrency_limit=concurrency_limit,
                    )
                ),
            )
            return func

        return register

    def observed_requests_per_second(
        self,
        entrypoint: str,
        epsilon: timedelta = timedelta(milliseconds=0.01),
    ) -> float:
        """
        Calculate the observed requests per second for an entrypoint.

        Args:
            entrypoint (str): The entrypoint to calculate the rate for.
            epsilon (timedelta): Small time delta to prevent division by zero.

        Returns:
            float: The observed requests per second.
        """
        samples = self.entrypoint_statistics[entrypoint].samples
        if not samples:
            return 0.0
        timespan = helpers.utc_now() - min(t for _, t in samples) + epsilon
        requests = sum(c for c, _ in samples)
        return requests / timespan.total_seconds()

    def entrypoints_below_capacity_limits(self) -> set[str]:
        """
        Determine which entrypoints are below their configured capacity limits.

        Returns:
            set[str]: A set of entrypoint names that are below capacity.
        """
        return {
            entrypoint
            for entrypoint, executor in self.entrypoint_registry.items()
            if self.observed_requests_per_second(entrypoint)
            < executor.parameters.requests_per_second
            and not self.entrypoint_statistics[entrypoint].concurrency_limiter.locked()
        }

    async def fetch_jobs(self, batch_size: int) -> AsyncGenerator[models.Job, None]:
        """
        Fetch jobs from the queue that are ready for processing, yielding them one at a time.

        This method retrieves a batch of jobs that match the current capacity constraints
        of each entrypoint. It ensures that jobs are fetched only when the QueueManager
        is operational and that rate limits and concurrency controls are respected.
        """

        while not self.shutdown.is_set():
            entrypoints = {
                x: queries.EntrypointExecutionParameter(
                    retry_after=self.entrypoint_registry[x].parameters.retry_timer,
                    serialized=self.entrypoint_registry[x].parameters.serialized_dispatch,
                    concurrency_limit=self.entrypoint_registry[x].parameters.concurrency_limit,
                )
                for x in self.entrypoints_below_capacity_limits()
            }

            if not (
                jobs := await self.queries.dequeue(
                    batch_size=batch_size,
                    entrypoints=entrypoints,
                    queue_manager_id=self.queue_manager_id,
                )
            ):
                break

            for job in jobs:
                yield job

    async def flush_rps(self, events: list[str]) -> None:
        """Update rate-per-second statistics for the given entrypoints."""
        if events:
            await self.queries.notify_debounce_event(
                {
                    k: v
                    for k, v in Counter(events).items()
                    if isfinite(self.entrypoint_registry[k].parameters.requests_per_second)
                }
            )

    async def verify_structure(self) -> None:
        """
        Verify the required database structure.

        Checks necessary columns and user-defined types. Raises RuntimeError if missing.
        """

        for table in (
            qb.add_prefix("pgqueuer"),
            qb.add_prefix("pgqueuer_statistics"),
        ):
            if not (await self.queries.has_table(table)):
                raise RuntimeError(
                    f"The required table '{table}' is missing. "
                    f"Please run 'pgq install' to set up the necessary tables."
                )

        for table, column in (
            (self.queries.qbe.settings.queue_table, "updated"),
            (self.queries.qbe.settings.queue_table, "heartbeat"),
            (self.queries.qbe.settings.queue_table, "queue_manager_id"),
            (self.queries.qbe.settings.queue_table, "execute_after"),
        ):
            if not (await self.queries.table_has_column(table, column)):
                raise RuntimeError(
                    f"The required column '{column}' is missing in the '{table}' table. "
                    f"Please run 'pgq upgrade' to ensure all schema changes are applied."
                )

        for key, enum in (("canceled", self.queries.qbe.settings.statistics_table_status_type),):
            if not (await self.queries.has_user_defined_enum(key, enum)):
                raise RuntimeError(
                    f"The {enum} is missing the '{key}' type, please run 'pgq upgrade'"
                )

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
    ) -> None:
        """
        Run the main loop to process jobs from the queue.

        Continuously listens for events and dispatches jobs, managing connections
        and tasks, logging timeouts, and resetting connections upon termination.

        Args:
            dequeue_timeout (timedelta): Timeout duration for waiting to dequeue jobs.
            batch_size (int): Number of jobs to retrieve in each batch.

        Raises:
            RuntimeError: If required database columns or types are missing.
        """

        await self.verify_structure()

        job_status_log_buffer_timeout = timedelta(seconds=0.01)
        heartbeat_buffer_timeout = helpers.retry_timer_buffer_timeout(
            [x.parameters.retry_timer for x in self.entrypoint_registry.values()]
        )

        async with (
            buffers.JobStatusLogBuffer(
                max_size=batch_size,
                timeout=job_status_log_buffer_timeout,
                callback=self.queries.log_jobs,
            ) as jbuff,
            buffers.HeartbeatBuffer(
                max_size=sys.maxsize,
                timeout=heartbeat_buffer_timeout / 2,
                callback=self.queries.update_heartbeat,
            ) as hbuff,
            buffers.RequestsPerSecondBuffer(
                max_size=batch_size,
                timeout=timedelta(seconds=0.01),
                callback=self.flush_rps,
            ) as rpsbuff,
            tm.TaskManager() as task_manager,
            self.connection,
        ):
            notice_event_listener = listeners.PGNoticeEventListener()
            await listeners.initialize_notice_event_listener(
                self.connection,
                self.channel,
                functools.partial(
                    listeners.handle_event_type,
                    notice_event_queue=notice_event_listener,
                    statistics=self.entrypoint_statistics,
                    canceled=self.job_context,
                ),
            )

            shutdown_task = asyncio.create_task(self.shutdown.wait())
            event_task: None | asyncio.Task[None | models.TableChangedEvent] = None

            while not self.shutdown.is_set():
                async for job in self.fetch_jobs(batch_size):
                    await rpsbuff.add(job.entrypoint)
                    self.job_context[job.id] = models.Context(cancellation=anyio.CancelScope())
                    task_manager.add(
                        asyncio.create_task(
                            self._dispatch(
                                job,
                                jbuff,
                                hbuff,
                            )
                        )
                    )

                    with contextlib.suppress(asyncio.QueueEmpty):
                        notice_event_listener.get_nowait()

                event_task = helpers.wait_for_notice_event(
                    notice_event_listener,
                    dequeue_timeout,
                )
                await asyncio.wait(
                    (shutdown_task, event_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )

        if event_task and not event_task.done():
            event_task.cancel()

        if not shutdown_task.done():
            shutdown_task.cancel()

    async def _dispatch(
        self,
        job: models.Job,
        jbuff: buffers.JobStatusLogBuffer,
        hbuff: buffers.HeartbeatBuffer,
    ) -> None:
        """
        Dispatch a job to its associated entrypoint executor.

        Handles asynchronous job execution, logs exceptions, updates job status,
        and manages job context including cancellation.

        Args:
            job (models.Job): The job to dispatch.
            jbuff (buffers.JobStatusLogBuffer): Buffer to log job completion status.
            hbuff (buffers.HeartbeatBuffer): Buffer to manage heartbeats.
        """

        logconfig.logger.debug(
            "Dispatching entrypoint/id: %s/%s",
            job.entrypoint,
            job.id,
        )

        executor = self.entrypoint_registry[job.entrypoint]

        async with (
            heartbeat.Heartbeat(
                job.id,
                executor.parameters.retry_timer,
                hbuff,
            ),
            self.entrypoint_statistics[job.entrypoint].concurrency_limiter,
        ):
            try:
                # Run the job unless it has already been cancelled. Check this here because jobs
                # can be cancelled between when they are dequeued and when they acquire the
                # concurrency limit semaphore.
                ctx = self.get_context(job.id)
                if not ctx.cancellation.cancel_called:
                    await executor.execute(job, ctx)
            except Exception:
                logconfig.logger.exception(
                    "Exception while processing entrypoint/job-id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                await jbuff.add((job, "exception"))
            else:
                logconfig.logger.debug(
                    "Dispatching entrypoint/id: %s/%s - successful",
                    job.entrypoint,
                    job.id,
                )
                canceled = ctx.cancellation.cancel_called
                await jbuff.add((job, "canceled" if canceled else "successful"))
            finally:
                self.job_context.pop(job.id, None)
