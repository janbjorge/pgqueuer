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
import sys
import warnings
from collections import Counter, deque
from datetime import timedelta
from math import isfinite
from typing import Callable

import anyio

from . import buffers, db, executors, heartbeat, helpers, listeners, logconfig, models, queries, tm


class Unset: ...


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
        alive (asyncio.Event): Event to signal when the QueueManager is shutting down.
        queries (queries.Queries): Instance for executing database queries.
        entrypoint_registry (dict[str, JobExecutor]): Registered job executors.
        entrypoint_statistics (dict[str, models.EntrypointStatistics]): Statistics for entrypoints.
        job_context (dict[models.JobId, models.Context]): Contexts for jobs,
            including cancellation scopes.
    """

    connection: db.Driver
    channel: models.PGChannel = dataclasses.field(
        default=models.PGChannel(queries.DBSettings().channel),
    )

    alive: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    queries: queries.Queries = dataclasses.field(init=False)

    # Per entrypoint
    entrypoint_registry: dict[str, executors.JobExecutor] = dataclasses.field(
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
        executor: executors.JobExecutor,
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
            concurrency_limiter=asyncio.Semaphore(executor.concurrency_limit or sys.maxsize),
        )

    def entrypoint(
        self,
        name: str,
        *,
        requests_per_second: float = float("inf"),
        concurrency_limit: int = 0,
        retry_timer: timedelta = timedelta(seconds=0),
        serialized_dispatch: bool = False,
        executor: type[executors.JobExecutor] = executors.EntrypointExecutor,
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

        def register(func: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
            self.register_executor(
                name,
                executor(
                    func=func,
                    requests_per_second=requests_per_second,
                    retry_timer=retry_timer,
                    serialized_dispatch=serialized_dispatch,
                    concurrency_limit=concurrency_limit,
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
        timespan = helpers.perf_counter_dt() - min(t for _, t in samples) + epsilon
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
            if self.observed_requests_per_second(entrypoint) < executor.requests_per_second
            and not self.entrypoint_statistics[entrypoint].concurrency_limiter.locked()
        }

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        retry_timer: timedelta | None | Unset = Unset(),
    ) -> None:
        """
        Run the main loop to process jobs from the queue.

        Continuously listens for events and dispatches jobs, managing connections
        and tasks, logging timeouts, and resetting connections upon termination.

        Args:
            dequeue_timeout (timedelta): Timeout duration for waiting to dequeue jobs.
            batch_size (int): Number of jobs to retrieve in each batch.
            retry_timer (timedelta | None): Duration to wait before retrying 'picked' jobs.

        Raises:
            RuntimeError: If required database columns or types are missing.
        """

        if not isinstance(retry_timer, Unset):
            warnings.warn(
                "retry_timer is deprecated, use retry_timer with the entrypoint decorator.",
                DeprecationWarning,
            )

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

        if not (await self.queries.has_heartbeat_column()):
            raise RuntimeError(
                f"The {self.queries.qb.settings.queue_table} table is missing the "
                "heartbeat column, please run 'python3 -m pgqueuer upgrade'"
            )
        job_status_log_buffer_timeout = timedelta(seconds=0.01)
        heartbeat_buffer_timeout = helpers.retry_timer_buffer_timeout(
            [x.retry_timer for x in self.entrypoint_registry.values()]
        )
        async with (
            buffers.JobStatusLogBuffer(
                max_size=batch_size,
                timeout=job_status_log_buffer_timeout,
                flush_callable=self.queries.log_jobs,
            ) as jbuff,
            buffers.HeartbeatBuffer(
                max_size=sys.maxsize,
                timeout=heartbeat_buffer_timeout,
                flush_callable=self.queries.notify_activity,
            ) as hbuff,
            tm.TaskManager() as task_manager,
            self.connection,
        ):
            notice_event_listener = await listeners.initialize_notice_event_listener(
                self.connection,
                self.channel,
                self.entrypoint_statistics,
                self.job_context,
            )

            alive_task = asyncio.create_task(self.alive.wait())

            while not self.alive.is_set():
                entrypoints = {
                    x: (
                        self.entrypoint_registry[x].retry_timer,
                        self.entrypoint_registry[x].serialized_dispatch,
                    )
                    for x in self.entrypoints_below_capacity_limits()
                }

                jobs = await self.queries.dequeue(
                    batch_size=batch_size,
                    entrypoints=entrypoints,
                )

                entrypoint_tally = Counter[str]()

                for job in jobs:
                    self.job_context[job.id] = models.Context(
                        cancellation=anyio.CancelScope(),
                    )
                    entrypoint_tally[job.entrypoint] += 1

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

                for entrypoint, count in entrypoint_tally.items():
                    # skip if rate is inf.
                    rps = self.entrypoint_registry[entrypoint].requests_per_second
                    if isfinite(rps):
                        task_manager.add(
                            asyncio.create_task(
                                self.queries.notify_debounce_event(
                                    entrypoint,
                                    count,
                                )
                            )
                        )

                event_task = helpers.wait_for_notice_event(
                    notice_event_listener,
                    dequeue_timeout,
                )
                await asyncio.wait(
                    (alive_task, event_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )

        if not event_task.done():
            event_task.cancel()

        if not alive_task.done():
            alive_task.cancel()

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
                executor.retry_timer / 2,
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
                    await executor.execute(job)
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
