"""
Queue Manager module for handling job queues and dispatching jobs.

This module defines the `QueueManager` class, which manages job queues, dispatches
jobs to registered entrypoints, and handles database connections and events.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import sys
import uuid
from collections.abc import MutableMapping
from contextlib import nullcontext, suppress
from datetime import timedelta
from typing import AsyncGenerator, Callable, get_args

import anyio

from pgqueuer.adapters.persistence import qb, queries
from pgqueuer.core import (
    buffers,
    cache,
    executors,
    heartbeat,
    listeners,
    logconfig,
    tm,
)
from pgqueuer.domain import errors, models, types
from pgqueuer.ports import RepositoryPort, tracing
from pgqueuer.ports.driver import Driver
from pgqueuer.ports.repository import EntrypointExecutionParameter


@dataclasses.dataclass
class QueueManager:
    """
    Manages job queues and dispatches jobs to registered entrypoints.

    The `QueueManager` handles the lifecycle of jobs, including dequeueing,
    dispatching to entrypoint functions, handling concurrency limits, and
    managing cancellations.

    Attributes:
        connection (db.Driver): The database driver used for database operations.
        channel (Channel): The PostgreSQL channel for notifications.
        shutdown (asyncio.Event): Event to signal when the QueueManager is shutting down.
        queries (queries.Queries): Instance for executing database queries.
        entrypoint_registry (dict[str, JobExecutor]): Registered job executors.
        queue_manager_id (uuid.UUID): Unique identifier for each QueueManager instance.
        job_context (dict[models.JobId, models.Context]): Contexts for jobs,
            including cancellation scopes.
        resources (MutableMapping[str, Any]): User-provided shared resources mapping
            injected into each job Context. Pass at construction time to share
            initialized pools/clients/models across jobs.
        shutdown_on_listener_failure
            If *True*, raise `FailingListenerError` and set the shutdown event when
            the periodic listener health-check times out.
    """

    connection: Driver
    channel: models.Channel = dataclasses.field(
        default=models.Channel(qb.DBSettings().channel),
    )

    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    queries: RepositoryPort = dataclasses.field(default=None)  # type: ignore[assignment]

    # Per entrypoint
    entrypoint_registry: dict[str, executors.AbstractEntrypointExecutor] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    queue_manager_id: uuid.UUID = dataclasses.field(
        init=False,
        default_factory=uuid.uuid4,
    )
    # Shared resources mapping propagated into each job Context.
    resources: MutableMapping = dataclasses.field(
        default_factory=dict,
    )

    # Optional injected tracer; falls back to the global ``tracing.TRACER.tracer``.
    tracer: tracing.TracingProtocol | None = None

    # Per job.
    job_context: dict[models.JobId, models.Context] = dataclasses.field(
        init=False,
        default_factory=dict,
    )

    pending_health_check: dict[uuid.UUID, asyncio.Future[models.HealthCheckEvent]] = (
        dataclasses.field(
            init=False,
            default_factory=dict,
        )
    )

    # Minimum sleep before re-checking the queue. Prevents busy-looping when a
    # deferred job's ETA is near zero or when the TOCTOU fallback detects
    # eligible work that the preceding dequeue narrowly missed.
    min_dequeue_poll_interval: timedelta = dataclasses.field(
        init=False,
        default=timedelta(seconds=0.1),
    )

    async def listener_healthy(
        self,
        timeout: timedelta = timedelta(seconds=10),
    ) -> models.HealthCheckEvent:
        """
        Perform a health check by sending a notification and waiting for a response.

        Args:
            timeout (timedelta): Maximum time to wait for the health check response.

        Returns:
            models.HealthCheckEvent: The received health check event.

        Raises:
            FailingListenerError: If the health check times out.
        """
        health_check_event_id = uuid.uuid4()
        fut = asyncio.Future[models.HealthCheckEvent]()
        self.pending_health_check[health_check_event_id] = fut

        await self.queries.notify_health_check(health_check_event_id)
        try:
            return await asyncio.wait_for(
                asyncio.shield(fut),
                timeout.total_seconds(),
            )
        except (TimeoutError, asyncio.TimeoutError):
            raise errors.FailingListenerError from None
        finally:
            self.pending_health_check.pop(health_check_event_id, None)

    async def _run_periodic_health_check(
        self,
        interval: timedelta = timedelta(seconds=10),
    ) -> None:
        """
        Periodically perform a health check by calling `listener_healthy`.

        Args:
            interval (timedelta): Time interval between health checks.
            shutdown_on_failure (bool): Whether to set the shutdown event if a health check fails.
        """

        while not self.shutdown.is_set():
            await self.listener_healthy(timeout=interval)
            with suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=interval.total_seconds(),
                )

    def __post_init__(self) -> None:
        """
        Initialize the QueueManager after dataclass fields have been set.

        Sets up the `queries` instance using the provided database connection
        when no queries instance was injected.
        """
        if self.queries is None:
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

    def entrypoint(
        self,
        name: str,
        *,
        concurrency_limit: int = 0,
        accepts_context: bool = False,
        on_failure: types.OnFailure = "delete",
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
            concurrency_limit (int): Max number of concurrent jobs allowed for this
                entrypoint across all workers. Enforced at the database level.
                0 means unlimited. Use 1 for serialized (one-at-a-time) dispatch.
            accepts_context (bool): When True, invoke the entrypoint with both job and context.
            on_failure (OnFailure): What to do when a job fails terminally. ``"delete"``
                removes the job (default); ``"hold"`` parks it with status ``'failed'``
                so it can be inspected and manually re-queued.

        Returns:
            Callable[[T], T]: A decorator that registers the function as an entrypoint.

        Raises:
            RuntimeError: If the entrypoint name is already registered.
            ValueError: If `concurrency_limit` is negative.
        """

        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        if not isinstance(concurrency_limit, int):
            raise ValueError("Concurrency limit must be int.")

        if concurrency_limit < 0:
            raise ValueError("Concurrency limit must be greater or eq. to zero.")

        if not isinstance(accepts_context, bool):
            raise ValueError("accepts_context must be boolean")

        if on_failure not in get_args(types.OnFailure):
            raise ValueError(f"on_failure must be one of {get_args(types.OnFailure)}.")

        executor_factory = executor_factory or executors.EntrypointExecutor

        def register(func: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
            self.register_executor(
                name,
                executor_factory(
                    executors.EntrypointExecutorParameters(
                        func=func,
                        concurrency_limit=concurrency_limit,
                        accepts_context=accepts_context,
                        on_failure=on_failure,
                    )
                ),
            )
            return func

        return register

    def entrypoints_below_capacity_limits(self) -> set[str]:
        """
        Return all registered entrypoint names.

        Concurrency limits are enforced at the database level, so all
        entrypoints are always eligible for dequeue attempts.
        """
        return set(self.entrypoint_registry)

    async def fetch_jobs(
        self,
        batch_size: int,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> AsyncGenerator[models.Job, None]:
        """
        Fetch jobs from the queue that are ready for processing, yielding them one at a time.

        This method retrieves a batch of jobs that match the current capacity constraints
        of each entrypoint. It ensures that jobs are fetched only when the QueueManager
        is operational and that concurrency controls are respected.
        """

        while not self.shutdown.is_set():
            entrypoints = {
                x: EntrypointExecutionParameter(
                    concurrency_limit=self.entrypoint_registry[x].parameters.concurrency_limit,
                )
                for x in self.entrypoints_below_capacity_limits()
            }

            # Cap batch_size to the smallest concurrency limit so a single
            # dequeue never picks more jobs than the tightest entrypoint allows.
            effective_batch = min(
                (p.concurrency_limit for p in entrypoints.values() if p.concurrency_limit > 0),
                default=batch_size,
            )
            effective_batch = min(batch_size, effective_batch)

            if not (
                jobs := await self.queries.dequeue(
                    batch_size=effective_batch,
                    entrypoints=entrypoints,
                    queue_manager_id=self.queue_manager_id,
                    global_concurrency_limit=global_concurrency_limit,
                    heartbeat_timeout=heartbeat_timeout,
                )
            ):
                break

            for job in jobs:
                yield job

    async def verify_structure(self) -> None:
        """
        Verify the required database structure.

        Checks necessary columns and user-defined types. Raises RuntimeError if missing.
        """

        for table in (
            self.queries.qbe.settings.queue_table,
            self.queries.qbe.settings.statistics_table,
        ):
            if not (await self.queries.has_table(table)):
                raise RuntimeError(
                    f"The required table '{table}' is missing. "
                    f"Please run 'pgq install' to set up the necessary tables."
                )

        if not (await self.queries.has_table(self.queries.qbe.settings.queue_table_log)):
            raise RuntimeError(
                f"The {self.queries.qbe.settings.queue_table_log} table is missing "
                "please run 'pgq upgrade'"
            )

        for table, column in (
            (self.queries.qbe.settings.queue_table, "updated"),
            (self.queries.qbe.settings.queue_table, "heartbeat"),
            (self.queries.qbe.settings.queue_table, "queue_manager_id"),
            (self.queries.qbe.settings.queue_table, "execute_after"),
            (self.queries.qbe.settings.queue_table, "headers"),
            (self.queries.qbe.settings.queue_table, "attempts"),
            (self.queries.qbe.settings.queue_table_log, "traceback"),
        ):
            if not (await self.queries.table_has_column(table, column)):
                raise RuntimeError(
                    f"The required column '{column}' is missing in the '{table}' table. "
                    f"Please run 'pgq upgrade' to ensure all schema changes are applied."
                )

        for key, enum in (
            ("canceled", self.queries.qbe.settings.queue_status_type),
            ("failed", self.queries.qbe.settings.queue_status_type),
        ):
            if not (await self.queries.has_user_defined_enum(key, enum)):
                raise RuntimeError(
                    f"The {enum} is missing the '{key}' type, please run 'pgq upgrade'"
                )

        for table, index in (
            (
                self.queries.qbe.settings.queue_table_log,
                f"{self.queries.qbe.settings.queue_table_log}_job_id_status",
            ),
        ):
            if not (await self.queries.table_has_index(table, index)):
                raise RuntimeError(
                    f"The required index '{index}' is missing in the '{table}' table. "
                    f"Please run 'pgq upgrade' to ensure all schema changes are applied."
                )

    async def _maybe_drain_shutdown(
        self,
        mode: types.QueueExecutionMode,
        task_manager: tm.TaskManager,
        cached_queued_work: cache.TTLCache[int],
    ) -> None:
        """In drain mode, shut down once the queue is empty and all tasks have finished.

        When ``max_concurrent_tasks`` is low, ``fetch_jobs`` may stop yielding
        because capacity is reached rather than because the queue is empty.
        We therefore re-check the database.  Before that we yield once so
        in-flight tasks (which may re-queue via ``RetryRequested``) can finish.
        """
        if mode is not types.QueueExecutionMode.drain:
            return
        if task_manager.tasks:
            await asyncio.sleep(0)
        if (await cached_queued_work()) == 0 and not task_manager.tasks:
            self.shutdown.set()

    async def _maybe_health_shutdown(
        self,
        periodic_health_check_task: asyncio.Task[None],
        mode: types.QueueExecutionMode,
        shutdown_on_listener_failure: bool,
    ) -> None:
        """Propagate a listener health-check failure as a shutdown signal."""
        if (
            periodic_health_check_task.done()
            and periodic_health_check_task.exception()
            and mode is not types.QueueExecutionMode.drain
            and shutdown_on_listener_failure
        ):
            self.shutdown.set()
            await periodic_health_check_task

    async def _effective_dequeue_timeout(self, dequeue_timeout: timedelta) -> timedelta:
        """Return a potentially shortened timeout if a deferred job is about to become eligible."""
        entrypoints = list(self.entrypoint_registry.keys())
        eta = await self.queries.next_deferred_eta(entrypoints)
        if eta is not None and eta < dequeue_timeout:
            return max(eta, self.min_dequeue_poll_interval)
        # When eta is None there are no future-deferred jobs, but a job may have
        # become eligible between the preceding dequeue and this check (TOCTOU).
        # If queued work exists, wake up quickly so the next iteration picks it up.
        if eta is None and await self.queries.queued_work(entrypoints) > 0:
            return self.min_dequeue_poll_interval
        return dequeue_timeout

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous,
        max_concurrent_tasks: int | None = None,
        shutdown_on_listener_failure: bool = False,
        heartbeat_timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        """
        Run the main loop to process jobs from the queue.

        Continuously listens for events and dispatches jobs, managing connections
        and tasks, logging timeouts, and resetting connections upon termination.

        Args:
            dequeue_timeout (timedelta): Timeout duration for waiting to dequeue jobs.
            batch_size (int): Number of jobs to retrieve in each batch.
            mode (QueueExecutionMode): Whether to run in `continuous` or `drain` until
                queue is empty then shutdown.
            max_concurrent_tasks (int|None, default=None): Limit the total number of tasks that
                can run at the same time. If unspecified or None, there is no limit.
            heartbeat_timeout (timedelta): Duration after which a picked job with a stale
                heartbeat becomes eligible for re-pickup by another worker. Heartbeats
                are sent automatically at half this interval. Defaults to 30 seconds.
        Raises:
            RuntimeError: If required database columns or types are missing.
        """

        await self.verify_structure()

        max_concurrent_tasks = max_concurrent_tasks or sys.maxsize

        if max_concurrent_tasks < 2 * batch_size:
            raise RuntimeError("max_concurrent_tasks must be at least twice the batch size.")

        async with (
            buffers.JobStatusLogBuffer(
                max_size=batch_size,
                repository=self.queries,
            ) as jbuff,
            buffers.HeartbeatBuffer(
                # Flush will be mainly driven by timeouts, but allow flush if
                # backlog becomes too large.
                max_size=batch_size**2,
                timeout=heartbeat_timeout / 4,
                repository=self.queries,
            ) as hbuff,
            tm.TaskManager() as task_manager,
            self.connection,
        ):
            periodic_health_check_task = asyncio.create_task(self._run_periodic_health_check())

            notice_event_listener = listeners.PGNoticeEventListener()
            await listeners.initialize_notice_event_listener(
                self.connection,
                self.channel,
                listeners.default_event_router(
                    notice_event_queue=notice_event_listener,
                    canceled=self.job_context,
                    pending_health_check=self.pending_health_check,
                ),
            )

            shutdown_task = asyncio.create_task(self.shutdown.wait())
            event_task: None | asyncio.Task[None | models.TableChangedEvent] = None
            cached_queued_work = cache.TTLCache.create(
                ttl=timedelta(seconds=0.250),
                on_expired=lambda: self.queries.queued_work(list(self.entrypoint_registry.keys())),
            )

            while not self.shutdown.is_set():
                async for job in self.fetch_jobs(
                    batch_size, max_concurrent_tasks, heartbeat_timeout
                ):
                    self.job_context[job.id] = models.Context(
                        cancellation=anyio.CancelScope(),
                        resources=self.resources,
                    )
                    task_manager.add(
                        asyncio.create_task(self._dispatch(job, jbuff, hbuff, heartbeat_timeout))
                    )

                    with contextlib.suppress(asyncio.QueueEmpty):
                        notice_event_listener.get_nowait()

                    if self.shutdown.is_set():
                        break

                await self._maybe_drain_shutdown(mode, task_manager, cached_queued_work)
                await self._maybe_health_shutdown(
                    periodic_health_check_task, mode, shutdown_on_listener_failure
                )

                event_task = listeners.wait_for_notice_event(
                    notice_event_listener,
                    await self._effective_dequeue_timeout(dequeue_timeout),
                )
                await asyncio.wait(
                    (shutdown_task, event_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )

            periodic_health_check_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await periodic_health_check_task

        if event_task and not event_task.done():
            event_task.cancel()

        if not shutdown_task.done():
            shutdown_task.cancel()

    async def _dispatch(
        self,
        job: models.Job,
        jbuff: buffers.JobStatusLogBuffer,
        hbuff: buffers.HeartbeatBuffer,
        heartbeat_timeout: timedelta,
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

        active_tracer = self.tracer or tracing.TRACER.tracer
        trace_context = active_tracer.trace_process(job) if active_tracer else nullcontext()
        async with (
            trace_context,
            heartbeat.Heartbeat(
                job.id,
                heartbeat_timeout / 2,
                hbuff,
            ),
        ):
            try:
                ctx = self.get_context(job.id)
                if not ctx.cancellation.cancel_called:
                    await executor.execute(job, ctx)
            except errors.RetryRequested as retry_exc:
                logconfig.logger.info(
                    "Retry requested for entrypoint/job-id: %s/%s (attempt=%d, delay=%s)",
                    job.entrypoint,
                    job.id,
                    job.attempts,
                    retry_exc.delay,
                )
                tbr = models.TracebackRecord.from_exception(
                    exc=retry_exc,
                    job_id=job.id,
                    additional_context={
                        "entrypoint": job.entrypoint,
                        "queue_manager_id": self.queue_manager_id,
                        "attempt": job.attempts,
                        "retry_delay": str(retry_exc.delay),
                        "reason": retry_exc.reason,
                    },
                )
                await self.queries.retry_job(job, retry_exc.delay, tbr)
            except Exception as e:
                logconfig.logger.exception(
                    "Exception while processing entrypoint/job-id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                tbr = models.TracebackRecord.from_exception(
                    exc=e,
                    job_id=job.id,
                    additional_context={
                        "entrypoint": job.entrypoint,
                        "queue_manager_id": self.queue_manager_id,
                    },
                )
                status: types.JOB_STATUS = (
                    "failed" if executor.parameters.on_failure == "hold" else "exception"
                )
                await jbuff.add((job, status, tbr))
            else:
                logconfig.logger.debug(
                    "Dispatching entrypoint/id: %s/%s - successful",
                    job.entrypoint,
                    job.id,
                )
                canceled = ctx.cancellation.cancel_called
                await jbuff.add((job, "canceled" if canceled else "successful", None))
            finally:
                self.job_context.pop(job.id, None)
