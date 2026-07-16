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
from pgqueuer.domain.settings import DBSettings
from pgqueuer.ports import RepositoryPort, tracing
from pgqueuer.ports.repository import EntrypointExecutionParameter


@dataclasses.dataclass
class QueueManager:
    """Dequeue jobs and dispatch them to registered entrypoint executors.

    ``resources`` is a user-provided mapping (DB pools, HTTP clients, ML models,
    etc.) propagated into every job Context. Pass at construction time to share
    initialised objects across jobs.
    """

    queries: RepositoryPort
    channel: models.Channel = dataclasses.field(
        default=models.Channel(DBSettings().channel),
    )

    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )

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
        """Round-trip a NOTIFY/LISTEN probe. Raises ``FailingListenerError`` on timeout."""
        health_check_event_id = uuid.uuid4()
        fut = asyncio.Future[models.HealthCheckEvent]()
        self.pending_health_check[health_check_event_id] = fut
        try:
            await self.queries.notify_health_check(health_check_event_id)
            return await asyncio.wait_for(fut, timeout.total_seconds())
        except (TimeoutError, asyncio.TimeoutError):
            raise errors.FailingListenerError from None
        finally:
            self.pending_health_check.pop(health_check_event_id, None)

    async def _run_periodic_health_check(
        self,
        interval: timedelta = timedelta(seconds=10),
    ) -> None:
        """Probe listener health every *interval*; raise on timeout via ``listener_healthy``."""
        while not self.shutdown.is_set():
            await self.listener_healthy(timeout=interval)
            with suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=interval.total_seconds(),
                )

    def get_context(self, job_id: models.JobId) -> models.Context:
        return self.job_context[job_id]

    def register_executor(
        self,
        name: str,
        executor: executors.AbstractEntrypointExecutor,
    ) -> None:
        """Bind *name* to *executor*. Raises RuntimeError on duplicate name."""
        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        self.entrypoint_registry[name] = executor

    def entrypoint(
        self,
        name: str,
        *,
        concurrency_limit: int = 0,
        accepts_context: bool | None = None,
        on_failure: types.OnFailure = "delete",
        executor_factory: Callable[
            [executors.EntrypointExecutorParameters],
            executors.AbstractEntrypointExecutor,
        ]
        | None = None,
    ) -> Callable[[executors.EntrypointTypeVar], executors.EntrypointTypeVar]:
        """Decorator: register an entrypoint for job processing.

        ``concurrency_limit=0`` means unlimited; ``1`` serialises dispatch.
        Limits are enforced at the database level across all workers.

        ``accepts_context=None`` auto-detects from the handler signature (a
        parameter annotated ``Context`` receives one); pass True/False to override.

        ``on_failure='delete'`` (default) removes a terminally-failed job;
        ``'hold'`` parks it with status ``'failed'`` for manual re-queue.
        """

        if name in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        if not isinstance(concurrency_limit, int):
            raise ValueError("Concurrency limit must be int.")

        if concurrency_limit < 0:
            raise ValueError("Concurrency limit must be greater or eq. to zero.")

        if accepts_context is not None and not isinstance(accepts_context, bool):
            raise ValueError("accepts_context must be boolean or None")

        if on_failure not in get_args(types.OnFailure):
            raise ValueError(f"on_failure must be one of {get_args(types.OnFailure)}.")

        executor_factory = executor_factory or executors.EntrypointExecutor

        def register(func: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
            resolved_context = (
                executors.wants_context(func, models.Context)
                if accepts_context is None
                else accepts_context
            )
            self.register_executor(
                name,
                executor_factory(
                    executors.EntrypointExecutorParameters(
                        func=func,
                        concurrency_limit=concurrency_limit,
                        accepts_context=resolved_context,
                        on_failure=on_failure,
                    )
                ),
            )
            return func

        return register

    def entrypoints_below_capacity_limits(self) -> set[str]:
        """All registered entrypoints; per-entrypoint limits are enforced in SQL."""
        return set(self.entrypoint_registry)

    async def fetch_jobs(
        self,
        batch_size: int,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> AsyncGenerator[models.Job, None]:
        """Yield ready jobs one at a time, respecting per-entrypoint and global limits."""

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
        """Assert that required tables, columns, indexes, enums, and triggers exist."""

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
        if task_manager.tasks or (await cached_queued_work()) != 0:
            return
        # The cached count may predate a RetryRequested re-queue that landed
        # within the TTL window; confirm with an uncached read before exiting.
        if await self.queries.queued_work(list(self.entrypoint_registry.keys())) == 0:
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
        """Process jobs until shutdown.

        ``mode=drain`` exits once the queue is empty and in-flight tasks finish.
        ``heartbeat_timeout`` is the staleness threshold for re-picking a job;
        heartbeats are emitted at half this interval.
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
            self.queries.driver,
        ):
            periodic_health_check_task = asyncio.create_task(self._run_periodic_health_check())
            shutdown_task = asyncio.create_task(self.shutdown.wait())
            event_task: None | asyncio.Task[None | models.TableChangedEvent] = None

            try:
                notice_event_listener = listeners.PGNoticeEventListener()
                await listeners.initialize_notice_event_listener(
                    self.queries.driver,
                    self.channel,
                    listeners.default_event_router(
                        notice_event_queue=notice_event_listener,
                        canceled=self.job_context,
                        pending_health_check=self.pending_health_check,
                    ),
                )

                cached_queued_work = cache.TTLCache.create(
                    ttl=timedelta(seconds=0.250),
                    on_expired=lambda: self.queries.queued_work(
                        list(self.entrypoint_registry.keys())
                    ),
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
                            asyncio.create_task(
                                self._dispatch(job, jbuff, hbuff, heartbeat_timeout)
                            )
                        )

                        with contextlib.suppress(asyncio.QueueEmpty):
                            notice_event_listener.get_nowait()

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
            finally:
                # Runs before the buffers/driver close so a failing loop body
                # cannot leak tasks that probe a driver mid-teardown.
                periodic_health_check_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await periodic_health_check_task

                shutdown_task.cancel()
                with suppress(asyncio.CancelledError):
                    await shutdown_task

                if event_task is not None:
                    event_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await event_task

    async def _dispatch(
        self,
        job: models.Job,
        jbuff: buffers.JobStatusLogBuffer,
        hbuff: buffers.HeartbeatBuffer,
        heartbeat_timeout: timedelta,
    ) -> None:
        """Run *job*'s executor, route retries/cancels/failures, log final status."""

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
            except asyncio.CancelledError:
                logconfig.logger.debug(
                    "Job canceled mid-flight for entrypoint/id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                # shield: same cancel would otherwise abort the log write.
                await asyncio.shield(jbuff.add((job, "canceled", None)))
                raise
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
