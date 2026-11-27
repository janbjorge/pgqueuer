from __future__ import annotations

import asyncio
import dataclasses
import functools
import random
import uuid
from collections.abc import AsyncGenerator, Callable, MutableMapping
from contextlib import AsyncExitStack, nullcontext, suppress
from datetime import timedelta
from typing import get_args

import anyio

from pgqueuer.core import (
    buffers,
    executors,
    heartbeat,
    listeners,
    logconfig,
    tm,
)
from pgqueuer.domain import errors, models, types
from pgqueuer.ports import RepositoryPort, tracing
from pgqueuer.ports.repository import EntrypointExecutionParameter, SchemaVerifier


@dataclasses.dataclass
class QueueManager:
    """Dequeue jobs and dispatch them to registered entrypoint executors.

    ``resources`` is a user-provided mapping (DB pools, HTTP clients, ML models,
    etc.) propagated into every job Context. Pass at construction time to share
    initialised objects across jobs.
    """

    queries: RepositoryPort
    # Empty: listen on ``queries.qbe.settings.channel``, where the queries notify.
    channel: types.Channel = types.Channel("")

    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    # Set once run() has its LISTEN in place; a probe sent before it would go unheard.
    listening: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )

    # Per entrypoint
    entrypoint_registry: dict[types.QueueEntrypoint, executors.AbstractEntrypointExecutor] = (
        dataclasses.field(
            init=False,
            default_factory=dict,
        )
    )
    queue_manager_id: types.QueueManagerId = dataclasses.field(
        init=False,
        default_factory=lambda: types.QueueManagerId(uuid.uuid4()),
    )
    # Shared resources mapping propagated into each job Context.
    resources: MutableMapping[str, object] = dataclasses.field(
        default_factory=dict,
    )

    # Optional injected tracer; falls back to the global ``tracing.TRACER.tracer``.
    tracer: tracing.TracingProtocol | None = None

    # Per job.
    job_context: dict[types.JobId, models.Context] = dataclasses.field(
        init=False,
        default_factory=dict,
    )

    pending_health_check: dict[types.HealthCheckId, asyncio.Future[models.HealthCheckEvent]] = (
        dataclasses.field(
            init=False,
            default_factory=dict,
        )
    )

    # Incremented once per dispatched job; the periodic log-aggregation task
    # only calls the database when this is nonzero, so an idle worker never
    # round-trips an aggregation query for nothing.
    jobs_logged: int = dataclasses.field(init=False, default=0)

    unwritten_job_logs: list[buffers.JobLogEntry] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    # Minimum sleep before re-checking the queue. Prevents busy-looping when a
    # deferred job's ETA is near zero or when the TOCTOU fallback detects
    # eligible work that the preceding dequeue narrowly missed.
    min_dequeue_poll_interval: timedelta = dataclasses.field(
        init=False,
        default=timedelta(seconds=0.1),
    )

    def __post_init__(self) -> None:
        self.channel = self.channel or types.Channel(self.queries.qbe.settings.channel)

    async def listener_healthy(
        self,
        timeout: timedelta = timedelta(seconds=10),  # noqa: ASYNC109 - public API
    ) -> models.HealthCheckEvent:
        """Round-trip a NOTIFY/LISTEN probe. Raises ``FailingListenerError`` on timeout."""
        health_check_event_id = types.HealthCheckId(uuid.uuid4())
        fut = asyncio.Future[models.HealthCheckEvent]()
        self.pending_health_check[health_check_event_id] = fut
        try:
            await self.queries.notify_health_check(health_check_event_id)
            done, _ = await asyncio.wait({fut}, timeout=timeout.total_seconds())
            if not done:
                raise errors.FailingListenerError
            return fut.result()
        finally:
            self.pending_health_check.pop(health_check_event_id, None)
            if not fut.done():
                fut.cancel()

    async def _run_periodic_health_check(
        self,
        interval: timedelta = timedelta(seconds=10),
        max_misses: int = 3,
    ) -> None:
        """Probe listener health every *interval*; raise after *max_misses* misses in a row."""
        await self.listening.wait()
        misses = 0
        while not self.shutdown.is_set():
            try:
                await self.listener_healthy(timeout=interval)
            except Exception:
                misses += 1
                if misses < max_misses:
                    # One late or dropped NOTIFY is not a dead listener; the miss
                    # already waited *interval*, so probe again right away.
                    logconfig.logger.warning(
                        "Listener health probe %d/%d failed; retrying.",
                        misses,
                        max_misses,
                        exc_info=True,
                    )
                    continue
                # Without shutdown_on_listener_failure nothing reads this task's exception.
                logconfig.logger.exception(
                    "Listener health check failed: NOTIFY wake-ups and job cancellations "
                    "no longer reach this worker. Unless shutdown_on_listener_failure is set, "
                    "it keeps running and only polls every dequeue timeout: restart it."
                )
                raise
            misses = 0
            with suppress(TimeoutError):
                async with asyncio.timeout(interval.total_seconds()):
                    await self.shutdown.wait()

    async def _run_periodic_log_aggregation(self, interval: timedelta) -> None:
        """Aggregate log->statistics every *interval* (jittered) until shutdown.

        Non-cron: mirrors ``_run_periodic_health_check``. Skipped when this worker
        hasn't logged a job since the last tick: an idle worker has nothing new to
        fold in, and the advisory lock in ``aggregate_logs`` already serializes real
        work across workers. A failed attempt leaves ``jobs_logged`` set so the next
        tick retries instead of silently dropping the backlog; only success resets it.
        """
        while not self.shutdown.is_set():
            if self.jobs_logged:
                try:
                    await self.queries.aggregate_logs()
                except Exception:  # noqa: BLE001, S110 - retried on the next interval
                    pass
                else:
                    self.jobs_logged = 0
            with suppress(TimeoutError):
                async with asyncio.timeout(interval.total_seconds() * random.uniform(0.8, 1.2)):  # noqa: S311 - timing jitter, not crypto
                    await self.shutdown.wait()

    def get_context(self, job_id: types.JobId) -> models.Context:
        return self.job_context[job_id]

    def forget_job(
        self,
        job_id: types.JobId,
        context: models.Context,
        task: asyncio.Task[None],
    ) -> None:
        """Drop *job_id*'s context once its dispatch task is done, however it ended.

        A retried job may already be running again under a new context; that one is kept.
        """
        if self.job_context.get(job_id) is context:
            del self.job_context[job_id]

    def register_executor(
        self,
        name: str,
        executor: executors.AbstractEntrypointExecutor,
    ) -> None:
        """Bind *name* to *executor*. Raises RuntimeError on duplicate name."""
        entrypoint = types.QueueEntrypoint(name)
        if entrypoint in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        self.entrypoint_registry[entrypoint] = executor

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
        if types.QueueEntrypoint(name) in self.entrypoint_registry:
            raise RuntimeError(f"{name} already in registry, name must be unique.")

        if not isinstance(concurrency_limit, int):
            raise ValueError("Concurrency limit must be int.")  # noqa: TRY004 - public behaviour

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
                for x in self.entrypoint_registry
            }

            if not (
                jobs := await self.queries.dequeue(
                    batch_size=batch_size,
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
        """Assert that required tables, columns, indexes, enums, and triggers exist.

        A repository that verifies its own schema checks every declared object;
        any other gets the fixed list below.
        """
        if isinstance(self.queries, SchemaVerifier):
            await self.queries.verify_schema()
            return

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
            (self.queries.qbe.settings.queue_table, "slot"),
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
            (
                self.queries.qbe.settings.queue_table,
                f"{self.queries.qbe.settings.queue_table}_picked_slot_idx",
            ),
        ):
            if not (await self.queries.table_has_index(table, index)):
                raise RuntimeError(
                    f"The required index '{index}' is missing in the '{table}' table. "
                    f"Please run 'pgq upgrade' to ensure all schema changes are applied."
                )

        if not (await self.queries.has_function(self.queries.qbe.settings.function)):
            raise RuntimeError(
                f"The required function '{self.queries.qbe.settings.function}' is missing. "
                f"Please run 'pgq upgrade' to ensure all schema changes are applied."
            )

        if not (await self.queries.has_trigger(self.queries.qbe.settings.trigger)):
            raise RuntimeError(
                f"The required trigger '{self.queries.qbe.settings.trigger}' is missing. "
                f"Please run 'pgq upgrade' to ensure all schema changes are applied."
            )

    async def _maybe_drain_shutdown(
        self,
        mode: types.QueueExecutionMode,
        task_manager: tm.TaskManager,
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
        if task_manager.tasks:
            return
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

    async def _effective_dequeue_timeout(
        self,
        dequeue_timeout: timedelta,
        max_concurrent_tasks: int | None = None,
        running_tasks: int = 0,
        heartbeat_timeout: timedelta = timedelta(seconds=30),
    ) -> timedelta:
        """Return a shortened timeout if work is ready, or a deferred or stale job is due soon."""
        # A full worker cannot take work; the NOTIFY from flushing a job end wakes it.
        if max_concurrent_tasks is not None and running_tasks >= max_concurrent_tasks:
            return dequeue_timeout
        entrypoints = list(self.entrypoint_registry.keys())
        # A job may have become eligible between the preceding dequeue and this
        # check (TOCTOU), and its notification may already have been drained by
        # the caller. Eligible work always warrants a fast re-poll, even when
        # deferred jobs exist.
        limits = [self.entrypoint_registry[x].parameters.concurrency_limit for x in entrypoints]
        if await self.queries.eligible_queued_work(entrypoints, concurrency_limits=limits) > 0:
            return self.min_dequeue_poll_interval
        eta = await self.queries.next_deferred_eta(entrypoints)
        # A deferred job can cross its execute_after between the two probes
        # above: the eligibility probe still saw it as deferred, the eta probe
        # no longer sees it as future. Re-probing eligibility closes the gap.
        if (
            eta is None
            and await self.queries.eligible_queued_work(entrypoints, concurrency_limits=limits) > 0
        ):
            return self.min_dequeue_poll_interval
        # A dead worker's picked job, which may hold a full slot, is claimable once stale.
        stale = await self.queries.next_stale_eta(
            entrypoints,
            queue_manager_id=self.queue_manager_id,
            heartbeat_timeout=heartbeat_timeout,
        )
        soonest = min((x for x in (eta, stale) if x is not None), default=None)
        if soonest is not None and soonest < dequeue_timeout:
            return max(soonest, self.min_dequeue_poll_interval)
        return dequeue_timeout

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous,
        max_concurrent_tasks: int | None = None,
        shutdown_on_listener_failure: bool = False,
        heartbeat_timeout: timedelta = timedelta(seconds=30),
        log_aggregation_interval: timedelta = timedelta(seconds=30),
    ) -> None:
        """Process jobs until shutdown.

        ``mode=drain`` exits once the queue is empty and in-flight tasks finish.
        ``max_concurrent_tasks`` is a hard cap on this worker's picked jobs,
        enforced by the dequeue query. It defaults to ``max(100, 2 * batch_size)``;
        pass ``0`` for unlimited. Per-entrypoint concurrency limits are
        likewise enforced in SQL, so batches keep their full size regardless
        of the tightest registered limit.
        ``heartbeat_timeout`` is the staleness threshold for re-picking a job;
        heartbeats are emitted at half this interval.
        ``log_aggregation_interval`` drives a background task that folds
        ``pgqueuer_log`` into ``pgqueuer_statistics`` on a timer instead of only
        on stats reads; pass ``timedelta(0)`` to disable (pure on-demand).
        """
        # Drop the previous run's ends before anything here can raise.
        self.unwritten_job_logs = []
        self.listening.clear()
        await self.verify_structure()

        # Unset gets a finite budget so one worker cannot claim a whole backlog (#1090).
        # 0 is the explicit opt-in to unlimited, kept as None all the way to the
        # dequeue query so it composes without the worker-budget CTEs.
        if max_concurrent_tasks is None:
            max_concurrent_tasks = max(100, 2 * batch_size)
        max_concurrent_tasks = max_concurrent_tasks or None

        if max_concurrent_tasks is not None and max_concurrent_tasks < batch_size:
            raise RuntimeError("max_concurrent_tasks must be at least the batch size.")

        async with (
            buffers.JobStatusLogBuffer(
                max_size=batch_size,
                repository=self.queries,
                unwritten=self.unwritten_job_logs,
            ) as jbuff,
            buffers.HeartbeatBuffer(
                # Flush will be mainly driven by timeouts, but allow flush if
                # backlog becomes too large.
                max_size=batch_size**2,
                # With beats at T/2, T/8 keeps a live job within 0.8 T through one failed
                # flush; see docs/guides/heartbeat.md.
                timeout=heartbeat_timeout / 8,
                repository=self.queries,
            ) as hbuff,
            # This stack removes the listener after the jobs stop.
            AsyncExitStack() as listener_cleanup,
            # Inside the listener stack so draining jobs can still receive cancel NOTIFYs.
            tm.TaskManager() as task_manager,
            # Listed last, so they stop first. A failing loop body must not leak tasks.
            tm.cancel_on_exit(
                asyncio.create_task(self._run_periodic_health_check())
            ) as periodic_health_check_task,
            (
                tm.cancel_on_exit(
                    asyncio.create_task(
                        self._run_periodic_log_aggregation(log_aggregation_interval)
                    )
                )
                if log_aggregation_interval.total_seconds() > 0
                else nullcontext()
            ),
            tm.cancel_on_exit(asyncio.create_task(self.shutdown.wait())) as shutdown_task,
        ):
            notice_event_listener = listeners.PGNoticeEventListener()
            listener = listeners.listener_for(self.queries)
            callback = await listeners.initialize_notice_event_listener(
                listener,
                self.channel,
                listeners.default_event_router(
                    notice_event_queue=notice_event_listener,
                    canceled=self.job_context,
                    pending_health_check=self.pending_health_check,
                ),
            )
            listener_cleanup.push_async_callback(listener.remove_listener, self.channel, callback)
            self.listening.set()

            while not self.shutdown.is_set():
                async for job in self.fetch_jobs(
                    batch_size, max_concurrent_tasks, heartbeat_timeout
                ):
                    # A late heartbeat lets our own dequeue re-pick a job still running here.
                    if job.id in self.job_context:
                        continue
                    context = models.Context(
                        cancellation=anyio.CancelScope(),
                        resources=self.resources,
                    )
                    self.job_context[job.id] = context
                    task = asyncio.create_task(self._dispatch(job, jbuff, hbuff, heartbeat_timeout))
                    task.add_done_callback(functools.partial(self.forget_job, job.id, context))
                    task_manager.add(task)

                # NOTIFY follows commit, so the checks below already see what drained events signal.
                while not notice_event_listener.empty():
                    notice_event_listener.get_nowait()
                await self._maybe_drain_shutdown(mode, task_manager)
                await self._maybe_health_shutdown(
                    periodic_health_check_task, mode, shutdown_on_listener_failure
                )

                # A failed health check ends the wait at once, not at the next
                # dequeue_timeout. Only when it shuts us down: a finished task
                # would otherwise wake every wait and spin the loop.
                wake_on_listener_failure = (
                    shutdown_on_listener_failure and mode is not types.QueueExecutionMode.drain
                )
                timeout = await self._effective_dequeue_timeout(
                    dequeue_timeout,
                    max_concurrent_tasks,
                    len(task_manager.tasks),
                    heartbeat_timeout,
                )
                async with tm.cancel_on_exit(
                    listeners.wait_for_notice_event(notice_event_listener, timeout)
                ) as event_task:
                    wake_on: set[asyncio.Task[object]] = {shutdown_task, event_task}
                    if wake_on_listener_failure:
                        wake_on.add(periodic_health_check_task)
                    await asyncio.wait(wake_on, return_when=asyncio.FIRST_COMPLETED)

    async def write_job_ends(self, logs: list[buffers.JobLogEntry]) -> None:
        """Write *logs* through this manager's queries."""
        await self.queries.log_jobs(logs)

    async def log_failure(
        self,
        job: models.Job,
        executor: executors.AbstractEntrypointExecutor,
        exc: Exception,
        jbuff: buffers.JobStatusLogBuffer,
    ) -> None:
        """Log *exc* and record *job* as failed, honouring the entrypoint's on_failure."""
        logconfig.logger.exception(
            "Exception while processing entrypoint/job-id: %s/%s",
            job.entrypoint,
            job.id,
            exc_info=exc,
        )
        tbr = models.TracebackRecord.from_exception(
            exc=exc,
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

    async def write_retry(
        self,
        job: models.Job,
        delay: timedelta,
        traceback_record: models.TracebackRecord,
    ) -> None:
        """Persist a requested retry until the write succeeds.

        Failures keep the job picked and heartbeating. Shutdown leaves the row
        picked so stale recovery runs the handler again, with ``attempts`` unchanged.
        """
        pause = timedelta(milliseconds=50)
        cap = timedelta(seconds=1)
        while True:
            try:
                await self.queries.retry_job(job, delay, traceback_record)
            except Exception:
                logconfig.logger.exception(
                    "Retry write failed for entrypoint/job-id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
            else:
                return
            if self.shutdown.is_set():
                return
            with suppress(TimeoutError):
                async with asyncio.timeout(pause.total_seconds()):
                    await self.shutdown.wait()
                return
            pause = min(pause * 2, cap)

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
                    try:
                        await executor.execute(job, ctx)
                    except Exception as e:
                        # A TaskGroup wraps a child's RetryRequested in an ExceptionGroup.
                        if (retry := errors.retry_request(e)) is not None and retry is not e:
                            raise retry from e
                        raise
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
                # Release before re-queueing, or our own dequeue skips the re-picked row.
                self.job_context.pop(job.id, None)
                await self.write_retry(job, retry_exc.delay, tbr)
            except asyncio.CancelledError:
                logconfig.logger.debug(
                    "Job canceled mid-flight for entrypoint/id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                # shield: same cancel would otherwise abort the log write.
                await asyncio.shield(jbuff.add((job, "canceled", None)))
                raise
            except Exception as e:  # noqa: BLE001 - job code may raise anything; recorded as a failure
                await self.log_failure(job, executor, e, jbuff)
            else:
                logconfig.logger.debug(
                    "Dispatching entrypoint/id: %s/%s - successful",
                    job.entrypoint,
                    job.id,
                )
                canceled = ctx.cancellation.cancel_called
                await jbuff.add((job, "canceled" if canceled else "successful", None))
            finally:
                self.jobs_logged += 1
