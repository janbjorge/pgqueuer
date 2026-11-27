from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable, MutableMapping
from contextlib import suppress
from datetime import timedelta

import croniter

from pgqueuer.core import executors, logconfig, tm
from pgqueuer.domain import models, types
from pgqueuer.domain.types import ScheduleId
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.repository import SchemaVerifier


@dataclasses.dataclass
class SchedulerManager:
    """Cron-driven scheduler. ``resources`` is propagated into each ScheduleContext."""

    queries: RepositoryPort
    resources: MutableMapping[str, object] = dataclasses.field(default_factory=dict)
    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    registry: dict[models.CronExpressionEntrypoint, executors.AbstractScheduleExecutor] = (
        dataclasses.field(
            init=False,
            default_factory=dict,
        )
    )
    active_heartbeat_ids: set[ScheduleId] = dataclasses.field(
        init=False,
        default_factory=set,
    )

    def schedule(
        self,
        entrypoint: str,
        expression: str,
        executor_factory: Callable[
            [executors.ScheduleExecutorFactoryParameters],
            executors.AbstractScheduleExecutor,
        ]
        | None = None,
        clean_old: bool = False,
        accepts_context: bool | None = None,
    ) -> Callable[[executors.ScheduleCrontab], executors.ScheduleCrontab]:
        """Decorator: register a cron-scheduled job.

        Raises ValueError on invalid cron, RuntimeError on duplicate (entrypoint, expression).
        """
        if not croniter.croniter.is_valid(expression):
            raise ValueError(f"Invalid cron expression: {expression}")

        expression = types.CronExpression(" ".join(croniter.croniter(expression).expressions))
        entrypoint = types.CronEntrypoint(entrypoint)

        key = models.CronExpressionEntrypoint(
            entrypoint=entrypoint,
            expression=expression,
        )
        if key in self.registry:
            raise RuntimeError(
                f"{key} already in registry, tuple (name, expression) must be unique."
            )

        executor_factory = executor_factory or executors.ScheduleExecutor

        def register(func: executors.ScheduleCrontab) -> executors.ScheduleCrontab:
            resolved_context = (
                executors.wants_context(func, models.ScheduleContext)
                if accepts_context is None
                else accepts_context
            )
            self.registry[key] = executor_factory(
                executors.ScheduleExecutorFactoryParameters(
                    entrypoint=entrypoint,
                    expression=expression,
                    func=func,
                    clean_old=clean_old,
                    accepts_context=resolved_context,
                )
            )
            return func

        return register

    async def run(self) -> None:
        """Poll for due schedules and dispatch them until shutdown."""
        if isinstance(self.queries, SchemaVerifier):
            await self.queries.verify_schema()
        elif not (await self.queries.has_table(self.queries.qbe.settings.schedules_table)):
            raise RuntimeError(
                f"The {self.queries.qbe.settings.schedules_table} table is missing "
                "please run 'pgq upgrade'"
            )
        elif not (await self.queries.has_table(self.queries.qbe.settings.queue_table_log)):
            raise RuntimeError(
                f"The {self.queries.qbe.settings.queue_table_log} table is missing "
                "please run 'pgq upgrade'"
            )

        if to_clean := {k.entrypoint for k, v in self.registry.items() if v.parameters.clean_old}:
            # Spare expressions still registered, so a due next_run survives restart.
            await self.queries.delete_schedule(
                ids=set(),
                entrypoints=to_clean,
                keep={key for key in self.registry if key.entrypoint in to_clean},
            )

        await self.queries.insert_schedule({k: v.next_in() for k, v in self.registry.items()})

        finished = asyncio.Event()
        async with (
            # Outside the TaskManager so it keeps beating until running schedules drain.
            tm.cancel_on_exit(asyncio.create_task(self._heartbeat_loop(finished))),
        ):
            try:
                async with tm.TaskManager() as task_manager:
                    await self._poll_schedules(task_manager)
            finally:
                finished.set()
                # Let the heartbeat finish cooperatively before cancelling any pending I/O.
                await asyncio.sleep(0)

    async def _poll_schedules(self, task_manager: tm.TaskManager) -> None:
        while not self.shutdown.is_set():
            due = {key: executor.next_in() for key, executor in self.registry.items()}
            for schedule in await self.queries.fetch_schedule(due):
                key = models.CronExpressionEntrypoint(schedule.entrypoint, schedule.expression)
                task = asyncio.create_task(self.dispatch(self.registry[key], schedule))
                task_manager.add(task)
            await self._wait_for_next_tick()

    async def _wait_for_next_tick(self) -> None:
        # Under the one-second cron resolution: a full second of slack woke the
        # loop after the next tick of an every-second schedule had passed.
        leeway = timedelta(milliseconds=100)
        delay = (
            min(executor.next_in() for executor in self.registry.values())
            if self.registry
            else timedelta(seconds=1)
        )
        with suppress(TimeoutError):
            async with asyncio.timeout((delay + leeway).total_seconds()):
                await self.shutdown.wait()

    async def dispatch(
        self,
        executor: executors.AbstractScheduleExecutor,
        schedule: models.Schedule,
    ) -> None:
        """Run *executor* against *schedule*; always release the schedule back to queued."""
        logconfig.logger.debug(
            "Dispatching entrypoint/expression: %s/%s",
            schedule.entrypoint,
            schedule.expression,
        )

        self.active_heartbeat_ids.add(schedule.id)
        context = models.ScheduleContext(resources=self.resources)
        try:
            await executor.execute(schedule, context)
        except Exception:
            logconfig.logger.exception(
                "Exception while processing entrypoint/expression: %s/%s",
                schedule.entrypoint,
                schedule.expression,
            )
        else:
            logconfig.logger.debug(
                "Dispatching entrypoint/expression: %s/%s - successful",
                schedule.entrypoint,
                schedule.expression,
            )
        finally:
            self.active_heartbeat_ids.discard(schedule.id)
            await asyncio.shield(
                self.queries.set_schedule_queued({schedule.id}),
            )

    async def _heartbeat_loop(self, finished: asyncio.Event) -> None:
        """Send heartbeats until schedule fetching and dispatch have finished.

        A schedule whose heartbeat goes stale can be picked again and run twice.
        """
        while not finished.is_set():
            if self.active_heartbeat_ids:
                try:
                    await self.queries.update_schedule_heartbeat(
                        set(self.active_heartbeat_ids),
                    )
                except Exception:
                    logconfig.logger.exception("Failed to update schedule heartbeat")
            with suppress(TimeoutError):
                async with asyncio.timeout(1.0):
                    await finished.wait()
