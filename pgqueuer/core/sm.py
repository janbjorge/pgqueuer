from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import MutableMapping
from contextlib import suppress
from datetime import timedelta
from typing import Callable

import croniter

from pgqueuer.core import executors, logconfig, tm
from pgqueuer.domain import models
from pgqueuer.domain.types import ScheduleId
from pgqueuer.ports import RepositoryPort


@dataclasses.dataclass
class SchedulerManager:
    """
    Scheduler class responsible for managing and scheduling jobs using cron expressions.

    Attributes:
        queries (RepositoryPort): Repository for schedule operations. The underlying
            database driver is accessed via ``queries.driver``.
        resources (MutableMapping): Shared resources propagated to scheduled task contexts.
        registry (dict[models.CronExpressionEntrypoint, executors.AbstractScheduleExecutor]):
            Registered job executors mapped to cron entrypoints and expressions.
    """

    queries: RepositoryPort
    resources: MutableMapping = dataclasses.field(default_factory=dict)
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
        accepts_context: bool = False,
    ) -> Callable[[executors.ScheduleCrontab], executors.ScheduleCrontab]:
        """
        Register a new job with a cron schedule.

        Args:
            entrypoint (str): The entrypoint identifier for the job to be scheduled.
            expression (str): The cron expression defining the schedule.
            executor_factory (Callable[[ScheduleExecutorFactoryParameters],
                executors.AbstractScheduleExecutor]): A factory function to
                create the executor for the job.

        Returns:
            Callable[[executors.AsyncCrontab], executors.AsyncCrontab]:
                A decorator function that registers the provided function as a job.

        Raises:
            ValueError: If the provided cron expression is invalid.
            RuntimeError: If the entrypoint and expression are already registered.
        """

        if not croniter.croniter.is_valid(expression):
            raise ValueError(f"Invalid cron expression: {expression}")

        expression = models.CronExpression(" ".join(croniter.croniter(expression).expressions))
        entrypoint = models.CronEntrypoint(entrypoint)

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
            self.registry[key] = executor_factory(
                executors.ScheduleExecutorFactoryParameters(
                    entrypoint=entrypoint,
                    expression=expression,
                    func=func,
                    clean_old=clean_old,
                    accepts_context=accepts_context,
                )
            )
            return func

        return register

    async def run(self) -> None:
        """
        Run the Scheduler, executing registered jobs based on their cron schedules.

        Continuously polls for jobs that need to be executed and dispatches them accordingly.
        Also waits for shutdown events and manages the scheduling loop.
        """
        if not (await self.queries.has_table(self.queries.qbe.settings.schedules_table)):
            raise RuntimeError(
                f"The {self.queries.qbe.settings.schedules_table} table is missing "
                "please run 'pgq upgrade'"
            )

        if not (await self.queries.has_table(self.queries.qbe.settings.queue_table_log)):
            raise RuntimeError(
                f"The {self.queries.qbe.settings.queue_table_log} table is missing "
                "please run 'pgq upgrade'"
            )

        # Identify entrypoints that need to be removed based on the 'clean_old' parameter
        if to_clean := {k.entrypoint for k, v in self.registry.items() if v.parameters.clean_old}:
            await self.queries.delete_schedule(ids=set(), entrypoints=to_clean)

        await self.queries.insert_schedule({k: v.next_in() for k, v in self.registry.items()})

        async with (
            tm.TaskManager() as task_manager,
            self.queries.driver,
        ):
            task_manager.add(asyncio.create_task(self._heartbeat_loop()))
            while not self.shutdown.is_set():
                scheduled = await self.queries.fetch_schedule(
                    {k: v.next_in() for k, v in self.registry.items()}
                )
                for schedule in scheduled:
                    task_manager.add(
                        asyncio.create_task(
                            self.dispatch(
                                self.registry[
                                    models.CronExpressionEntrypoint(
                                        schedule.entrypoint,
                                        schedule.expression,
                                    )
                                ],
                                schedule,
                            ),
                        )
                    )

                leeway = timedelta(seconds=1)
                wait = (
                    min(x.next_in() for x in self.registry.values())
                    if self.registry
                    else timedelta(seconds=0)
                )

                with suppress(TimeoutError, asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.shutdown.wait(),
                        timeout=(wait + leeway).total_seconds(),
                    )

    async def dispatch(
        self,
        executor: executors.AbstractScheduleExecutor,
        schedule: models.Schedule,
    ) -> None:
        """
        Dispatch a scheduled job for execution.

        Args:
            executor (executors.AbstractScheduleExecutor): The executor instance
                responsible for running the job.
            schedule (models.Schedule): The schedule object containing details
                of the job to be executed.
        """
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

    async def _heartbeat_loop(self) -> None:
        """Send batched heartbeat updates for all active schedules."""
        while not self.shutdown.is_set():
            if self.active_heartbeat_ids:
                await self.queries.update_schedule_heartbeat(
                    set(self.active_heartbeat_ids),
                )
            with suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=1.0,
                )
