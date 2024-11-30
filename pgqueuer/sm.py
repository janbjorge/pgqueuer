from __future__ import annotations

import asyncio
import dataclasses
import warnings
from contextlib import suppress
from datetime import timedelta
from typing import Callable

import croniter

from . import db, executors, helpers, logconfig, models, queries, tm

warnings.simplefilter("default", DeprecationWarning)


@dataclasses.dataclass
class SchedulerManager:
    """
    Scheduler class responsible for managing and scheduling jobs using cron expressions.

    This class registers job executors, maintains a schedule registry, and facilitates running
    scheduled tasks based on cron expressions. The Scheduler interacts with the database to
    manage task execution and state, and also handles asynchronous task management.

    Attributes:
        connection (db.Driver): The database driver used for database operations.
        shutdown (asyncio.Event): Event to signal when the Scheduler is shutting down.
        queries (queries.Queries): Instance for executing database queries.
        registry (dict[models.CronExpressionEntrypoint, executors.AbstractScheduleExecutor]):
            Registered job executors mapped to cron entrypoints and expressions.
    """

    connection: db.Driver
    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    queries: queries.Queries = dataclasses.field(
        init=False,
    )
    registry: dict[models.CronExpressionEntrypoint, executors.AbstractScheduleExecutor] = (
        dataclasses.field(
            init=False,
            default_factory=dict,
        )
    )

    def __post_init__(self) -> None:
        """
        Initialize the Scheduler after dataclass fields have been set.

        Sets up the `queries` instance using the provided database connection.
        """
        self.queries = queries.Queries(self.connection)

    def schedule(
        self,
        entrypoint: str,
        expression: str,
        executor: type[executors.AbstractScheduleExecutor] | None = None,
        executor_factory: Callable[
            [executors.ScheduleExecutorFactoryParameters],
            executors.AbstractScheduleExecutor,
        ]
        | None = None,
    ) -> Callable[[executors.AsyncCrontab], executors.AsyncCrontab]:
        """
        Register a new job with a cron schedule.

        Args:
            entrypoint (str): The entrypoint identifier for the job to be scheduled.
            expression (str): The cron expression defining the schedule.
            executor (type[executors.AbstractScheduleExecutor] | None, optional):
                Deprecated. The executor type that will run the job.
                This parameter is deprecated and will be removed in a future version.
                Please use 'executor_factory' instead for custom executor handling.
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

        if executor is not None:
            warnings.warn(
                "The 'executor' parameter is deprecated and will be removed in a future version. "
                "Please use 'executor_factory' instead for custom executor handling.",
                DeprecationWarning,
                stacklevel=3,
            )

        if not croniter.croniter.is_valid(expression):
            raise ValueError(f"Invalid cron expression: {expression}")

        expression = models.CronExpression(helpers.normalize_cron_expression(expression))
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

        def register(func: executors.AsyncCrontab) -> executors.AsyncCrontab:
            self.registry[key] = executor_factory(
                executors.ScheduleExecutorFactoryParameters(
                    connection=self.connection,
                    shutdown=self.shutdown,
                    queries=self.queries,
                    entrypoint=entrypoint,
                    expression=expression,
                    func=func,
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

        await self.queries.insert_schedule({k: v.next_in() for k, v in self.registry.items()})

        async with (
            tm.TaskManager() as task_manager,
            self.connection,
        ):
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
                                task_manager,
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
        task_manager: tm.TaskManager,
    ) -> None:
        """
        Dispatch a scheduled job for execution.

        Args:
            executor (executors.AbstractScheduleExecutor): The executor instance
                responsible for running the job.
            schedule (models.Schedule): The schedule object containing details
                of the job to be executed.
            task_manager (tm.TaskManager): The task manager to manage the
                execution tasks.
        """
        logconfig.logger.debug(
            "Dispatching entrypoint/expression: %s/%s",
            schedule.entrypoint,
            schedule.expression,
        )
        shutdown = asyncio.Event()

        async def heartbeat() -> None:
            while not shutdown.is_set() and not self.shutdown.is_set():
                await self.queries.update_schedule_heartbeat({schedule.id})
                await asyncio.sleep(1)

        task_manager.add(asyncio.create_task(heartbeat()))

        try:
            await executor.execute(schedule)
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
            shutdown.set()
            await asyncio.shield(
                self.queries.set_schedule_queued({schedule.id}),
            )
