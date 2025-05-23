"""
This module defines the `PgQueuer` class, which orchestrates job scheduling and queue management
using PostgreSQL as a backend. The `PgQueuer` class is designed to combine the functionalities
of `QueueManager` and `SchedulerManager` to provide a unified interface for managing job queues
and scheduling periodic tasks efficiently.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta
from typing import Callable

from .db import Driver
from .executors import (
    AbstractEntrypointExecutor,
    AbstractScheduleExecutor,
    AsyncCrontab,
    EntrypointExecutorParameters,
    EntrypointTypeVar,
    ScheduleExecutorFactoryParameters,
)
from .models import Channel
from .qb import DBSettings
from .qm import QueueManager
from .sm import SchedulerManager
from .tm import TaskManager
from .types import QueueExecutionMode


@dataclasses.dataclass
class PgQueuer:
    """
    PgQueuer class

    This class provides a unified interface for job queue management and task scheduling,
    leveraging PostgreSQL for managing job states and distributed processing.
    """

    connection: Driver
    channel: Channel = dataclasses.field(
        default=Channel(DBSettings().channel),
    )
    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    qm: QueueManager = dataclasses.field(
        init=False,
    )
    sm: SchedulerManager = dataclasses.field(
        init=False,
    )

    def __post_init__(self) -> None:
        self.qm = QueueManager(self.connection, self.channel)
        self.sm = SchedulerManager(self.connection)
        self.qm.shutdown = self.shutdown
        self.sm.shutdown = self.shutdown

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        mode: QueueExecutionMode = QueueExecutionMode.continuous,
        max_concurrent_tasks: int | None = None,
        shutdown_on_listener_failure: bool = False,
    ) -> None:
        """
        Run both QueueManager and SchedulerManager concurrently.

        This method starts both the `QueueManager` and `SchedulerManager` concurrently to
        handle job processing and scheduling.
        """

        # The task manager waits for all tasks for compile before
        # exit.
        async with TaskManager() as tm:
            # Start queue manager
            tm.add(
                asyncio.create_task(
                    self.qm.run(
                        batch_size=batch_size,
                        dequeue_timeout=dequeue_timeout,
                        mode=mode,
                        max_concurrent_tasks=max_concurrent_tasks,
                        shutdown_on_listener_failure=shutdown_on_listener_failure,
                    )
                )
            )
            # Start scheduler manager
            tm.add(asyncio.create_task(self.sm.run()))

    def entrypoint(
        self,
        name: str,
        *,
        requests_per_second: float = float("inf"),
        concurrency_limit: int = 0,
        retry_timer: timedelta = timedelta(seconds=0),
        serialized_dispatch: bool = False,
        executor_factory: Callable[
            [EntrypointExecutorParameters],
            AbstractEntrypointExecutor,
        ]
        | None = None,
    ) -> Callable[[EntrypointTypeVar], EntrypointTypeVar]:
        return self.qm.entrypoint(
            name=name,
            requests_per_second=requests_per_second,
            concurrency_limit=concurrency_limit,
            retry_timer=retry_timer,
            serialized_dispatch=serialized_dispatch,
            executor_factory=executor_factory,
        )

    def schedule(
        self,
        entrypoint: str,
        expression: str,
        executor_factory: Callable[
            [ScheduleExecutorFactoryParameters],
            AbstractScheduleExecutor,
        ]
        | None = None,
        clean_old: bool = False,
    ) -> Callable[[AsyncCrontab], AsyncCrontab]:
        return self.sm.schedule(
            entrypoint=entrypoint,
            expression=expression,
            executor_factory=executor_factory,
            clean_old=clean_old,
        )
