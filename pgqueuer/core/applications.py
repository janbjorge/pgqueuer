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
from typing import TYPE_CHECKING, Callable, MutableMapping

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.domain.settings import DBSettings
from pgqueuer.core.executors import (
    AbstractEntrypointExecutor,
    AbstractScheduleExecutor,
    EntrypointExecutorParameters,
    EntrypointTypeVar,
    ScheduleCrontab,
    ScheduleExecutorFactoryParameters,
)
from pgqueuer.core.qm import QueueManager
from pgqueuer.core.sm import SchedulerManager
from pgqueuer.domain.models import Channel
from pgqueuer.domain.types import OnFailure, QueueExecutionMode
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import asyncpg
    import psycopg


@dataclasses.dataclass
class PgQueuer:
    """
    PgQueuer class

    This class provides a unified interface for job queue management and task scheduling,
    leveraging PostgreSQL for managing job states and distributed processing.

    Resources:
        resources: Mutable mapping for user‑provided shared objects (DB pools, HTTP
            clients, caches, ML models) created once at startup and injected into
            each job Context via QueueManager.
    """

    connection: Driver
    channel: Channel = dataclasses.field(
        default=Channel(DBSettings().channel),
    )
    # Shared resources mapping passed to QueueManager and propagated into each job Context.
    resources: MutableMapping = dataclasses.field(
        default_factory=dict,
    )
    queries: RepositoryPort | None = dataclasses.field(default=None)
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
        # RepositoryPort | None is passed here; QueueManager/SchedulerManager will
        # create default Queries instances if None, which satisfies their type contract.
        self.qm = QueueManager(
            self.connection,
            self.channel,
            queries=self.queries,  # type: ignore[arg-type]
            resources=self.resources,
        )
        self.sm = SchedulerManager(
            self.connection,
            resources=self.resources,
            queries=self.queries,  # type: ignore[arg-type]
        )
        self.qm.shutdown = self.shutdown
        self.sm.shutdown = self.shutdown

    @classmethod
    def from_asyncpg_connection(
        cls,
        connection: "asyncpg.Connection",
        channel: Channel | None = None,
        resources: MutableMapping | None = None,
    ) -> "PgQueuer":
        """
        Create a PgQueuer instance from an asyncpg connection.

        Args:
            connection: An asyncpg connection object.
            channel: Optional Channel configuration. Defaults to Channel(DBSettings().channel).
            resources: Optional mutable mapping for shared resources.

        Returns:
            PgQueuer: A configured PgQueuer instance.
        """
        return cls._from_driver(
            driver=AsyncpgDriver(connection),
            channel=channel,
            resources=resources,
        )

    @classmethod
    def from_asyncpg_pool(
        cls,
        pool: "asyncpg.Pool",
        channel: Channel | None = None,
        resources: MutableMapping | None = None,
    ) -> "PgQueuer":
        """
        Create a PgQueuer instance from an asyncpg connection pool.

        Args:
            pool: An asyncpg connection pool object.
            channel: Optional Channel configuration. Defaults to Channel(DBSettings().channel).
            resources: Optional mutable mapping for shared resources.

        Returns:
            PgQueuer: A configured PgQueuer instance.
        """
        return cls._from_driver(
            driver=AsyncpgPoolDriver(pool),
            channel=channel,
            resources=resources,
        )

    @classmethod
    def from_psycopg_connection(
        cls,
        connection: "psycopg.AsyncConnection",
        channel: Channel | None = None,
        resources: MutableMapping | None = None,
    ) -> "PgQueuer":
        """
        Create a PgQueuer instance from a psycopg async connection.

        Args:
            connection: A psycopg async connection object. Must have autocommit enabled.
            channel: Optional Channel configuration. Defaults to Channel(DBSettings().channel).
            resources: Optional mutable mapping for shared resources.

        Returns:
            PgQueuer: A configured PgQueuer instance.
        """
        return cls._from_driver(
            driver=PsycopgDriver(connection),
            channel=channel,
            resources=resources,
        )

    @classmethod
    def _from_driver(
        cls,
        driver: Driver,
        channel: Channel | None = None,
        resources: MutableMapping | None = None,
    ) -> "PgQueuer":
        channel = channel or Channel(DBSettings().channel)
        resources = resources or {}
        return cls(connection=driver, channel=channel, resources=resources)

    @classmethod
    def in_memory(
        cls,
        channel: Channel | None = None,
        resources: MutableMapping | None = None,
    ) -> "PgQueuer":
        """Create a PgQueuer backed entirely by in-memory data structures.

        No PostgreSQL connection is needed.  Useful for testing, dev,
        and short-lived batch-processing containers.
        """
        driver = InMemoryDriver()
        channel = channel or Channel(DBSettings().channel)
        inmem = InMemoryQueries(driver=driver)
        return cls(
            connection=driver,
            channel=channel,
            queries=inmem,
            resources=resources or {},
        )

    async def run(
        self,
        dequeue_timeout: timedelta = timedelta(seconds=30),
        batch_size: int = 10,
        mode: QueueExecutionMode = QueueExecutionMode.continuous,
        max_concurrent_tasks: int | None = None,
        shutdown_on_listener_failure: bool = False,
        heartbeat_timeout: timedelta = timedelta(seconds=30),
    ) -> None:
        """
        Run both QueueManager and SchedulerManager concurrently.

        This method starts both the `QueueManager` and `SchedulerManager` concurrently to
        handle job processing and scheduling.
        """
        tasks = [
            asyncio.create_task(
                self.qm.run(
                    batch_size=batch_size,
                    dequeue_timeout=dequeue_timeout,
                    mode=mode,
                    max_concurrent_tasks=max_concurrent_tasks,
                    shutdown_on_listener_failure=shutdown_on_listener_failure,
                    heartbeat_timeout=heartbeat_timeout,
                )
            ),
            asyncio.create_task(self.sm.run()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            self.shutdown.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def entrypoint(
        self,
        name: str,
        *,
        concurrency_limit: int = 0,
        accepts_context: bool = False,
        on_failure: OnFailure = "delete",
        executor_factory: Callable[
            [EntrypointExecutorParameters],
            AbstractEntrypointExecutor,
        ]
        | None = None,
    ) -> Callable[[EntrypointTypeVar], EntrypointTypeVar]:
        return self.qm.entrypoint(
            name=name,
            concurrency_limit=concurrency_limit,
            accepts_context=accepts_context,
            on_failure=on_failure,
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
        accepts_context: bool = False,
    ) -> Callable[[ScheduleCrontab], ScheduleCrontab]:
        return self.sm.schedule(
            entrypoint=entrypoint,
            expression=expression,
            executor_factory=executor_factory,
            clean_old=clean_old,
            accepts_context=accepts_context,
        )
