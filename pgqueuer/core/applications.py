from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable, MutableMapping
from datetime import timedelta
from typing import TYPE_CHECKING

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.core import logconfig
from pgqueuer.core.buffers import JobLogEntry
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
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import Channel, OnFailure, QueueExecutionMode
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import asyncpg
    import psycopg


@dataclasses.dataclass
class PgQueuer:
    """PgQueuer class.

    This class provides a unified interface for job queue management and task scheduling,
    leveraging PostgreSQL for managing job states and distributed processing.

    Resources:
        resources: Mutable mapping for user-provided shared objects (DB pools, HTTP
            clients, caches, ML models) created once at startup and injected into
            each job Context via QueueManager.
    """

    connection: Driver
    # Empty: use ``queries.qbe.settings.channel``, where the queries notify.
    channel: Channel = Channel("")
    # Shared resources mapping passed to QueueManager and propagated into each job Context.
    resources: MutableMapping[str, object] = dataclasses.field(
        default_factory=dict,
    )
    queries: RepositoryPort | None = dataclasses.field(default=None)
    # Table, channel and schema names for the default ``Queries``; None reads the
    # PGQUEUER_* env vars. Pass settings to your own ``queries`` instead when you
    # bring one.
    settings: DBSettings | None = None
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
        if self.queries is None:
            self.queries = Queries(self.connection, self.settings or DBSettings())
        elif self.settings is not None and self.settings is not self.queries.qbe.settings:
            raise ValueError("Pass settings to the queries, not to PgQueuer alongside them.")
        self.channel = self.channel or Channel(self.queries.qbe.settings.channel)
        self.qm = QueueManager(
            self.queries,
            self.channel,
            resources=self.resources,
        )
        self.sm = SchedulerManager(
            self.queries,
            resources=self.resources,
        )
        self.qm.shutdown = self.shutdown
        self.sm.shutdown = self.shutdown

    @property
    def unwritten_job_logs(self) -> list[JobLogEntry]:
        """The queue manager's unwritten job logs."""
        return self.qm.unwritten_job_logs

    async def write_job_ends(self, logs: list[JobLogEntry]) -> None:
        """Write *logs* through the queue manager's queries."""
        await self.qm.write_job_ends(logs)

    @classmethod
    def from_asyncpg_connection(
        cls,
        connection: asyncpg.Connection,
        channel: Channel | None = None,
        resources: MutableMapping[str, object] | None = None,
        settings: DBSettings | None = None,
    ) -> PgQueuer:
        """Build PgQueuer over an asyncpg connection."""
        return cls._from_driver(
            driver=AsyncpgDriver(connection),
            channel=channel,
            resources=resources,
            settings=settings,
        )

    @classmethod
    def from_asyncpg_pool(
        cls,
        pool: asyncpg.Pool,
        channel: Channel | None = None,
        resources: MutableMapping[str, object] | None = None,
        settings: DBSettings | None = None,
    ) -> PgQueuer:
        """Build PgQueuer over an asyncpg pool."""
        return cls._from_driver(
            driver=AsyncpgPoolDriver(pool),
            channel=channel,
            resources=resources,
            settings=settings,
        )

    @classmethod
    def from_psycopg_connection(
        cls,
        connection: psycopg.AsyncConnection,
        channel: Channel | None = None,
        resources: MutableMapping[str, object] | None = None,
        settings: DBSettings | None = None,
    ) -> PgQueuer:
        """Build PgQueuer over a psycopg async connection (must have autocommit=True)."""
        return cls._from_driver(
            driver=PsycopgDriver(connection),
            channel=channel,
            resources=resources,
            settings=settings,
        )

    @classmethod
    def _from_driver(
        cls,
        driver: Driver,
        channel: Channel | None = None,
        resources: MutableMapping[str, object] | None = None,
        settings: DBSettings | None = None,
    ) -> PgQueuer:
        return cls(
            connection=driver,
            channel=channel or Channel(""),
            resources=resources or {},
            settings=settings,
        )

    @classmethod
    def in_memory(
        cls,
        channel: Channel | None = None,
        resources: MutableMapping[str, object] | None = None,
        settings: DBSettings | None = None,
    ) -> PgQueuer:
        """Create a PgQueuer backed entirely by in-memory data structures.

        No PostgreSQL connection is needed.  Useful for testing, dev,
        and short-lived batch-processing containers.
        """
        driver = InMemoryDriver()
        inmem = InMemoryQueries(driver=driver, settings=settings or DBSettings())
        return cls(
            connection=driver,
            channel=channel or Channel(""),
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
        log_aggregation_interval: timedelta = timedelta(seconds=30),
    ) -> None:
        """Run QueueManager and SchedulerManager concurrently."""
        tasks = [
            asyncio.create_task(
                self.qm.run(
                    batch_size=batch_size,
                    dequeue_timeout=dequeue_timeout,
                    mode=mode,
                    max_concurrent_tasks=max_concurrent_tasks,
                    shutdown_on_listener_failure=shutdown_on_listener_failure,
                    heartbeat_timeout=heartbeat_timeout,
                    log_aggregation_interval=log_aggregation_interval,
                )
            ),
            asyncio.create_task(self.sm.run()),
        ]
        raised: BaseException | None = None
        try:
            await asyncio.gather(*tasks)
        except BaseException as exc:
            raised = exc
            raise
        finally:
            self.shutdown.set()
            for t in tasks:
                t.cancel()
            for outcome in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(outcome, Exception) and outcome is not raised:
                    logconfig.logger.error("Manager failed during shutdown", exc_info=outcome)

    def entrypoint(
        self,
        name: str,
        *,
        concurrency_limit: int = 0,
        accepts_context: bool | None = None,
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
        accepts_context: bool | None = None,
    ) -> Callable[[ScheduleCrontab], ScheduleCrontab]:
        return self.sm.schedule(
            entrypoint=entrypoint,
            expression=expression,
            executor_factory=executor_factory,
            clean_old=clean_old,
            accepts_context=accepts_context,
        )
