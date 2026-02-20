"""Port protocols for PgQueuer's hexagonal architecture.

These Protocol classes define the contracts that the core domain
(QueueManager, SchedulerManager) depends on. The existing ``Queries``
class satisfies all four ports via structural subtyping -- no
inheritance or registration is required.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import timedelta
from typing import Protocol, overload

from pgqueuer.domain import models
from pgqueuer.domain.types import CronEntrypoint
from pgqueuer.ports.driver import Driver


@dataclasses.dataclass
class EntrypointExecutionParameter:
    """
    Job execution parameters like retry, concurrency.

    Attributes:
        retry_after (timedelta): Time to wait before retrying.
        serialized (bool): Whether execution is serialized.
        concurrency_limit (int): Max number of concurrent executions.
    """

    retry_after: timedelta
    serialized: bool
    concurrency_limit: int


# ---------------------------------------------------------------------------
# Queue persistence
# ---------------------------------------------------------------------------


class QueueRepositoryPort(Protocol):
    """Persistence operations for the job queue."""

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
    ) -> list[models.Job]: ...

    @overload
    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
        execute_after: timedelta | None = None,
        dedupe_key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[models.JobId]: ...

    @overload
    async def enqueue(
        self,
        entrypoint: list[str],
        payload: list[bytes | None],
        priority: list[int],
        execute_after: list[timedelta | None] | None = None,
        dedupe_key: list[str | None] | None = None,
        headers: list[dict[str, str] | None] | None = None,
    ) -> list[models.JobId]: ...

    async def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes | None],
        priority: int | list[int] = 0,
        execute_after: timedelta | None | list[timedelta | None] = None,
        dedupe_key: str | list[str | None] | None = None,
        headers: dict[str, str] | list[dict[str, str] | None] | None = None,
    ) -> list[models.JobId]: ...

    async def log_jobs(
        self,
        job_status: list[
            tuple[
                models.Job,
                models.JOB_STATUS,
                models.TracebackRecord | None,
            ]
        ],
    ) -> None: ...

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None: ...

    async def queue_size(self) -> list[models.QueueStatistics]: ...

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None: ...

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None: ...

    async def queued_work(self, entrypoints: list[str]) -> int: ...

    async def queue_log(self) -> list[models.Log]: ...

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]: ...

    async def job_status(
        self,
        ids: list[models.JobId],
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]: ...

    @property
    def driver(self) -> Driver:
        """Access the underlying database driver."""
        ...

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None:
        """Clear statistics log entries."""
        ...


# ---------------------------------------------------------------------------
# Schedule persistence
# ---------------------------------------------------------------------------


class ScheduleRepositoryPort(Protocol):
    """Persistence operations for cron schedules."""

    async def insert_schedule(
        self,
        schedules: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> None: ...

    async def fetch_schedule(
        self,
        entrypoints: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> list[models.Schedule]: ...

    async def set_schedule_queued(self, ids: set[models.ScheduleId]) -> None: ...

    async def update_schedule_heartbeat(self, ids: set[models.ScheduleId]) -> None: ...

    async def peak_schedule(self) -> list[models.Schedule]: ...

    async def delete_schedule(
        self,
        ids: set[models.ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None: ...

    async def clear_schedule(self) -> None: ...


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------


class NotificationPort(Protocol):
    """Abstraction over PostgreSQL NOTIFY for inter-process signalling."""

    async def notify_entrypoint_rps(self, entrypoint_count: dict[str, int]) -> None: ...

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None: ...

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None: ...


# ---------------------------------------------------------------------------
# Schema management (DDL)
# ---------------------------------------------------------------------------


class SchemaManagementPort(Protocol):
    """DDL operations for installing, upgrading, and inspecting the schema."""

    async def install(self) -> None: ...

    async def uninstall(self) -> None: ...

    async def upgrade(self) -> None: ...

    async def has_table(self, table: str) -> bool: ...

    async def table_has_column(self, table: str, column: str) -> bool: ...

    async def table_has_index(self, table: str, index: str) -> bool: ...

    async def has_user_defined_enum(self, key: str, enum: str) -> bool: ...
