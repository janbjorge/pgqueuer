"""Repository port protocols satisfied by Queries via structural subtyping."""

from __future__ import annotations

import dataclasses
import uuid
from datetime import timedelta
from typing import Literal, Protocol, overload

from pgqueuer.domain import models
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import CronEntrypoint, OnConflict, SortOrder
from pgqueuer.ports.driver import Driver


class QueryBuilderEnvironmentPort(Protocol):
    """Protocol for query builder environment used in schema operations."""

    settings: DBSettings


@dataclasses.dataclass
class EntrypointExecutionParameter:
    """Per-entrypoint dequeue parameter. ``concurrency_limit=0`` means unlimited."""

    concurrency_limit: int


class QueueRepositoryPort(Protocol):
    """Persistence operations for the job queue."""

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
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
        *,
        on_conflict: Literal["raise"] = "raise",
    ) -> list[models.JobId]: ...

    @overload
    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
        execute_after: timedelta | None = None,
        dedupe_key: str | None = None,
        headers: dict[str, str] | None = None,
        *,
        on_conflict: Literal["skip"],
    ) -> list[models.JobId | None]: ...

    @overload
    async def enqueue(
        self,
        entrypoint: list[str],
        payload: list[bytes | None],
        priority: list[int],
        execute_after: list[timedelta | None] | None = None,
        dedupe_key: list[str | None] | None = None,
        headers: list[dict[str, str] | None] | None = None,
        *,
        on_conflict: Literal["raise"] = "raise",
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
        *,
        on_conflict: Literal["skip"],
    ) -> list[models.JobId | None]: ...

    async def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes | None],
        priority: int | list[int] = 0,
        execute_after: timedelta | None | list[timedelta | None] = None,
        dedupe_key: str | list[str | None] | None = None,
        headers: dict[str, str] | list[dict[str, str] | None] | None = None,
        *,
        on_conflict: OnConflict = "raise",
    ) -> list[models.JobId] | list[models.JobId | None]: ...

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

    async def retry_job(
        self,
        job: models.Job,
        delay: timedelta,
        traceback_record: models.TracebackRecord | None,
    ) -> None: ...

    async def requeue_jobs(self, ids: list[models.JobId]) -> None: ...

    async def list_failed_jobs(
        self, limit: int = 100, order: SortOrder = "DESC"
    ) -> list[models.Job]: ...

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None: ...

    async def queue_size(self) -> list[models.QueueStatistics]: ...

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None: ...

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None: ...

    async def queued_work(self, entrypoints: list[str]) -> int: ...

    async def eligible_queued_work(self, entrypoints: list[str]) -> int:
        """Like ``queued_work`` but counting only jobs whose ``execute_after`` has passed."""
        ...

    async def queue_log(self) -> list[models.Log]: ...

    async def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]: ...

    async def job_status(
        self,
        ids: list[models.JobId],
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]: ...

    @property
    def driver(self) -> Driver: ...

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None: ...

    async def next_deferred_eta(self, entrypoints: list[str]) -> timedelta | None:
        """Return time until the soonest deferred job becomes eligible, or None."""
        ...


class InsightsRepositoryPort(Protocol):
    """Read-only queue introspection consumed by presentation layers (web, MCP, CLI)."""

    async def queue_size(self) -> list[models.QueueStatistics]: ...

    async def queue_age(self) -> list[models.QueueAgeStats]: ...

    async def job_duration_percentiles(
        self,
        last: timedelta,
    ) -> list[models.JobDurationStats]: ...

    async def throughput_summary(
        self,
        last: timedelta | None = None,
    ) -> list[models.ThroughputStats]: ...

    async def throughput_timeseries(
        self,
        last: timedelta,
    ) -> list[models.ThroughputBucket]: ...

    async def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]: ...

    async def active_workers(self) -> list[models.ActiveWorker]: ...

    async def stale_jobs(
        self,
        threshold: timedelta,
        limit: int = 100,
    ) -> list[models.StaleJob]: ...

    async def exception_logs(self, limit: int = 100) -> list[models.Log]: ...

    async def list_failed_jobs(
        self, limit: int = 100, order: SortOrder = "DESC"
    ) -> list[models.Job]: ...

    async def browse_queue(
        self,
        limit: int = 50,
        offset: int = 0,
        statuses: list[models.JOB_STATUS] | None = None,
        entrypoints: list[str] | None = None,
    ) -> list[models.Job]: ...

    async def queue_job_by_id(self, id: models.JobId) -> models.Job | None: ...

    async def job_log_history(
        self,
        id: models.JobId,
        limit: int = 100,
    ) -> list[models.Log]: ...

    async def unaggregated_log_count(self) -> int: ...

    async def schema_info(self) -> list[models.TableInfo]: ...

    async def peek_schedule(self) -> list[models.Schedule]: ...


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

    async def peek_schedule(self) -> list[models.Schedule]: ...

    async def delete_schedule(
        self,
        ids: set[models.ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None: ...

    async def clear_schedule(self) -> None: ...


class NotificationPort(Protocol):
    """Abstraction over PostgreSQL NOTIFY for inter-process signalling."""

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None: ...

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None: ...


class SchemaManagementPort(Protocol):
    """DDL operations for installing, upgrading, and inspecting the schema."""

    @property
    def qbe(self) -> QueryBuilderEnvironmentPort: ...

    async def install(self) -> None: ...

    async def uninstall(self) -> None: ...

    async def upgrade(self) -> None: ...

    async def has_table(self, table: str) -> bool: ...

    async def table_has_column(self, table: str, column: str) -> bool: ...

    async def table_has_index(self, table: str, index: str) -> bool: ...

    async def has_user_defined_enum(self, key: str, enum: str) -> bool: ...

    async def has_function(self, function: str) -> bool: ...

    async def has_trigger(self, trigger: str) -> bool: ...
