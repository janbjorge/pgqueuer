"""In-memory queries satisfying all four port protocols."""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import timedelta
from typing import Literal, overload

from pydantic_core import to_json

from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.adapters.inmemory.jobs import JobRow, JobTable
from pgqueuer.adapters.inmemory.logs import LogBook
from pgqueuer.adapters.inmemory.schedules import ScheduleTable
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.query_helpers import (
    merge_tracing_headers,
    normalize_enqueue_params,
)
from pgqueuer.domain import models, types
from pgqueuer.domain.models import utc_now
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import (
    CronEntrypoint,
    HealthCheckId,
    JobId,
    OnConflict,
    QueueEntrypoint,
    QueueManagerId,
    ScheduleId,
    SortOrder,
)
from pgqueuer.ports import tracing
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.ports.tracing import TracingProtocol


def entrypoints_of(entrypoint: str | list[str] | None) -> set[str] | None:
    """None clears every row. A name or list clears those entrypoints."""
    if entrypoint is None:
        return None
    if isinstance(entrypoint, str):
        return {entrypoint}
    return set(entrypoint)


@dataclasses.dataclass
class InMemoryQueries:
    """Drop-in replacement for ``Queries`` backed by pure Python dicts."""

    driver: InMemoryDriver

    # Table, channel and schema names. The builders, and every caller holding
    # these queries, read this one object; defaults to the PGQUEUER_* env vars.
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    qbe: qb.QueryBuilderEnvironment = dataclasses.field(init=False)
    qbq: qb.QueryQueueBuilder = dataclasses.field(init=False)
    qbs: qb.QuerySchedulerBuilder = dataclasses.field(init=False)

    tracer: TracingProtocol | None = None

    job_table: JobTable = dataclasses.field(init=False)
    logs: LogBook = dataclasses.field(init=False)
    schedules: ScheduleTable = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.qbe = qb.QueryBuilderEnvironment(self.settings)
        self.qbq = qb.QueryQueueBuilder(self.settings)
        self.qbs = qb.QuerySchedulerBuilder(self.settings)
        self.job_table = JobTable()
        self.logs = LogBook()
        self.schedules = ScheduleTable()

    @property
    def jobs(self) -> dict[int, JobRow]:
        """Live job rows, keyed by id."""
        return self.job_table.rows

    async def install(self) -> None:
        pass

    async def upgrade(self) -> None:
        pass

    async def uninstall(self) -> None:
        self.job_table.clear(None)
        self.logs.wipe()
        self.schedules.clear()

    async def has_table(self, table: str) -> bool:
        return True

    async def table_has_column(self, table: str, column: str) -> bool:
        return True

    async def table_has_index(self, table: str, index: str) -> bool:
        return True

    async def has_user_defined_enum(self, key: str, enum: str) -> bool:
        return True

    async def has_function(self, function: str) -> bool:
        return True

    async def has_trigger(self, trigger: str) -> bool:
        return True

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
        on_conflict: Literal["raise", "update_priority"] = "raise",
    ) -> list[JobId]: ...

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
    ) -> list[JobId | None]: ...

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
        on_conflict: Literal["raise", "update_priority"] = "raise",
    ) -> list[JobId]: ...

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
    ) -> list[JobId | None]: ...

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
    ) -> list[JobId] | list[JobId | None]:
        normed = normalize_enqueue_params(
            entrypoint, payload, priority, execute_after, dedupe_key, headers
        )
        active_tracer = self.tracer or tracing.TRACER.tracer
        if active_tracer:
            normed.headers = list(
                merge_tracing_headers(
                    normed.headers,
                    active_tracer.trace_publish(normed.entrypoint),
                )
            )
        result = self.job_table.insert(normed, on_conflict, utc_now())
        for row in result.created:
            self.logs.append(
                created=row.created,
                job_id=row.id,
                status="queued",
                priority=row.priority,
                entrypoint=row.entrypoint,
                traceback=None,
            )
        await self.emit_table_changed("insert")
        return result.ids

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
        queue_manager_id: QueueManagerId,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> list[models.Job]:
        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")
        if not entrypoints:
            return []

        now = utc_now()
        picked = self.job_table.picked_counts(queue_manager_id, entrypoints)
        if global_concurrency_limit is not None and picked.total >= global_concurrency_limit:
            return []
        remaining = batch_size
        if global_concurrency_limit is not None:
            remaining = min(remaining, global_concurrency_limit - picked.total)
        selected = self.job_table.take(
            entrypoints,
            picked.per_entrypoint,
            remaining,
            queue_manager_id,
            heartbeat_timeout,
            now,
        )
        for row in selected:
            self.logs.append(
                created=now,
                job_id=row.id,
                status="picked",
                priority=row.priority,
                entrypoint=row.entrypoint,
                traceback=None,
            )
        # Postgres dequeue awaits I/O. Yield so QueueManager.fetch_jobs cannot spin.
        await asyncio.sleep(0)
        return [row.to_model() for row in selected]

    async def log_jobs(
        self,
        job_status: list[
            tuple[
                models.Job,
                types.JOB_STATUS,
                models.TracebackRecord | None,
            ]
        ],
    ) -> None:
        now = utc_now()
        for job, status, record in job_status:
            if not self.job_table.finish(job, status, now):
                continue
            self.logs.append(
                created=now,
                job_id=int(job.id),
                status=status,
                priority=job.priority,
                entrypoint=job.entrypoint,
                traceback=record.model_dump_json() if record else None,
            )
        # The SQL DELETE runs on every call, and its statement trigger always notifies.
        await self.emit_table_changed("delete")

    async def retry_job(
        self,
        job: models.Job,
        delay: timedelta,
        traceback_record: models.TracebackRecord | None,
    ) -> None:
        now = utc_now()
        row = self.job_table.retry(job, delay, now)
        if row is None:
            return
        self.logs.append(
            created=now,
            job_id=row.id,
            status="queued",
            priority=row.priority,
            entrypoint=row.entrypoint,
            traceback=traceback_record.model_dump_json() if traceback_record else None,
        )
        await self.emit_table_changed("update")

    async def requeue_jobs(self, ids: list[JobId]) -> None:
        pending = self.job_table.claim_requeue(
            ids,
            f"{self.settings.queue_table}_unique_dedupe_key",
        )
        now = utc_now()
        for row in pending:
            self.job_table.apply_requeue(row, now)
            self.logs.append(
                created=now,
                job_id=row.id,
                status="queued",
                priority=row.priority,
                entrypoint=row.entrypoint,
                traceback=None,
            )
            await self.emit_table_changed("update")

    async def list_failed_jobs(
        self,
        limit: int = 100,
        order: SortOrder = "DESC",
    ) -> list[models.Job]:
        return self.job_table.list_failed(limit, order)

    async def mark_job_as_cancelled(self, ids: list[JobId]) -> None:
        now = utc_now()
        for row in self.job_table.cancel(ids):
            self.logs.append(
                created=now,
                job_id=row.id,
                status="canceled",
                priority=row.priority,
                entrypoint=row.entrypoint,
                traceback=None,
            )
        await self.notify_job_cancellation(ids)

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        now = utc_now()
        # A full clear is TRUNCATE: it returns nothing, so no deleted log rows.
        for row in self.job_table.clear(entrypoints_of(entrypoint)):
            self.logs.append(
                created=now,
                job_id=row.id,
                status="deleted",
                priority=row.priority,
                entrypoint=row.entrypoint,
                traceback=None,
            )

    async def queue_size(self) -> list[models.QueueStatistics]:
        return self.job_table.queue_size()

    async def queued_work(self, entrypoints: list[QueueEntrypoint]) -> int:
        """1 when any queued job matches these entrypoints, else 0."""
        return int(self.job_table.has_queued(entrypoints))

    async def eligible_queued_work(self, entrypoints: list[QueueEntrypoint]) -> int:
        """1 when a queued job for these entrypoints is due, else 0."""
        return int(self.job_table.has_due(entrypoints, utc_now()))

    async def queue_log(self) -> list[models.Log]:
        return self.logs.to_models()

    async def update_heartbeat(self, job_ids: list[JobId]) -> None:
        self.job_table.touch(job_ids, utc_now())

    async def job_status(
        self,
        ids: list[JobId],
    ) -> list[tuple[JobId, types.JOB_STATUS]]:
        return self.logs.job_status(ids)

    async def aggregate_logs(self) -> None:
        """Fold unaggregated log rows into per-second statistics buckets."""
        self.logs.aggregate()

    async def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        return self.logs.log_statistics(limit, last)

    async def notify_job_cancellation(self, ids: list[JobId]) -> None:
        event = models.CancellationEvent(
            channel=self.qbq.settings.channel,
            ids=ids,
            sent_at=utc_now(),
            type="cancellation_event",
        )
        await self.driver.notify(self.qbq.settings.channel, event.model_dump_json())

    async def notify_health_check(self, health_check_event_id: HealthCheckId) -> None:
        event = models.HealthCheckEvent(
            channel=self.qbq.settings.channel,
            sent_at=utc_now(),
            type="health_check_event",
            id=health_check_event_id,
        )
        await self.driver.notify(self.qbq.settings.channel, event.model_dump_json())

    async def insert_schedule(
        self,
        schedules: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> None:
        self.schedules.insert(schedules)

    async def fetch_schedule(
        self,
        entrypoints: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> list[models.Schedule]:
        return self.schedules.fetch(entrypoints)

    async def set_schedule_queued(self, ids: set[ScheduleId]) -> None:
        self.schedules.set_queued(ids)

    async def update_schedule_heartbeat(self, ids: set[ScheduleId]) -> None:
        self.schedules.touch(ids)

    async def peek_schedule(self) -> list[models.Schedule]:
        return self.schedules.peek()

    async def delete_schedule(
        self,
        ids: set[ScheduleId],
        entrypoints: set[CronEntrypoint],
        *,
        keep: set[models.CronExpressionEntrypoint] | None = None,
    ) -> None:
        self.schedules.delete(ids, entrypoints, keep)

    async def clear_schedule(self) -> None:
        self.schedules.clear()

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None:
        self.logs.clear_statistics(entrypoints_of(entrypoint))

    async def clear_queue_log(self, entrypoint: str | list[str] | None = None) -> None:
        self.logs.clear_entries(entrypoints_of(entrypoint))

    async def next_deferred_eta(self, entrypoints: list[QueueEntrypoint]) -> timedelta | None:
        """Return time until the soonest deferred job becomes eligible, or None."""
        return self.job_table.next_deferred_eta(entrypoints, utc_now())

    async def queue_age(self) -> list[models.QueueAgeStats]:
        return self.job_table.queue_age(utc_now())

    async def job_duration_percentiles(
        self,
        last: timedelta,
    ) -> list[models.JobDurationStats]:
        return self.logs.duration_percentiles(last)

    async def throughput_summary(
        self,
        last: timedelta | None = None,
    ) -> list[models.ThroughputStats]:
        return self.logs.throughput_summary(last)

    async def throughput_timeseries(
        self,
        last: timedelta,
    ) -> list[models.ThroughputBucket]:
        return self.logs.throughput_timeseries(last)

    async def active_workers(self) -> list[models.ActiveWorker]:
        return self.job_table.active_workers()

    async def stale_jobs(
        self,
        threshold: timedelta,
        limit: int = 100,
    ) -> list[models.StaleJob]:
        return self.job_table.stale_jobs(threshold, limit, utc_now())

    async def exception_logs(self, limit: int = 100) -> list[models.Log]:
        return self.logs.exception_logs(limit)

    async def browse_queue(
        self,
        limit: int = 50,
        offset: int = 0,
        statuses: list[types.JOB_STATUS] | None = None,
        entrypoints: list[str] | None = None,
    ) -> list[models.Job]:
        return self.job_table.browse(limit, offset, statuses, entrypoints)

    async def queue_job_by_id(self, id: JobId) -> models.Job | None:
        row = self.job_table.rows.get(int(id))
        if row is None:
            return None
        return row.to_model()

    async def job_log_history(
        self,
        id: JobId,
        limit: int = 100,
    ) -> list[models.Log]:
        return self.logs.history(int(id), limit)

    async def unaggregated_log_count(self) -> int:
        return self.logs.unaggregated_count()

    async def schema_info(self) -> list[models.TableInfo]:
        names = self.qbq.settings
        tables = [
            (names.queue_table, len(self.job_table.rows)),
            (names.queue_table_log, len(self.logs.rows)),
            (names.statistics_table, len(self.logs.statistics)),
            (names.schedules_table, len(self.schedules.rows)),
        ]
        return [
            models.TableInfo(
                table_name=name,
                persistence="IN-MEMORY",
                estimated_rows=rows,
                total_size="-",
            )
            for name, rows in sorted(tables)
        ]

    async def emit_table_changed(self, operation: types.OPERATIONS) -> None:
        # Same keys as the Postgres trigger; the listener stamps received_at on parse.
        payload = to_json(
            {
                "channel": self.qbq.settings.channel,
                "operation": operation,
                "sent_at": utc_now(),
                "table": self.qbe.settings.queue_table,
                "type": "table_changed_event",
            }
        ).decode()
        await self.driver.notify(self.qbq.settings.channel, payload)
