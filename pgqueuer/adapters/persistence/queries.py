from __future__ import annotations

import asyncio
import dataclasses
import uuid
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    import asyncpg
    import psycopg

from pydantic_core import to_json

from pgqueuer.adapters.persistence import qb, query_helpers
from pgqueuer.adapters.persistence.query_helpers import merge_tracing_headers
from pgqueuer.domain import errors, models, types
from pgqueuer.domain.types import CronEntrypoint
from pgqueuer.ports import tracing
from pgqueuer.ports.driver import Driver, SyncDriver
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.ports.tracing import TracingProtocol


def is_unique_violation(exc: Exception) -> bool:
    """Return True if *exc* is a unique-constraint violation from asyncpg or psycopg."""
    with suppress(ImportError):
        import asyncpg

        if isinstance(exc, asyncpg.UniqueViolationError):
            return True

    with suppress(ImportError):
        import psycopg

        if isinstance(exc, psycopg.errors.UniqueViolation):
            return True

    return False


@dataclasses.dataclass
class Queries:
    """High-level job-queue operations: schema install/upgrade, enqueue/dequeue, log, stats."""

    driver: Driver

    qbe: qb.QueryBuilderEnvironment = dataclasses.field(
        default_factory=qb.QueryBuilderEnvironment,
    )
    qbq: qb.QueryQueueBuilder = dataclasses.field(
        default_factory=qb.QueryQueueBuilder,
    )
    qbs: qb.QuerySchedulerBuilder = dataclasses.field(
        default_factory=qb.QuerySchedulerBuilder,
    )

    # Optional injected tracer; falls back to the global ``tracing.TRACER.tracer``.
    tracer: TracingProtocol | None = None

    @classmethod
    def from_asyncpg_connection(cls, connection: "asyncpg.Connection") -> "Queries":
        """Build Queries over an asyncpg connection."""
        from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver

        return cls(AsyncpgDriver(connection))

    @classmethod
    def from_asyncpg_pool(cls, pool: "asyncpg.Pool") -> "Queries":
        """Build Queries over an asyncpg pool."""
        from pgqueuer.adapters.drivers.asyncpg import AsyncpgPoolDriver

        return cls(AsyncpgPoolDriver(pool))

    @classmethod
    def from_psycopg_connection(cls, connection: "psycopg.AsyncConnection") -> "Queries":
        """Build Queries over a psycopg async connection (must have autocommit=True)."""
        from pgqueuer.adapters.drivers.psycopg import PsycopgDriver

        return cls(PsycopgDriver(connection))

    async def install(self) -> None:
        """Create tables, types, indexes, triggers, and functions."""
        await self.driver.execute(self.qbe.build_install_query())

    async def uninstall(self) -> None:
        """Drop every PgQueuer schema object. Destructive."""
        await self.driver.execute(self.qbe.build_uninstall_query())

    async def upgrade(self) -> None:
        """Apply pending schema migrations one statement at a time."""
        for query in self.qbe.build_upgrade_queries():
            await self.driver.execute(query)

    async def alter_durability(self) -> None:
        """Switch table durability mode without data loss."""
        await self.driver.execute("\n\n".join(self.qbe.build_alter_durability_query()))

    async def optimize_autovacuum(self) -> None:
        """Apply autovacuum settings."""
        query = self.qbe.build_optimize_autovacuum_query()
        await self.driver.execute(query)

    async def optimize_autovacuum_rollback(self) -> None:
        """Revert autovacuum settings."""
        query = self.qbe.build_optimize_autovacuum_rollback_query()
        await self.driver.execute(query)

    async def table_has_column(self, table: str, column: str) -> bool:
        """Return True if *column* exists on *table*."""
        rows = await self.driver.fetch(
            self.qbe.build_table_has_column_query(),
            table,
            column,
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def table_has_index(self, table: str, index: str) -> bool:
        """Return True if *index* exists on *table*."""
        rows = await self.driver.fetch(
            self.qbe.build_table_has_index_query(),
            table,
            index,
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def has_user_defined_enum(self, key: str, enum: str) -> bool:
        """Check if a value exists in a user-defined ENUM type."""
        rows = await self.driver.fetch(self.qbe.build_user_types_query())
        return (key, enum) in {(row["enumlabel"], row["typname"]) for row in rows}

    async def has_table(self, table: str) -> bool:
        rows = await self.driver.fetch(
            self.qbe.build_has_table_query(),
            table,
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def has_function(self, function: str) -> bool:
        rows = await self.driver.fetch(
            self.qbe.build_has_function_query(),
            function,
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def has_trigger(self, trigger: str) -> bool:
        rows = await self.driver.fetch(
            self.qbe.build_has_trigger_query(),
            trigger,
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> list[models.Job]:
        """
        Retrieve and update jobs from the queue to be processed.

        Selects jobs that are queued or whose heartbeat has gone stale
        (exceeding heartbeat_timeout), locks them with FOR UPDATE SKIP
        LOCKED, and atomically sets their status to 'picked'.
        """

        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        rows = await self.driver.fetch(
            self.qbq.build_dequeue_query(),
            batch_size,
            list(entrypoints.keys()),
            [x.concurrency_limit for x in entrypoints.values()],
            queue_manager_id,
            global_concurrency_limit,
            heartbeat_timeout,
        )
        return [models.Job.model_validate(row) for row in rows]

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
    ) -> list[models.JobId]:
        """Insert one or many jobs. Scalar args = single insert; lists = batch insert."""
        normed_params = query_helpers.normalize_enqueue_params(
            entrypoint, payload, priority, execute_after, dedupe_key, headers
        )
        active_tracer = self.tracer or tracing.TRACER.tracer
        if active_tracer:
            normed_params.headers = list(
                merge_tracing_headers(
                    normed_params.headers,
                    active_tracer.trace_publish(normed_params.entrypoint),
                )
            )

        try:
            return [
                models.JobId(row["id"])
                for row in await self.driver.fetch(
                    self.qbq.build_enqueue_query(),
                    normed_params.priority,
                    normed_params.entrypoint,
                    normed_params.payload,
                    normed_params.execute_after,
                    normed_params.dedupe_key,
                    [to_json(x).decode() for x in normed_params.headers],
                )
            ]
        except Exception as e:
            if is_unique_violation(e):
                raise errors.DuplicateJobError(normed_params.dedupe_key) from e
            raise

    async def queued_work(self, entrypoints: list[str]) -> int:
        rows = await self.driver.fetch(self.qbq.build_has_queued_work(), entrypoints)
        return rows[0]["queued_work"] if rows else 0

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        """Delete jobs; restrict to *entrypoint* when given, else truncate."""
        if entrypoint:
            await self.driver.execute(
                self.qbq.build_delete_from_queue_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.driver.execute(self.qbq.build_truncate_queue_query())

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None:
        """Log *ids* as 'canceled' and emit a cancellation NOTIFY."""
        await asyncio.gather(
            self.driver.execute(
                self.qbq.build_log_job_query(),
                ids,
                ["canceled"] * len(ids),
                [None] * len(ids),
            ),
            self.notify_job_cancellation(ids),
        )

    async def queue_size(self) -> list[models.QueueStatistics]:
        """Per-(entrypoint, priority, status) queue counts."""
        return [
            models.QueueStatistics.model_validate(x)
            for x in await self.driver.fetch(self.qbq.build_queue_size_query())
        ]

    async def log_jobs(
        self,
        job_status: list[
            tuple[
                models.Job,
                models.JOB_STATUS,
                models.TracebackRecord | None,
            ]
        ],
    ) -> None:
        """
        Move completed or failed jobs from the queue to the log table.

        Jobs with status ``'failed'`` are held in the queue table (UPDATE)
        rather than deleted.  All other statuses are removed (DELETE) as before.
        Both paths write an entry to the log table.
        """
        await self.driver.execute(
            self.qbq.build_log_job_query(),
            [job.id for job, _, _ in job_status],
            [status for _, status, _ in job_status],
            [tb.model_dump_json() if tb else None for _, _, tb in job_status],
        )

    async def retry_job(
        self,
        job: models.Job,
        delay: timedelta,
        traceback_record: models.TracebackRecord | None,
    ) -> None:
        """Re-queue a job for retry with the given delay.

        Atomically updates the job in-place (status → queued, execute_after
        bumped, attempts incremented) and writes a log entry recording the
        retry event.
        """
        await self.driver.execute(
            self.qbq.build_retry_job_query(),
            job.id,
            delay,
            traceback_record.model_dump_json() if traceback_record else None,
        )

    async def requeue_jobs(self, ids: list[models.JobId]) -> None:
        """Move failed jobs back to queued status for reprocessing.

        Resets attempts to 0 and sets execute_after to NOW().
        Only affects jobs with status ``'failed'``.
        """
        await self.driver.execute(
            self.qbq.build_requeue_jobs_query(),
            ids,
        )

    async def list_failed_jobs(
        self,
        limit: int = 100,
        order: types.SortOrder = "DESC",
    ) -> list[models.Job]:
        """List jobs held with status ``'failed'``, ordered by creation time."""
        rows = await self.driver.fetch(
            self.qbq.build_list_failed_jobs_query(order=order),
            limit,
        )
        return [models.Job.model_validate(r) for r in rows]

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None:
        """Delete statistics rows; restrict to *entrypoint* when given, else truncate."""
        if entrypoint:
            await self.driver.execute(
                self.qbq.build_delete_from_log_statistics_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.driver.execute(self.qbq.build_truncate_log_statistics_query())

    async def clear_queue_log(self, entrypoint: str | list[str] | None = None) -> None:
        """Delete queue-log rows; restrict to *entrypoint* when given, else truncate."""
        if entrypoint:
            await self.driver.execute(
                self.qbq.build_delete_log_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.driver.execute(self.qbq.build_truncate_log_query())

    async def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        """Aggregate pending log rows, then return up to *limit* recent stats within *last*."""
        await self.driver.execute(self.qbq.build_aggregate_log_data_to_statistics_query())
        return [
            models.LogStatistics.model_validate(x)
            for x in await self.driver.fetch(
                self.qbq.build_log_statistics_query(),
                limit,
                last,
            )
        ]

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None:
        """Emit a ``cancellation_event`` NOTIFY carrying *ids*."""
        await self.driver.notify(
            self.qbq.settings.channel,
            models.CancellationEvent(
                channel=self.qbq.settings.channel,
                ids=ids,
                sent_at=models.utc_now(),
                type="cancellation_event",
            ).model_dump_json(),
        )

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None:
        """Emit a ``health_check_event`` NOTIFY tagged with ``health_check_event_id``."""
        await self.driver.notify(
            self.qbq.settings.channel,
            models.HealthCheckEvent(
                channel=self.qbq.settings.channel,
                sent_at=models.utc_now(),
                type="health_check_event",
                id=health_check_event_id,
            ).model_dump_json(),
        )

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        await self.driver.execute(
            self.qbq.build_update_heartbeat_query(),
            list(set(job_ids)),
        )

    async def insert_schedule(
        self,
        schedules: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> None:
        await self.driver.execute(
            self.qbs.build_insert_schedule_query(),
            [k.expression for k in schedules],
            [k.entrypoint for k in schedules],
            list(schedules.values()),
        )

    async def fetch_schedule(
        self,
        entrypoints: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> list[models.Schedule]:
        return [
            models.Schedule.model_validate(row)
            for row in await self.driver.fetch(
                self.qbs.build_fetch_schedule_query(),
                [x.expression for x in entrypoints],
                [x.entrypoint for x in entrypoints],
                list(entrypoints.values()),
            )
        ]

    async def set_schedule_queued(self, ids: set[models.ScheduleId]) -> None:
        await self.driver.execute(
            self.qbs.build_set_schedule_queued_query(),
            list(ids),
        )

    async def update_schedule_heartbeat(self, ids: set[models.ScheduleId]) -> None:
        await self.driver.execute(
            self.qbs.build_update_schedule_heartbeat(),
            list(ids),
        )

    async def peek_schedule(self) -> list[models.Schedule]:
        return [
            models.Schedule.model_validate(row)
            for row in await self.driver.fetch(
                self.qbs.build_peek_schedule_query(),
            )
        ]

    async def delete_schedule(
        self,
        ids: set[models.ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None:
        await self.driver.execute(
            self.qbs.build_delete_schedule_query(),
            list(ids),
            list(entrypoints),
        )

    async def clear_schedule(self) -> None:
        await self.driver.execute(
            self.qbs.build_truncate_schedule_query(),
        )

    async def queue_log(self) -> list[models.Log]:
        return [
            models.Log.model_validate(x)
            for x in await self.driver.fetch(self.qbq.build_fetch_log_query())
        ]

    async def job_status(
        self,
        ids: list[models.JobId],
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]:
        return [
            (row["job_id"], row["status"])
            for row in await self.driver.fetch(self.qbq.build_job_status_query(), ids)
        ]

    async def next_deferred_eta(self, entrypoints: list[str]) -> timedelta | None:
        """Return time until the soonest deferred job becomes eligible, or None."""
        rows = await self.driver.fetch(self.qbq.build_next_deferred_eta_query(), entrypoints)
        return rows[0]["eta"] if rows and rows[0]["eta"] is not None else None


@dataclasses.dataclass
class SyncQueries:
    """Synchronous subset of :class:`Queries` (currently enqueue + queue_size)."""

    driver: SyncDriver

    qbq: qb.QueryQueueBuilder = dataclasses.field(
        default_factory=qb.QueryQueueBuilder,
    )

    # Optional injected tracer; falls back to the global ``tracing.TRACER.tracer``.
    tracer: TracingProtocol | None = None

    @overload
    def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
        execute_after: timedelta | None = None,
        dedupe_key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[models.JobId]: ...

    @overload
    def enqueue(
        self,
        entrypoint: list[str],
        payload: list[bytes | None],
        priority: list[int],
        execute_after: list[timedelta | None] | None = None,
        dedupe_key: list[str | None] | None = None,
        headers: list[dict[str, str] | None] | None = None,
    ) -> list[models.JobId]: ...

    def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes | None],
        priority: int | list[int] = 0,
        execute_after: timedelta | None | list[timedelta | None] = None,
        dedupe_key: str | list[str | None] | None = None,
        headers: dict[str, str] | list[dict[str, str] | None] | None = None,
    ) -> list[models.JobId]:
        """Insert one or many jobs. Scalar args = single insert; lists = batch insert."""
        normed_params = query_helpers.normalize_enqueue_params(
            entrypoint,
            payload,
            priority,
            execute_after,
            dedupe_key,
            headers,
        )

        active_tracer = self.tracer or tracing.TRACER.tracer
        if active_tracer:
            normed_params.headers = list(
                merge_tracing_headers(
                    normed_params.headers,
                    active_tracer.trace_publish(normed_params.entrypoint),
                )
            )

        try:
            return [
                models.JobId(row["id"])
                for row in self.driver.fetch(
                    self.qbq.build_enqueue_query(),
                    normed_params.priority,
                    normed_params.entrypoint,
                    normed_params.payload,
                    normed_params.execute_after,
                    normed_params.dedupe_key,
                    [to_json(x).decode() for x in normed_params.headers],
                )
            ]
        except Exception as e:
            if is_unique_violation(e):
                raise errors.DuplicateJobError(normed_params.dedupe_key) from e
            raise

    def queue_size(self) -> list[models.QueueStatistics]:
        """Per-(entrypoint, priority, status) queue counts."""
        return [
            models.QueueStatistics.model_validate(x)
            for x in self.driver.fetch(self.qbq.build_queue_size_query())
        ]
