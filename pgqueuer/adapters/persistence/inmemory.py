"""In-memory implementation of all PgQueuer port protocols.

Useful for unit testing and ephemeral background tasks without PostgreSQL.

This module is a thin Python wrapper around the Rust InMemoryCore PyO3 extension.
All hot-path state (jobs, logs, queues) lives in Rust; this layer only handles
asyncio coordination, datetime↔µs conversion, and Pydantic model construction
at API return boundaries.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import cast, overload

from pydantic_core import to_json

from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.core_rs import InMemoryCore
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, JobId, ScheduleId

# Timezone constant
_UTC = timezone.utc


def _now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(_UTC)


def _us() -> int:
    """Get current UTC timestamp in microseconds since Unix epoch."""
    return int(datetime.now(_UTC).timestamp() * 1_000_000)


def _us_to_dt(us: int) -> datetime:
    """Convert microseconds since Unix epoch to UTC datetime."""
    return datetime.fromtimestamp(us / 1e6, tz=_UTC)


def _td_to_us(td: timedelta | None) -> int:
    """Convert timedelta to microseconds."""
    if td is None:
        return 0
    return int(td.total_seconds() * 1_000_000)


def _row_to_job(row: tuple) -> models.Job:
    """Convert Rust dequeue tuple to models.Job Pydantic model.

    Row format: (id, priority, created_us, updated_us, heartbeat_us, execute_after_us,
                 entrypoint, payload, queue_manager_id_bytes, headers_json)
    """
    (
        id_,
        priority,
        created_us,
        updated_us,
        heartbeat_us,
        execute_after_us,
        entrypoint,
        payload,
        qm_bytes,
        headers_json,
    ) = row

    # PyO3 converts Vec<u8> to list[int], convert back to bytes
    if isinstance(payload, list):
        payload = bytes(payload) if payload is not None else None

    # Convert queue_manager_id bytes back to UUID
    queue_manager_id = None
    if qm_bytes is not None:
        queue_manager_id = uuid.UUID(bytes=bytes(qm_bytes))

    return models.Job(
        id=JobId(id_),
        priority=priority,
        created=_us_to_dt(created_us),
        updated=_us_to_dt(updated_us),
        heartbeat=_us_to_dt(heartbeat_us),
        execute_after=_us_to_dt(execute_after_us),
        entrypoint=entrypoint,
        payload=payload,
        queue_manager_id=queue_manager_id,
        status="picked",  # dequeue always returns picked jobs
        headers=headers_json,  # already JSON string from Rust
    )


class InMemoryRepository:
    """Pure-Rust in-memory adapter implementing all four PgQueuer ports.

    The Rust InMemoryCore handles all hot-path state; this class provides:
    - Asyncio lock for safe concurrent access from multiple tasks
    - Time conversions (datetime ↔ µs, timedelta → µs)
    - Pydantic model construction at API boundaries
    """

    def __init__(self) -> None:
        self._core = InMemoryCore()
        self._lock = asyncio.Lock()

        # Python-side state for schedules (not in Rust)
        self._schedules: dict[ScheduleId, models.Schedule] = {}
        self._schedule_seq = 1

    # ===================================================================
    # QueueRepositoryPort
    # ===================================================================

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
    ) -> list[models.Job]:
        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        now_us = _us()

        # Build per-entrypoint parameter arrays
        ep_names = list(entrypoints.keys())
        ep_retry_after_us = [_td_to_us(entrypoints[ep].retry_after) for ep in ep_names]
        ep_serialized = [entrypoints[ep].serialized for ep in ep_names]
        ep_concurrency_limits = [entrypoints[ep].concurrency_limit for ep in ep_names]

        # Convert UUID to 16-byte array (or None if not provided)
        qm_bytes = list(queue_manager_id.bytes) if queue_manager_id else [0] * 16

        async with self._lock:
            rows = self._core.dequeue_batch(
                batch_size,
                ep_names,
                ep_retry_after_us,
                ep_serialized,
                ep_concurrency_limits,
                qm_bytes,
                global_concurrency_limit,
                now_us,
            )

        return [_row_to_job(row) for row in rows]

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
        # Normalize to batch form
        if isinstance(entrypoint, list):
            ep_list = entrypoint
            pl_list = cast(list[bytes | None], payload)
            pr_list = cast(list[int], priority)
        else:
            ep_list = [entrypoint]
            pl_list = cast(list[bytes | None], [payload])
            pr_list = cast(list[int], [priority])
        n = len(ep_list)

        # normalize execute_after
        if execute_after is None:
            ea_list: list[timedelta] = [timedelta(0)] * n
        elif isinstance(execute_after, list):
            ea_list = [x or timedelta(0) for x in execute_after]
        else:
            ea_list = [execute_after or timedelta(0)]

        # Normalize dedupe_key
        if dedupe_key is None:
            dk_list: list[str | None] = [None] * n
        elif isinstance(dedupe_key, list):
            dk_list = dedupe_key
        else:
            dk_list = [dedupe_key]

        # Normalize headers: convert dict to JSON string
        if headers is None:
            hd_list: list[str | None] = [None] * n
        elif isinstance(headers, list):
            hd_list = [to_json(h).decode() if h is not None else None for h in headers]
        else:
            hd_list = [to_json(headers).decode() if headers is not None else None]

        # Convert timedeltas to microseconds
        ea_us_list = [_td_to_us(ea) for ea in ea_list]

        now_us = _us()

        async with self._lock:
            try:
                ids = self._core.enqueue_batch(
                    ep_list,
                    pl_list,
                    pr_list,
                    ea_us_list,
                    dk_list,
                    hd_list,
                    now_us,
                )
            except ValueError as e:
                if "Duplicate job error" in str(e):
                    raise errors.DuplicateJobError(dk_list) from e
                raise

        return [JobId(i) for i in ids]

    async def log_jobs(
        self,
        job_status: list[tuple[models.Job, models.JOB_STATUS, models.TracebackRecord | None]],
    ) -> None:
        job_ids: list[int] = []
        statuses: list[str] = []
        tracebacks: list[str | None] = []

        for job, status, tb in job_status:
            job_ids.append(int(job.id))
            statuses.append(status)
            tracebacks.append(to_json(tb.model_dump()).decode() if tb else None)

        now_us = _us()

        async with self._lock:
            self._core.log_jobs(job_ids, statuses, tracebacks, now_us)

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None:
        now_us = _us()

        async with self._lock:
            self._core.mark_cancelled([int(jid) for jid in ids], now_us)

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        now_us = _us()

        async with self._lock:
            self._core.update_heartbeat([int(jid) for jid in job_ids], now_us)

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        now_us = _us()

        if entrypoint is not None:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
        else:
            eps = None

        async with self._lock:
            self._core.clear_queue(eps, now_us)

    async def queue_size(self) -> list[models.QueueStatistics]:
        async with self._lock:
            stats = self._core.queue_size()

        result = []
        for ep, priority, status, count in stats:
            result.append(
                models.QueueStatistics(
                    count=count,
                    entrypoint=ep,
                    priority=priority,
                    status=cast(models.JOB_STATUS, status),
                )
            )

        return result

    async def queued_work(self, entrypoints: list[str]) -> int:
        async with self._lock:
            return self._core.queued_work(entrypoints)

    async def queue_log(self) -> list[models.Log]:
        async with self._lock:
            log_entries = self._core.queue_log()

        result = []
        for (
            created_us,
            job_id,
            status_str,
            priority,
            entrypoint,
            traceback,
            aggregated,
        ) in log_entries:
            result.append(
                models.Log(
                    created=_us_to_dt(created_us),
                    job_id=JobId(job_id),
                    status=cast(models.JOB_STATUS, status_str),
                    priority=priority,
                    entrypoint=entrypoint,
                    traceback=traceback,
                    aggregated=aggregated,
                )
            )

        return result

    async def job_status(
        self, ids: list[models.JobId]
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]:
        async with self._lock:
            statuses = self._core.job_status([int(jid) for jid in ids])

        result: list[tuple[models.JobId, models.JOB_STATUS]] = []
        for jid, status_str in statuses:
            result.append((JobId(jid), cast(models.JOB_STATUS, status_str)))

        return result

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        since_us = None
        if last is not None:
            since_us = _us() - _td_to_us(last)

        async with self._lock:
            stats = self._core.log_statistics(tail, since_us)

        result = []
        for entrypoint, priority, status_str, count, created_us in stats:
            result.append(
                models.LogStatistics(
                    count=count,
                    created=_us_to_dt(created_us),
                    entrypoint=entrypoint,
                    priority=priority,
                    status=cast(models.JOB_STATUS, status_str),
                )
            )

        return result

    # ===================================================================
    # ScheduleRepositoryPort
    # ===================================================================

    async def insert_schedule(
        self, schedules: dict[models.CronExpressionEntrypoint, timedelta]
    ) -> None:
        now = _now()
        existing = {(s.entrypoint, s.expression) for s in self._schedules.values()}

        for cron_ep in schedules:
            key = (cron_ep.entrypoint, cron_ep.expression)
            if key in existing:
                continue

            sid = ScheduleId(self._schedule_seq)
            self._schedule_seq += 1

            self._schedules[sid] = models.Schedule(
                id=sid,
                expression=cron_ep.expression,
                heartbeat=now,
                created=now,
                updated=now,
                next_run=now,
                status="queued",
                entrypoint=cron_ep.entrypoint,
            )

    async def fetch_schedule(
        self, entrypoints: dict[models.CronExpressionEntrypoint, timedelta]
    ) -> list[models.Schedule]:
        keys = {(c.entrypoint, c.expression) for c in entrypoints}
        return [s for s in self._schedules.values() if (s.entrypoint, s.expression) in keys]

    async def set_schedule_queued(self, ids: set[models.ScheduleId]) -> None:
        now = _now()
        for sid in ids:
            if sid in self._schedules:
                self._schedules[sid] = self._schedules[sid].model_copy(
                    update={"status": "queued", "updated": now}
                )

    async def update_schedule_heartbeat(self, ids: set[models.ScheduleId]) -> None:
        now = _now()
        for sid in ids:
            if sid in self._schedules:
                self._schedules[sid] = self._schedules[sid].model_copy(update={"heartbeat": now})

    async def peak_schedule(self) -> list[models.Schedule]:
        return list(self._schedules.values())

    async def delete_schedule(
        self,
        ids: set[models.ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None:
        to_del = [
            sid for sid, s in self._schedules.items() if sid in ids or s.entrypoint in entrypoints
        ]
        for sid in to_del:
            del self._schedules[sid]

    async def clear_schedule(self) -> None:
        self._schedules.clear()

    # ===================================================================
    # NotificationPort (no-ops)
    # ===================================================================

    async def notify_entrypoint_rps(self, entrypoint_count: dict[str, int]) -> None:
        pass

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None:
        pass

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None:
        pass

    # ===================================================================
    # SchemaManagementPort (no-ops)
    # ===================================================================

    async def install(self) -> None:
        pass

    async def uninstall(self) -> None:
        pass

    async def upgrade(self) -> None:
        pass

    async def has_table(self, table: str) -> bool:
        return True

    async def table_has_column(self, table: str, column: str) -> bool:
        return True

    async def table_has_index(self, table: str, index: str) -> bool:
        return True

    async def has_user_defined_enum(self, key: str, enum: str) -> bool:
        return True
