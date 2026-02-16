"""In-memory implementation of all PgQueuer port protocols.

Useful for unit testing and ephemeral background tasks without PostgreSQL.
"""

from __future__ import annotations

import asyncio
import itertools
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import overload

from pydantic_core import to_json

from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.adapters.persistence.query_helpers import normalize_enqueue_params
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, JobId, ScheduleId


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_log(
    job_id: JobId,
    status: models.JOB_STATUS,
    priority: int,
    entrypoint: str,
    traceback: str | None = None,
) -> models.Log:
    return models.Log(
        created=_now(),
        job_id=job_id,
        status=status,
        priority=priority,
        entrypoint=entrypoint,
        traceback=traceback,
        aggregated=False,
    )


class InMemoryRepository:
    """Pure-Python in-memory adapter implementing all four PgQueuer ports."""

    def __init__(self) -> None:
        self._jobs: dict[JobId, models.Job] = {}
        self._job_seq = itertools.count(1)
        self._log: list[models.Log] = []
        self._lock = asyncio.Lock()
        self._dedupe: dict[str, JobId] = {}

        self._schedules: dict[ScheduleId, models.Schedule] = {}
        self._schedule_seq = itertools.count(1)

    def _remove_job(self, jid: JobId) -> models.Job | None:
        job = self._jobs.pop(jid, None)
        if job is not None:
            # Clean dedupe index
            self._dedupe = {k: v for k, v in self._dedupe.items() if v != jid}
        return job

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

        now = _now()
        async with self._lock:
            picked_by_ep: Counter[str] = Counter(
                j.entrypoint for j in self._jobs.values() if j.status == "picked"
            )
            total_picked = sum(picked_by_ep.values())

            candidates: list[tuple[int, int, JobId]] = []
            for job in self._jobs.values():
                if job.entrypoint not in entrypoints:
                    continue
                ep = entrypoints[job.entrypoint]

                is_queued = job.status == "queued" and job.execute_after <= now
                is_retry = (
                    job.status == "picked"
                    and ep.retry_after > timedelta(0)
                    and job.heartbeat < now - ep.retry_after
                    and job.execute_after <= now
                )
                if not (is_queued or is_retry):
                    continue

                # Skip if constraints are violated (retries bypass constraints)
                if not is_retry:
                    if ep.serialized and picked_by_ep[job.entrypoint] > 0:
                        continue
                    if (
                        ep.concurrency_limit > 0
                        and picked_by_ep[job.entrypoint] >= ep.concurrency_limit
                    ):
                        continue
                    if (
                        global_concurrency_limit is not None
                        and total_picked >= global_concurrency_limit
                    ):
                        continue

                candidates.append((-job.priority, job.id, job.id))

            candidates.sort()

            result: list[models.Job] = []
            for _, _, jid in candidates[:batch_size]:
                job = self._jobs[jid]
                updated = job.model_copy(
                    update={
                        "status": "picked",
                        "updated": now,
                        "heartbeat": now,
                        "queue_manager_id": queue_manager_id,
                    }
                )
                self._jobs[jid] = updated
                result.append(updated)
                picked_by_ep[updated.entrypoint] += 1
                total_picked += 1
                self._log.append(_make_log(jid, "picked", updated.priority, updated.entrypoint))

            return result

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
        normed = normalize_enqueue_params(
            entrypoint, payload, priority, execute_after, dedupe_key, headers
        )
        now = _now()

        # Check all dedupe keys up front
        for dk in normed.dedupe_key:
            if dk is not None and dk in self._dedupe:
                raise errors.DuplicateJobError(normed.dedupe_key)

        ids: list[JobId] = []
        for ep, pl, pr, ea, dk, hd in zip(
            normed.entrypoint,
            normed.payload,
            normed.priority,
            normed.execute_after,
            normed.dedupe_key,
            normed.headers,
            strict=True,
        ):
            jid = JobId(next(self._job_seq))
            job = models.Job.model_validate(
                {
                    "id": jid,
                    "priority": pr,
                    "created": now,
                    "updated": now,
                    "heartbeat": now,
                    "execute_after": now + ea,
                    "status": "queued",
                    "entrypoint": ep,
                    "payload": pl,
                    "queue_manager_id": None,
                    "headers": to_json(hd).decode() if hd is not None else None,
                }
            )
            self._jobs[jid] = job
            if dk is not None:
                self._dedupe[dk] = jid
            ids.append(jid)
            self._log.append(_make_log(jid, "queued", pr, ep))

        return ids

    async def log_jobs(
        self,
        job_status: list[tuple[models.Job, models.JOB_STATUS, models.TracebackRecord | None]],
    ) -> None:
        for job, status, tb in job_status:
            self._remove_job(job.id)
            self._log.append(
                _make_log(
                    job.id,
                    status,
                    job.priority,
                    job.entrypoint,
                    traceback=to_json(tb.model_dump()).decode() if tb else None,
                )
            )

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None:
        for jid in ids:
            job = self._remove_job(jid)
            if job is not None:
                self._log.append(_make_log(jid, "canceled", job.priority, job.entrypoint))

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        now = _now()
        for jid in set(job_ids):
            if jid in self._jobs:
                self._jobs[jid] = self._jobs[jid].model_copy(update={"heartbeat": now})

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint is not None:
            # Delete by entrypoint — log each removal (matches PG CTE behavior)
            eps = {entrypoint} if isinstance(entrypoint, str) else set(entrypoint)
            to_remove = [jid for jid, j in self._jobs.items() if j.entrypoint in eps]
            for jid in to_remove:
                job = self._remove_job(jid)
                if job is not None:
                    self._log.append(_make_log(jid, "deleted", job.priority, job.entrypoint))
        else:
            # Truncate — no log entries (matches PG TRUNCATE behavior)
            self._jobs.clear()
            self._dedupe.clear()

    async def queue_size(self) -> list[models.QueueStatistics]:
        counts: Counter[tuple[str, int, models.JOB_STATUS]] = Counter()
        for job in self._jobs.values():
            counts[(job.entrypoint, job.priority, job.status)] += 1
        return [
            models.QueueStatistics(count=c, entrypoint=ep, priority=pr, status=st)
            for (ep, pr, st), c in counts.items()
        ]

    async def queued_work(self, entrypoints: list[str]) -> int:
        ep_set = set(entrypoints)
        return sum(
            1 for j in self._jobs.values() if j.status == "queued" and j.entrypoint in ep_set
        )

    async def queue_log(self) -> list[models.Log]:
        return list(self._log)

    async def job_status(
        self, ids: list[models.JobId]
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]:
        id_set = set(ids)
        seen: dict[JobId, models.JOB_STATUS] = {}
        for entry in reversed(self._log):
            if entry.job_id in id_set and entry.job_id not in seen:
                seen[entry.job_id] = entry.status
                if len(seen) == len(id_set):
                    break
        return list(seen.items())

    async def log_statistics(
        self, tail: int | None, last: timedelta | None = None
    ) -> list[models.LogStatistics]:
        # Aggregate all log entries, grouped by (entrypoint, priority, status, second)
        counts: Counter[tuple[str, int, models.JOB_STATUS, datetime]] = Counter()
        for entry in self._log:
            key = (
                entry.entrypoint,
                entry.priority,
                entry.status,
                entry.created.replace(microsecond=0),
            )
            counts[key] += 1

        stats = [
            models.LogStatistics(count=c, created=ts, entrypoint=ep, priority=pr, status=st)
            for (ep, pr, st, ts), c in counts.items()
        ]

        if last is not None:
            cutoff = _now() - last
            stats = [s for s in stats if s.created >= cutoff]

        if tail is not None:
            stats = stats[-tail:]

        return stats

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
            sid = ScheduleId(next(self._schedule_seq))
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
        self, ids: set[models.ScheduleId], entrypoints: set[CronEntrypoint]
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
