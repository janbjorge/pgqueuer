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
        self._job_seq = itertools.count(1)
        self._log: list[models.Log] = []
        self._lock = asyncio.Lock()
        self._dedupe: dict[str, JobId] = {}

        # Per-entrypoint storage: entrypoint -> (jobs dict, queued list, picked dict)
        self._queues: dict[str, tuple[dict[JobId, models.Job], list[JobId], dict[JobId, models.Job]]] = {}
        # Maintain count of picked jobs to avoid O(n) scan in dequeue
        self._picked_count: Counter[str] = Counter()  # entrypoint -> count
        self._total_picked = 0

        self._schedules: dict[ScheduleId, models.Schedule] = {}
        self._schedule_seq = itertools.count(1)

    def _get_queue(self, entrypoint: str):
        """Get or create queue storage for entrypoint."""
        if entrypoint not in self._queues:
            self._queues[entrypoint] = ({}, [], {})  # (jobs, queued_list, picked_dict)
        return self._queues[entrypoint]

    def _remove_job(self, jid: JobId, entrypoint: str) -> models.Job | None:
        """Remove a job from its entrypoint queue."""
        queue_tuple = self._queues.get(entrypoint)
        if queue_tuple is None:
            return None

        jobs_dict, queued_list, picked_dict = queue_tuple
        job = jobs_dict.pop(jid, None)
        if job is not None:
            # Clean dedupe index
            self._dedupe = {k: v for k, v in self._dedupe.items() if v != jid}
            # Update picked count and remove from picked/queued
            if jid in picked_dict:
                picked_dict.pop(jid)
                self._picked_count[entrypoint] -= 1
                self._total_picked -= 1
            elif jid in queued_list:
                queued_list.remove(jid)
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
            # Use maintained counters instead of O(n) scan
            picked_by_ep = self._picked_count.copy()
            total_picked = self._total_picked

            candidates: list[tuple[int, JobId, str]] = []

            # Only iterate requested entrypoints
            for ep_name, ep_config in entrypoints.items():
                queue_tuple = self._queues.get(ep_name)
                if queue_tuple is None:
                    continue

                jobs_dict, queued_list, picked_dict = queue_tuple

                # Check queued jobs for this entrypoint
                for jid in queued_list:
                    job = jobs_dict[jid]

                    # Check if job is ready to execute
                    if job.execute_after > now:
                        continue

                    # Skip if constraints are violated
                    if ep_config.serialized and picked_by_ep[ep_name] > 0:
                        continue
                    if (
                        ep_config.concurrency_limit > 0
                        and picked_by_ep[ep_name] >= ep_config.concurrency_limit
                    ):
                        continue
                    if (
                        global_concurrency_limit is not None
                        and total_picked >= global_concurrency_limit
                    ):
                        continue

                    candidates.append((-job.priority, jid, ep_name))

                # Check picked jobs for retries for this entrypoint
                for jid, job in picked_dict.items():
                    is_retry = (
                        ep_config.retry_after > timedelta(0)
                        and job.heartbeat < now - ep_config.retry_after
                        and job.execute_after <= now
                    )
                    if is_retry:
                        candidates.append((-job.priority, jid, ep_name))

            candidates.sort()

            result: list[models.Job] = []
            for _, jid, ep_name in candidates[:batch_size]:
                queue_tuple = self._queues[ep_name]
                jobs_dict, queued_list, picked_dict = queue_tuple
                job = jobs_dict[jid]

                updated = job.model_copy(
                    update={
                        "status": "picked",
                        "updated": now,
                        "heartbeat": now,
                        "queue_manager_id": queue_manager_id,
                    }
                )
                jobs_dict[jid] = updated
                result.append(updated)

                # Move from queued to picked if it was queued
                if jid in queued_list:
                    queued_list.remove(jid)
                    picked_dict[jid] = updated
                    self._picked_count[ep_name] += 1
                    self._total_picked += 1

                self._log.append(_make_log(jid, "picked", updated.priority, ep_name))

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
            # Add to per-entrypoint queue
            queue_tuple = self._get_queue(ep)
            jobs_dict, queued_list, picked_dict = queue_tuple
            jobs_dict[jid] = job
            queued_list.append(jid)

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
            self._remove_job(job.id, job.entrypoint)
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
            # Find which queue contains this job
            for ep_name, queue_tuple in self._queues.items():
                jobs_dict, _, _ = queue_tuple
                if jid in jobs_dict:
                    job = self._remove_job(jid, ep_name)
                    if job is not None:
                        self._log.append(_make_log(jid, "canceled", job.priority, ep_name))
                    break

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        now = _now()
        for jid in set(job_ids):
            # Find the job in any queue
            for queue_tuple in self._queues.values():
                jobs_dict, _, picked_dict = queue_tuple
                if jid in jobs_dict:
                    jobs_dict[jid] = jobs_dict[jid].model_copy(update={"heartbeat": now})
                    # Also update in picked if it's there
                    if jid in picked_dict:
                        picked_dict[jid] = jobs_dict[jid]
                    break

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint is not None:
            # Delete by entrypoint — log each removal (matches PG CTE behavior)
            eps = {entrypoint} if isinstance(entrypoint, str) else set(entrypoint)
            for ep in eps:
                queue_tuple = self._queues.get(ep)
                if queue_tuple is None:
                    continue
                jobs_dict, queued_list, picked_dict = queue_tuple
                # Log each job removal
                for jid in list(jobs_dict.keys()):
                    job = jobs_dict[jid]
                    self._log.append(_make_log(jid, "deleted", job.priority, ep))
                    # Clean up counters
                    if jid in picked_dict:
                        self._picked_count[ep] -= 1
                        self._total_picked -= 1
                # Clear the queue
                jobs_dict.clear()
                queued_list.clear()
                picked_dict.clear()
        else:
            # Truncate — no log entries (matches PG TRUNCATE behavior)
            self._queues.clear()
            self._dedupe.clear()
            self._picked_count.clear()
            self._total_picked = 0

    async def queue_size(self) -> list[models.QueueStatistics]:
        counts: Counter[tuple[str, int, models.JOB_STATUS]] = Counter()
        for queue_tuple in self._queues.values():
            jobs_dict, _, _ = queue_tuple
            for job in jobs_dict.values():
                counts[(job.entrypoint, job.priority, job.status)] += 1
        return [
            models.QueueStatistics(count=c, entrypoint=ep, priority=pr, status=st)
            for (ep, pr, st), c in counts.items()
        ]

    async def queued_work(self, entrypoints: list[str]) -> int:
        ep_set = set(entrypoints)
        total = 0
        for ep in ep_set:
            queue_tuple = self._queues.get(ep)
            if queue_tuple is not None:
                _, queued_list, _ = queue_tuple
                total += len(queued_list)
        return total

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
