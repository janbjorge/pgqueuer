"""In-memory implementation of all PgQueuer port protocols.

Useful for unit testing and ephemeral background tasks without PostgreSQL.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import overload

from pydantic_core import to_json

from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, JobId, ScheduleId

# Pre-allocated constants
_ZERO_TIMEDELTA = timedelta(0)
_ZERO_TD_LIST: list[timedelta] = [_ZERO_TIMEDELTA]
_NONE_LIST: list[None] = [None]


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


class _EntrypointQueue:
    """Per-entrypoint storage with a priority heap for O(k log n) dequeue."""

    __slots__ = ("jobs", "queued_ids", "queued_heap", "picked")

    def __init__(self) -> None:
        self.jobs: dict[JobId, models.Job] = {}
        self.queued_ids: set[JobId] = set()
        self.queued_heap: list[tuple[int, JobId]] = []
        self.picked: dict[JobId, models.Job] = {}

    def pop_queued_candidates(
        self, now: datetime, limit: int
    ) -> list[tuple[int, JobId]]:
        """Pop up to `limit` ready candidates from the heap in priority order.

        Returns list of (-priority, jid). Stale entries (removed jobs) are
        skipped. Not-yet-ready jobs are pushed back.
        """
        result: list[tuple[int, JobId]] = []
        deferred: list[tuple[int, JobId]] = []
        heap = self.queued_heap
        queued_ids = self.queued_ids
        jobs = self.jobs

        while heap and len(result) < limit:
            neg_pri, jid = heapq.heappop(heap)
            if jid not in queued_ids:
                continue
            if jobs[jid].execute_after > now:
                deferred.append((neg_pri, jid))
                continue
            result.append((neg_pri, jid))

        for item in deferred:
            heapq.heappush(heap, item)

        return result

    def clear(self) -> None:
        self.jobs.clear()
        self.queued_ids.clear()
        self.queued_heap.clear()
        self.picked.clear()


class InMemoryRepository:
    """Pure-Python in-memory adapter implementing all four PgQueuer ports."""

    def __init__(self) -> None:
        self._job_seq = itertools.count(1)
        self._log: list[models.Log] = []
        self._lock = asyncio.Lock()
        self._dedupe: dict[str, JobId] = {}
        self._dedupe_reverse: dict[JobId, str] = {}

        self._queues: dict[str, _EntrypointQueue] = {}
        self._picked_count: Counter[str] = Counter()
        self._total_picked = 0

        self._schedules: dict[ScheduleId, models.Schedule] = {}
        self._schedule_seq = itertools.count(1)

    def _get_queue(self, entrypoint: str) -> _EntrypointQueue:
        q = self._queues.get(entrypoint)
        if q is None:
            q = _EntrypointQueue()
            self._queues[entrypoint] = q
        return q

    def _remove_job(self, jid: JobId, entrypoint: str) -> models.Job | None:
        q = self._queues.get(entrypoint)
        if q is None:
            return None

        job = q.jobs.pop(jid, None)
        if job is not None:
            dk = self._dedupe_reverse.pop(jid, None)
            if dk is not None:
                self._dedupe.pop(dk, None)
            if jid in q.picked:
                q.picked.pop(jid)
                self._picked_count[entrypoint] -= 1
                self._total_picked -= 1
            else:
                q.queued_ids.discard(jid)
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
            picked_by_ep = self._picked_count.copy()
            total_picked = self._total_picked

            candidates: list[tuple[int, JobId, str]] = []

            for ep_name, ep_config in entrypoints.items():
                q = self._queues.get(ep_name)
                if q is None:
                    continue

                # Skip queued candidates if constraints already exhausted
                if ep_config.serialized and picked_by_ep[ep_name] > 0:
                    pass
                elif (
                    ep_config.concurrency_limit > 0
                    and picked_by_ep[ep_name] >= ep_config.concurrency_limit
                ):
                    pass
                elif (
                    global_concurrency_limit is not None
                    and total_picked >= global_concurrency_limit
                ):
                    pass
                else:
                    for neg_pri, jid in q.pop_queued_candidates(now, batch_size):
                        candidates.append((neg_pri, jid, ep_name))

                # Check picked jobs for retries
                if ep_config.retry_after > _ZERO_TIMEDELTA:
                    retry_cutoff = now - ep_config.retry_after
                    for jid, job in q.picked.items():
                        if (
                            job.heartbeat < retry_cutoff
                            and job.execute_after <= now
                        ):
                            candidates.append((-job.priority, jid, ep_name))

            candidates.sort()

            result: list[models.Job] = []
            for _, jid, ep_name in candidates[:batch_size]:
                q = self._queues[ep_name]
                old = q.jobs[jid]

                updated = old.model_copy(
                    update={
                        "status": "picked",
                        "updated": now,
                        "heartbeat": now,
                        "queue_manager_id": queue_manager_id,
                    }
                )
                q.jobs[jid] = updated
                result.append(updated)

                if jid in q.queued_ids:
                    q.queued_ids.discard(jid)
                    q.picked[jid] = updated
                    self._picked_count[ep_name] += 1
                    self._total_picked += 1
                else:
                    # Retry path — already in picked, just update ref
                    q.picked[jid] = updated

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
        # Inline normalization to avoid function-call + dataclass overhead
        is_batch = isinstance(entrypoint, list)
        ep_list: list[str] = entrypoint if is_batch else [entrypoint]  # type: ignore[assignment]
        n = len(ep_list)
        pl_list: list[bytes | None] = payload if is_batch else [payload]  # type: ignore[assignment]
        pr_list: list[int] = priority if is_batch else [priority]  # type: ignore[assignment]

        if execute_after is None:
            ea_list: list[timedelta] = _ZERO_TD_LIST if n == 1 else [_ZERO_TIMEDELTA] * n
        elif isinstance(execute_after, list):
            ea_list = [x or _ZERO_TIMEDELTA for x in execute_after]
        else:
            ea_list = [execute_after or _ZERO_TIMEDELTA]

        if dedupe_key is None:
            dk_list: list[str | None] = _NONE_LIST if n == 1 else [None] * n
        elif isinstance(dedupe_key, list):
            dk_list = dedupe_key
        else:
            dk_list = [dedupe_key]

        if headers is None:
            hd_list: list[dict[str, str] | None] = _NONE_LIST if n == 1 else [None] * n  # type: ignore[assignment]
        elif isinstance(headers, list):
            hd_list = headers
        else:
            hd_list = [headers]

        # Dedupe check
        dedupe = self._dedupe
        for dk in dk_list:
            if dk is not None and dk in dedupe:
                raise errors.DuplicateJobError(dk_list)

        # Hot loop
        now = _now()
        job_seq = self._job_seq
        queues = self._queues
        dedupe_reverse = self._dedupe_reverse
        log = self._log
        ids: list[JobId] = []
        ids_append = ids.append
        log_append = log.append
        queue_cache: dict[str, _EntrypointQueue] = {}

        for i in range(n):
            ep = ep_list[i]
            pr = pr_list[i]
            jid = JobId(next(job_seq))

            q = queue_cache.get(ep)
            if q is None:
                q = queues.get(ep)
                if q is None:
                    q = _EntrypointQueue()
                    queues[ep] = q
                queue_cache[ep] = q

            hd = hd_list[i]
            job = models.Job.model_validate(
                {
                    "id": jid,
                    "priority": pr,
                    "created": now,
                    "updated": now,
                    "heartbeat": now,
                    "execute_after": now + ea_list[i],
                    "status": "queued",
                    "entrypoint": ep,
                    "payload": pl_list[i],
                    "queue_manager_id": None,
                    "headers": to_json(hd).decode() if hd is not None else None,
                }
            )

            q.jobs[jid] = job
            q.queued_ids.add(jid)
            heapq.heappush(q.queued_heap, (-pr, jid))

            dk = dk_list[i]
            if dk is not None:
                dedupe[dk] = jid
                dedupe_reverse[jid] = dk

            ids_append(jid)
            log_append(_make_log(jid, "queued", pr, ep))

        return ids

    async def log_jobs(
        self,
        job_status: list[
            tuple[models.Job, models.JOB_STATUS, models.TracebackRecord | None]
        ],
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
            for ep_name, q in self._queues.items():
                if jid in q.jobs:
                    job = self._remove_job(jid, ep_name)
                    if job is not None:
                        self._log.append(
                            _make_log(jid, "canceled", job.priority, ep_name)
                        )
                    break

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        now = _now()
        for jid in set(job_ids):
            for q in self._queues.values():
                if jid in q.jobs:
                    q.jobs[jid] = q.jobs[jid].model_copy(update={"heartbeat": now})
                    if jid in q.picked:
                        q.picked[jid] = q.jobs[jid]
                    break

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint is not None:
            eps = {entrypoint} if isinstance(entrypoint, str) else set(entrypoint)
            for ep in eps:
                q = self._queues.get(ep)
                if q is None:
                    continue
                for jid, job in q.jobs.items():
                    self._log.append(
                        _make_log(jid, "deleted", job.priority, ep)
                    )
                    dk = self._dedupe_reverse.pop(jid, None)
                    if dk is not None:
                        self._dedupe.pop(dk, None)
                    if jid in q.picked:
                        self._picked_count[ep] -= 1
                        self._total_picked -= 1
                q.clear()
        else:
            self._queues.clear()
            self._dedupe.clear()
            self._dedupe_reverse.clear()
            self._picked_count.clear()
            self._total_picked = 0

    async def queue_size(self) -> list[models.QueueStatistics]:
        counts: Counter[tuple[str, int, models.JOB_STATUS]] = Counter()
        for q in self._queues.values():
            for job in q.jobs.values():
                counts[(job.entrypoint, job.priority, job.status)] += 1
        return [
            models.QueueStatistics(count=c, entrypoint=ep, priority=pr, status=st)
            for (ep, pr, st), c in counts.items()
        ]

    async def queued_work(self, entrypoints: list[str]) -> int:
        ep_set = set(entrypoints)
        total = 0
        for ep in ep_set:
            q = self._queues.get(ep)
            if q is not None:
                total += len(q.queued_ids)
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
            models.LogStatistics(
                count=c, created=ts, entrypoint=ep, priority=pr, status=st
            )
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
        return [
            s
            for s in self._schedules.values()
            if (s.entrypoint, s.expression) in keys
        ]

    async def set_schedule_queued(self, ids: set[models.ScheduleId]) -> None:
        now = _now()
        for sid in ids:
            if sid in self._schedules:
                self._schedules[sid] = self._schedules[sid].model_copy(
                    update={"status": "queued", "updated": now}
                )

    async def update_schedule_heartbeat(
        self, ids: set[models.ScheduleId]
    ) -> None:
        now = _now()
        for sid in ids:
            if sid in self._schedules:
                self._schedules[sid] = self._schedules[sid].model_copy(
                    update={"heartbeat": now}
                )

    async def peak_schedule(self) -> list[models.Schedule]:
        return list(self._schedules.values())

    async def delete_schedule(
        self, ids: set[models.ScheduleId], entrypoints: set[CronEntrypoint]
    ) -> None:
        to_del = [
            sid
            for sid, s in self._schedules.items()
            if sid in ids or s.entrypoint in entrypoints
        ]
        for sid in to_del:
            del self._schedules[sid]

    async def clear_schedule(self) -> None:
        self._schedules.clear()

    # ===================================================================
    # NotificationPort (no-ops)
    # ===================================================================

    async def notify_entrypoint_rps(
        self, entrypoint_count: dict[str, int]
    ) -> None:
        pass

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None:
        pass

    async def notify_health_check(
        self, health_check_event_id: uuid.UUID
    ) -> None:
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
