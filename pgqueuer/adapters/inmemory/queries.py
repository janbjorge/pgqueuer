"""In-memory queries satisfying all four port protocols.

Pure Python data structures — no SQL, no database driver calls.
"""

from __future__ import annotations

import asyncio
import dataclasses
import heapq
import uuid
from datetime import datetime, timedelta
from typing import Any, Literal, overload

from pydantic_core import to_json
from typing_extensions import assert_never

from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.adapters.persistence import qb, query_helpers
from pgqueuer.adapters.persistence.query_helpers import merge_tracing_headers
from pgqueuer.domain import errors, models
from pgqueuer.domain.models import utc_now
from pgqueuer.domain.types import CronEntrypoint, JobId, OnConflict, ScheduleId, SortOrder
from pgqueuer.ports import tracing
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.ports.tracing import TracingProtocol


@dataclasses.dataclass
class InMemoryQueries:
    """Drop-in replacement for ``Queries`` backed by pure Python dicts."""

    driver: InMemoryDriver

    qbe: qb.QueryBuilderEnvironment = dataclasses.field(
        default_factory=qb.QueryBuilderEnvironment,
    )
    qbq: qb.QueryQueueBuilder = dataclasses.field(
        default_factory=qb.QueryQueueBuilder,
    )
    qbs: qb.QuerySchedulerBuilder = dataclasses.field(
        default_factory=qb.QuerySchedulerBuilder,
    )

    tracer: TracingProtocol | None = None

    _jobs: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict, init=False)
    _log: list[dict[str, Any]] = dataclasses.field(default_factory=list, init=False)
    _statistics: list[dict[str, Any]] = dataclasses.field(default_factory=list, init=False)
    _schedules: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict, init=False)
    _dedupe_index: dict[str, int] = dataclasses.field(default_factory=dict, init=False)
    _dedupe_key_by_job: dict[int, str] = dataclasses.field(default_factory=dict, init=False)

    # Dequeue indexes. Heap entries are never removed eagerly; validity is
    # re-checked against ``_jobs`` on pop, so entries for jobs that were
    # cancelled, cleared, or re-queued are skipped as tombstones.
    _ready_heaps: dict[str, list[tuple[int, int]]] = dataclasses.field(
        default_factory=dict, init=False
    )
    _deferred_heap: list[tuple[datetime, int, str]] = dataclasses.field(
        default_factory=list, init=False
    )
    _picked_ids: set[int] = dataclasses.field(default_factory=set, init=False)

    _next_job_id: int = dataclasses.field(default=1, init=False)
    _next_log_id: int = dataclasses.field(default=1, init=False)
    _next_schedule_id: int = dataclasses.field(default=1, init=False)
    _next_stats_id: int = dataclasses.field(default=1, init=False)

    async def install(self) -> None:
        pass

    async def upgrade(self) -> None:
        pass

    async def uninstall(self) -> None:
        self._jobs.clear()
        self._log.clear()
        self._statistics.clear()
        self._schedules.clear()
        self._dedupe_index.clear()
        self._dedupe_key_by_job.clear()
        self._ready_heaps.clear()
        self._deferred_heap.clear()
        self._picked_ids.clear()

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
        on_conflict: Literal["raise"] = "raise",
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
        on_conflict: Literal["raise"] = "raise",
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
        normed = query_helpers.normalize_enqueue_params(
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

        if on_conflict == "raise":
            seen = set[str]()
            for dk in normed.dedupe_key:
                if dk is None:
                    continue
                if dk in self._dedupe_index or dk in seen:
                    raise errors.DuplicateJobError(normed.dedupe_key)
                seen.add(dk)
        elif on_conflict == "skip":
            pass  # Duplicates resolve per row in the insert loop below.
        else:
            assert_never(on_conflict)

        now = utc_now()
        ids: list[JobId | None] = []

        for i in range(len(normed.entrypoint)):
            dk = normed.dedupe_key[i]
            if dk is not None and dk in self._dedupe_index:
                ids.append(None)
                continue

            job_id = self._next_job_id
            self._next_job_id += 1

            ea = now + normed.execute_after[i]
            hdr = to_json(normed.headers[i]).decode()

            job_dict: dict[str, Any] = {
                "id": job_id,
                "priority": normed.priority[i],
                "created": now,
                "updated": now,
                "heartbeat": now,
                "execute_after": ea,
                "status": "queued",
                "entrypoint": normed.entrypoint[i],
                "payload": normed.payload[i],
                "attempts": 0,
                "queue_manager_id": None,
                "headers": hdr,
            }

            if dk is not None:
                self._dedupe_index[dk] = job_id
                self._dedupe_key_by_job[job_id] = dk

            self._jobs[job_id] = job_dict
            self._push_queued_job(job_dict, now)
            ids.append(JobId(job_id))

            self._log.append(
                {
                    "id": self._next_log_id,
                    "created": now,
                    "job_id": job_id,
                    "status": "queued",
                    "priority": normed.priority[i],
                    "entrypoint": normed.entrypoint[i],
                    "traceback": None,
                    "aggregated": False,
                }
            )
            self._next_log_id += 1

        await self.emit_table_changed("insert")
        return ids

    def _push_queued_job(self, job: dict[str, Any], now: datetime) -> None:
        if job["execute_after"] <= now:
            heapq.heappush(
                self._ready_heaps.setdefault(job["entrypoint"], []),
                (-job["priority"], job["id"]),
            )
        else:
            heapq.heappush(
                self._deferred_heap,
                (job["execute_after"], job["id"], job["entrypoint"]),
            )

    def _promote_due_deferred(self, now: datetime) -> None:
        while self._deferred_heap and self._deferred_heap[0][0] <= now:
            _, job_id, ep = heapq.heappop(self._deferred_heap)
            j = self._jobs.get(job_id)
            if j is None or j["status"] != "queued":
                continue
            if j["execute_after"] > now:
                # retry_job pushed the job further into the future after this
                # entry was created; track the current deadline instead.
                heapq.heappush(self._deferred_heap, (j["execute_after"], job_id, ep))
            else:
                heapq.heappush(
                    self._ready_heaps.setdefault(ep, []),
                    (-j["priority"], job_id),
                )

    def _pop_ready_jobs(
        self,
        entrypoint: str,
        limit: int,
        now: datetime,
        seen: set[int],
    ) -> list[dict[str, Any]]:
        popped: list[dict[str, Any]] = []
        heap = self._ready_heaps.get(entrypoint)
        if heap is None:
            return popped
        while heap and len(popped) < limit:
            _, job_id = heapq.heappop(heap)
            j = self._jobs.get(job_id)
            if j is None or j["status"] != "queued" or job_id in seen:
                continue
            if j["execute_after"] > now:
                heapq.heappush(self._deferred_heap, (j["execute_after"], job_id, entrypoint))
                continue
            seen.add(job_id)
            popped.append(j)
        return popped

    def _count_picked_jobs(
        self,
        queue_manager_id: uuid.UUID,
        entrypoints: dict[str, EntrypointExecutionParameter],
    ) -> tuple[dict[str, int], int]:
        picked_per_ep: dict[str, int] = {}
        total_picked = 0
        for job_id in self._picked_ids:
            j = self._jobs[job_id]
            if j["queue_manager_id"] == queue_manager_id:
                total_picked += 1
            ep = j["entrypoint"]
            if ep in entrypoints:
                picked_per_ep[ep] = picked_per_ep.get(ep, 0) + 1
        return picked_per_ep, total_picked

    def _collect_stale_candidates(
        self,
        now: datetime,
        entrypoints: dict[str, EntrypointExecutionParameter],
        heartbeat_timeout: timedelta,
    ) -> list[dict[str, Any]]:
        candidates = [
            j
            for j in (self._jobs[job_id] for job_id in self._picked_ids)
            if j["entrypoint"] in entrypoints and now - j["heartbeat"] >= heartbeat_timeout
        ]
        candidates.sort(key=lambda j: (-j["priority"], j["id"]))
        return candidates

    def _write_picked_logs(
        self,
        jobs: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        for j in jobs:
            self._log.append(
                {
                    "id": self._next_log_id,
                    "created": now,
                    "job_id": j["id"],
                    "status": "picked",
                    "priority": j["priority"],
                    "entrypoint": j["entrypoint"],
                    "traceback": None,
                    "aggregated": False,
                }
            )
            self._next_log_id += 1

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> list[models.Job]:
        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        if not entrypoints:
            return []

        now = utc_now()

        picked_per_ep, total_picked = self._count_picked_jobs(queue_manager_id, entrypoints)

        if global_concurrency_limit is not None and total_picked >= global_concurrency_limit:
            return []

        remaining = batch_size
        if global_concurrency_limit is not None:
            remaining = min(remaining, global_concurrency_limit - total_picked)

        self._promote_due_deferred(now)

        seen = set[int]()
        queued_candidates: list[dict[str, Any]] = []
        for ep, params in entrypoints.items():
            cap = remaining
            if params.concurrency_limit > 0:
                cap = min(cap, params.concurrency_limit - picked_per_ep.get(ep, 0))
            if cap > 0:
                queued_candidates.extend(self._pop_ready_jobs(ep, cap, now, seen))

        stale_candidates = self._collect_stale_candidates(now, entrypoints, heartbeat_timeout)

        # One global priority order so recovered stale jobs compete on priority
        # with fresh work instead of always losing to it (mirrors eligible in
        # qb.py). Per-entrypoint concurrency is enforced by the capped pops
        # above; stale rows transfer ownership of an already-counted pick, so
        # they bypass the gate (mirrors next_stale in qb.py).
        merged = sorted(
            queued_candidates + stale_candidates,
            key=lambda j: (-j["priority"], j["id"]),
        )
        selected = merged[:remaining]
        selected_ids = {j["id"] for j in selected}

        # Popped-but-unselected queued jobs must return to their ready heap,
        # or they would never be delivered.
        for j in queued_candidates:
            if j["id"] not in selected_ids:
                heapq.heappush(
                    self._ready_heaps.setdefault(j["entrypoint"], []),
                    (-j["priority"], j["id"]),
                )

        for j in selected:
            j["status"] = "picked"
            j["queue_manager_id"] = queue_manager_id
            j["updated"] = now
            j["heartbeat"] = now
        self._picked_ids.update(selected_ids)

        self._write_picked_logs(selected, now)

        # Yield to the event loop.  In the PostgreSQL path every dequeue
        # suspends on real I/O (driver.fetch); without this explicit yield
        # QueueManager.fetch_jobs can loop indefinitely and starve signals,
        # timers, and other tasks.
        await asyncio.sleep(0)

        return [models.Job.model_validate(j) for j in selected]

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
        now = utc_now()
        for job, status, tb in job_status:
            jid = int(job.id)
            if status == "failed":
                j = self._jobs.get(jid)
                if j is not None:
                    j["status"] = "failed"
                    j["updated"] = now
                    j["queue_manager_id"] = None
                self._remove_dedupe_for_job(jid)
            else:
                self._jobs.pop(jid, None)
                self._remove_dedupe_for_job(jid)
            self._picked_ids.discard(jid)
            self._log.append(
                {
                    "id": self._next_log_id,
                    "created": now,
                    "job_id": jid,
                    "status": status,
                    "priority": job.priority,
                    "entrypoint": job.entrypoint,
                    "traceback": tb.model_dump_json() if tb else None,
                    "aggregated": False,
                }
            )
            self._next_log_id += 1

    async def retry_job(
        self,
        job: models.Job,
        delay: timedelta,
        traceback_record: models.TracebackRecord | None,
    ) -> None:
        now = utc_now()
        jid = int(job.id)
        j = self._jobs.get(jid)
        if j is not None:
            j["status"] = "queued"
            j["execute_after"] = now + delay
            j["attempts"] = j["attempts"] + 1
            j["updated"] = now
            j["queue_manager_id"] = None
            self._picked_ids.discard(jid)
            self._push_queued_job(j, now)
            self._log.append(
                {
                    "id": self._next_log_id,
                    "created": now,
                    "job_id": jid,
                    "status": "queued",
                    "priority": j["priority"],
                    "entrypoint": j["entrypoint"],
                    "traceback": (traceback_record.model_dump_json() if traceback_record else None),
                    "aggregated": False,
                }
            )
            self._next_log_id += 1
            await self.emit_table_changed("update")

    async def requeue_jobs(self, ids: list[JobId]) -> None:
        now = utc_now()
        for jid in ids:
            j = self._jobs.get(int(jid))
            if j is not None and j["status"] == "failed":
                j["status"] = "queued"
                j["execute_after"] = now
                j["updated"] = now
                j["attempts"] = 0
                j["queue_manager_id"] = None
                self._push_queued_job(j, now)
                self._log.append(
                    {
                        "id": self._next_log_id,
                        "created": now,
                        "job_id": int(jid),
                        "status": "queued",
                        "priority": j["priority"],
                        "entrypoint": j["entrypoint"],
                        "traceback": None,
                        "aggregated": False,
                    }
                )
                self._next_log_id += 1
                await self.emit_table_changed("update")

    async def list_failed_jobs(
        self,
        limit: int = 100,
        order: SortOrder = "DESC",
    ) -> list[models.Job]:
        failed = [j for j in self._jobs.values() if j["status"] == "failed"]
        failed.sort(key=lambda j: j["created"], reverse=(order != "ASC"))
        return [models.Job.model_validate(j) for j in failed[:limit]]

    async def mark_job_as_cancelled(self, ids: list[JobId]) -> None:
        now = utc_now()
        for jid in ids:
            job_dict = self._jobs.pop(int(jid), None)
            if job_dict is not None:
                self._remove_dedupe_for_job(int(jid))
                self._picked_ids.discard(int(jid))
                self._log.append(
                    {
                        "id": self._next_log_id,
                        "created": now,
                        "job_id": int(jid),
                        "status": "canceled",
                        "priority": job_dict["priority"],
                        "entrypoint": job_dict["entrypoint"],
                        "traceback": None,
                        "aggregated": False,
                    }
                )
                self._next_log_id += 1

        await self.notify_job_cancellation(ids)

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            now = utc_now()
            to_remove = [jid for jid, j in self._jobs.items() if j["entrypoint"] in eps]
            for jid in to_remove:
                j = self._jobs.pop(jid)
                self._remove_dedupe_for_job(jid)
                self._picked_ids.discard(jid)
                self._log.append(
                    {
                        "id": self._next_log_id,
                        "created": now,
                        "job_id": jid,
                        "status": "deleted",
                        "priority": j["priority"],
                        "entrypoint": j["entrypoint"],
                        "traceback": None,
                        "aggregated": False,
                    }
                )
                self._next_log_id += 1
        else:
            # TRUNCATE equivalent — no log entries
            self._jobs.clear()
            self._dedupe_index.clear()
            self._dedupe_key_by_job.clear()
            self._ready_heaps.clear()
            self._deferred_heap.clear()
            self._picked_ids.clear()

    async def queue_size(self) -> list[models.QueueStatistics]:
        counts: dict[tuple[str, int, str], int] = {}
        for j in self._jobs.values():
            key = (j["entrypoint"], j["priority"], j["status"])
            counts[key] = counts.get(key, 0) + 1
        return [
            models.QueueStatistics(
                count=count,
                entrypoint=ep,
                priority=pri,
                status=st,
            )
            for (ep, pri, st), count in sorted(counts.items())
        ]

    async def queued_work(self, entrypoints: list[str]) -> int:
        ep_set = set(entrypoints)
        return sum(
            1 for j in self._jobs.values() if j["status"] == "queued" and j["entrypoint"] in ep_set
        )

    async def eligible_queued_work(self, entrypoints: list[str]) -> int:
        """Like ``queued_work`` but counting only jobs whose ``execute_after`` has passed."""
        now = utc_now()
        ep_set = set(entrypoints)
        return sum(
            1
            for j in self._jobs.values()
            if j["status"] == "queued" and j["entrypoint"] in ep_set and j["execute_after"] <= now
        )

    async def queue_log(self) -> list[models.Log]:
        return [models.Log.model_validate(entry) for entry in self._log]

    async def update_heartbeat(self, job_ids: list[JobId]) -> None:
        now = utc_now()
        unique_ids = {int(jid) for jid in job_ids}
        for jid in unique_ids:
            if jid in self._jobs:
                self._jobs[jid]["heartbeat"] = now

    async def job_status(
        self,
        ids: list[JobId],
    ) -> list[tuple[JobId, models.JOB_STATUS]]:
        id_set = {int(jid) for jid in ids}
        latest: dict[int, models.JOB_STATUS] = {}
        for entry in reversed(self._log):
            jid = entry["job_id"]
            if jid in id_set and jid not in latest:
                latest[jid] = entry["status"]
        return [(JobId(jid), st) for jid, st in latest.items()]

    async def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        to_agg: dict[tuple[str, int, str, datetime], int] = {}
        for entry in self._log:
            if entry["aggregated"]:
                continue
            created_sec = entry["created"].replace(microsecond=0)
            key = (
                entry["entrypoint"],
                entry["priority"],
                entry["status"],
                created_sec,
            )
            to_agg[key] = to_agg.get(key, 0) + 1
            entry["aggregated"] = True

        for (ep, pri, st, created_sec), count in to_agg.items():
            self._statistics.append(
                {
                    "id": self._next_stats_id,
                    "created": created_sec,
                    "count": count,
                    "entrypoint": ep,
                    "priority": pri,
                    "status": st,
                }
            )
            self._next_stats_id += 1

        result = list(self._statistics)

        if last is not None:
            cutoff = utc_now() - last
            result = [r for r in result if r["created"] >= cutoff]

        result.sort(key=lambda r: r["id"], reverse=True)

        if limit is not None:
            result = result[:limit]

        return [models.LogStatistics.model_validate(r) for r in result]

    async def notify_job_cancellation(self, ids: list[JobId]) -> None:
        event = models.CancellationEvent(
            channel=self.qbq.settings.channel,
            ids=ids,
            sent_at=utc_now(),
            type="cancellation_event",
        )
        await self.driver.notify(self.qbq.settings.channel, event.model_dump_json())

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None:
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
        now = utc_now()
        for key, delay in schedules.items():
            # ON CONFLICT DO NOTHING: skip if expression+entrypoint exists
            exists = any(
                s["expression"] == key.expression and s["entrypoint"] == key.entrypoint
                for s in self._schedules.values()
            )
            if exists:
                continue

            sid = self._next_schedule_id
            self._next_schedule_id += 1
            # Truncate next_run to seconds (matching date_trunc('seconds', ...))
            next_run = (now + delay).replace(microsecond=0)
            self._schedules[sid] = {
                "id": sid,
                "expression": key.expression,
                "entrypoint": key.entrypoint,
                "heartbeat": now,
                "created": now,
                "updated": now,
                "next_run": next_run,
                "last_run": None,
                "status": "queued",
            }

    async def fetch_schedule(
        self,
        entrypoints: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> list[models.Schedule]:
        now = utc_now()
        selected: list[dict[str, Any]] = []

        ep_set = {(k.expression, k.entrypoint): v for k, v in entrypoints.items()}

        for s in self._schedules.values():
            key = (s["expression"], s["entrypoint"])
            if key not in ep_set:
                continue

            # Pick if (queued AND next_run <= now) OR (picked AND heartbeat stale >30s)
            if s["status"] == "queued" and s["next_run"] <= now:
                s["status"] = "picked"
                s["heartbeat"] = now
                s["updated"] = now
                selected.append(s)
            elif s["status"] == "picked" and (now - s["heartbeat"]).total_seconds() > 30:
                s["heartbeat"] = now
                s["updated"] = now
                selected.append(s)

        return [models.Schedule.model_validate(s) for s in selected]

    async def set_schedule_queued(self, ids: set[ScheduleId]) -> None:
        now = utc_now()
        for sid in ids:
            s = self._schedules.get(int(sid))
            if s is not None:
                s["status"] = "queued"
                s["last_run"] = now
                s["updated"] = now

    async def update_schedule_heartbeat(self, ids: set[ScheduleId]) -> None:
        now = utc_now()
        for sid in ids:
            s = self._schedules.get(int(sid))
            if s is not None:
                s["heartbeat"] = now
                s["updated"] = now

    async def peek_schedule(self) -> list[models.Schedule]:
        return [models.Schedule.model_validate(s) for s in self._schedules.values()]

    async def delete_schedule(
        self,
        ids: set[ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None:
        id_ints = {int(sid) for sid in ids}
        to_remove = [
            sid
            for sid, s in self._schedules.items()
            if sid in id_ints or s["entrypoint"] in entrypoints
        ]
        for sid in to_remove:
            del self._schedules[sid]

    async def clear_schedule(self) -> None:
        self._schedules.clear()

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            ep_set = set(eps)
            self._statistics = [s for s in self._statistics if s["entrypoint"] not in ep_set]
        else:
            self._statistics.clear()

    async def clear_queue_log(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            ep_set = set(eps)
            self._log = [e for e in self._log if e["entrypoint"] not in ep_set]
        else:
            self._log.clear()

    async def next_deferred_eta(self, entrypoints: list[str]) -> timedelta | None:
        """Return time until the soonest deferred job becomes eligible, or None."""
        now = utc_now()
        ep_set = set(entrypoints)
        candidates = [
            j["execute_after"]
            for j in self._jobs.values()
            if j["status"] == "queued" and j["entrypoint"] in ep_set and j["execute_after"] > now
        ]
        if candidates:
            return min(candidates) - now
        return None

    def _remove_dedupe_for_job(self, job_id: int) -> None:
        dk = self._dedupe_key_by_job.pop(job_id, None)
        if dk is not None:
            self._dedupe_index.pop(dk, None)

    async def emit_table_changed(self, operation: models.OPERATIONS) -> None:
        event = models.TableChangedEvent(
            channel=self.qbq.settings.channel,
            sent_at=utc_now(),
            type="table_changed_event",
            operation=operation,
            table=self.qbe.settings.queue_table,
        )
        await self.driver.notify(self.qbq.settings.channel, event.model_dump_json())
