"""In-memory queries satisfying all four port protocols.

Pure Python data structures — no SQL, no database driver calls.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, overload

from pydantic_core import to_json

from pgqueuer.adapters import tracing
from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.adapters.persistence import qb, query_helpers
from pgqueuer.core.helpers import merge_tracing_headers
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, JobId, ScheduleId
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.ports.tracing import TracingProtocol


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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

    # -- internal state --------------------------------------------------------

    _jobs: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict, init=False)
    _log: list[dict[str, Any]] = dataclasses.field(default_factory=list, init=False)
    _statistics: list[dict[str, Any]] = dataclasses.field(default_factory=list, init=False)
    _schedules: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict, init=False)
    _dedupe_index: dict[str, int] = dataclasses.field(default_factory=dict, init=False)
    _next_job_id: int = dataclasses.field(default=1, init=False)
    _next_log_id: int = dataclasses.field(default=1, init=False)
    _next_schedule_id: int = dataclasses.field(default=1, init=False)
    _next_stats_id: int = dataclasses.field(default=1, init=False)

    # -- SchemaManagementPort --------------------------------------------------

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

    async def has_table(self, table: str) -> bool:
        return True

    async def table_has_column(self, table: str, column: str) -> bool:
        return True

    async def table_has_index(self, table: str, index: str) -> bool:
        return True

    async def has_user_defined_enum(self, key: str, enum: str) -> bool:
        return True

    # -- enqueue ---------------------------------------------------------------

    @overload
    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
        execute_after: timedelta | None = None,
        dedupe_key: str | None = None,
        headers: dict[str, str] | None = None,
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
    ) -> list[JobId]: ...

    async def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes | None],
        priority: int | list[int] = 0,
        execute_after: timedelta | None | list[timedelta | None] = None,
        dedupe_key: str | list[str | None] | None = None,
        headers: dict[str, str] | list[dict[str, str] | None] | None = None,
    ) -> list[JobId]:
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

        # Check dedupe uniqueness upfront
        for dk in normed.dedupe_key:
            if dk is not None and dk in self._dedupe_index:
                raise errors.DuplicateJobError(normed.dedupe_key)

        now = _utc_now()
        ids: list[JobId] = []

        for i in range(len(normed.entrypoint)):
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
                "queue_manager_id": None,
                "headers": hdr,
            }

            dk = normed.dedupe_key[i]
            if dk is not None:
                self._dedupe_index[dk] = job_id

            self._jobs[job_id] = job_dict
            ids.append(JobId(job_id))

            # Write 'queued' log entry
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

        self._emit_table_changed("insert")
        return ids

    # -- dequeue ---------------------------------------------------------------

    def _count_picked_jobs(
        self,
        queue_manager_id: uuid.UUID,
        entrypoints: dict[str, EntrypointExecutionParameter],
    ) -> tuple[dict[str, int], int, set[str]]:
        """Count picked jobs per entrypoint and track which have picked jobs."""
        picked_per_ep: dict[str, int] = {}
        total_picked = 0
        ep_has_picked: set[str] = set()
        for j in self._jobs.values():
            if j["status"] == "picked" and j["queue_manager_id"] == queue_manager_id:
                ep = j["entrypoint"]
                picked_per_ep[ep] = picked_per_ep.get(ep, 0) + 1
                total_picked += 1
            if j["status"] == "picked" and j["entrypoint"] in entrypoints:
                ep_has_picked.add(j["entrypoint"])
        return picked_per_ep, total_picked, ep_has_picked

    def _collect_queued_candidates(
        self,
        now: datetime,
        entrypoints: dict[str, EntrypointExecutionParameter],
    ) -> list[dict[str, Any]]:
        """Collect queued candidates sorted by priority DESC, id ASC."""
        candidates: list[dict[str, Any]] = []
        for j in self._jobs.values():
            if (
                j["status"] != "queued"
                or j["entrypoint"] not in entrypoints
                or j["execute_after"] > now
            ):
                continue
            candidates.append(j)
        candidates.sort(key=lambda j: (-j["priority"], j["id"]))
        return candidates

    def _collect_retry_candidates(
        self,
        now: datetime,
        entrypoints: dict[str, EntrypointExecutionParameter],
    ) -> list[dict[str, Any]]:
        """Collect retry candidates sorted by heartbeat ASC, id ASC."""
        candidates: list[dict[str, Any]] = []
        for j in self._jobs.values():
            if j["status"] != "picked" or j["entrypoint"] not in entrypoints:
                continue
            params = entrypoints[j["entrypoint"]]
            if params.retry_after <= timedelta(0) or now - j["heartbeat"] < params.retry_after:
                continue
            candidates.append(j)
        candidates.sort(key=lambda j: (j["heartbeat"], j["id"]))
        return candidates

    def _select_jobs(
        self,
        batch_size: int,
        candidates: list[dict[str, Any]],
        entrypoints: dict[str, EntrypointExecutionParameter],
        picked_per_ep: dict[str, int],
        ep_has_picked: set[str],
    ) -> list[dict[str, Any]]:
        """Select jobs respecting concurrency and serialization constraints."""
        selected: list[dict[str, Any]] = []
        seen: set[int] = set()
        for j in candidates:
            if len(selected) >= batch_size:
                break
            if j["id"] in seen:
                continue
            seen.add(j["id"])

            ep = j["entrypoint"]
            params = entrypoints[ep]

            # serialized_dispatch: at most 1 picked job per entrypoint
            if params.serialized and ep in ep_has_picked:
                continue

            # concurrency_limit: check current count
            if (
                params.concurrency_limit > 0
                and picked_per_ep.get(ep, 0) >= params.concurrency_limit
            ):
                continue

            selected.append(j)
            ep_has_picked.add(ep)
            picked_per_ep[ep] = picked_per_ep.get(ep, 0) + 1

        return selected

    def _write_picked_logs(
        self,
        jobs: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        """Write 'picked' log entries for selected jobs."""
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
    ) -> list[models.Job]:
        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        if not entrypoints:
            return []

        now = _utc_now()

        picked_per_ep, total_picked, ep_has_picked = self._count_picked_jobs(
            queue_manager_id, entrypoints
        )

        # Apply global concurrency limit
        if global_concurrency_limit is not None and total_picked >= global_concurrency_limit:
            return []

        remaining = batch_size
        if global_concurrency_limit is not None:
            remaining = min(remaining, global_concurrency_limit - total_picked)

        queued_candidates = self._collect_queued_candidates(now, entrypoints)
        retry_candidates = self._collect_retry_candidates(now, entrypoints)

        selected = self._select_jobs(
            remaining,
            [*queued_candidates, *retry_candidates],
            entrypoints,
            picked_per_ep,
            ep_has_picked,
        )

        # Update matched jobs
        for j in selected:
            j["status"] = "picked"
            j["queue_manager_id"] = queue_manager_id
            j["updated"] = now
            j["heartbeat"] = now

        self._write_picked_logs(selected, now)

        # Sort result: priority DESC, id ASC
        selected.sort(key=lambda j: (-j["priority"], j["id"]))

        # Yield to the event loop.  In the PostgreSQL path every dequeue
        # suspends on real I/O (driver.fetch); without this explicit yield
        # QueueManager.fetch_jobs can loop indefinitely and starve signals,
        # timers, and other tasks.
        await asyncio.sleep(0)

        return [models.Job.model_validate(dict(j)) for j in selected]

    # -- log_jobs --------------------------------------------------------------

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
        now = _utc_now()
        for job, status, tb in job_status:
            self._jobs.pop(int(job.id), None)
            # Clean dedupe index
            self._remove_dedupe_for_job(int(job.id))
            self._log.append(
                {
                    "id": self._next_log_id,
                    "created": now,
                    "job_id": int(job.id),
                    "status": status,
                    "priority": job.priority,
                    "entrypoint": job.entrypoint,
                    "traceback": tb.model_dump_json() if tb else None,
                    "aggregated": False,
                }
            )
            self._next_log_id += 1

    # -- mark_job_as_cancelled -------------------------------------------------

    async def mark_job_as_cancelled(self, ids: list[JobId]) -> None:
        now = _utc_now()
        for jid in ids:
            job_dict = self._jobs.pop(int(jid), None)
            if job_dict is not None:
                self._remove_dedupe_for_job(int(jid))
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

    # -- clear_queue -----------------------------------------------------------

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            now = _utc_now()
            to_remove = [jid for jid, j in self._jobs.items() if j["entrypoint"] in eps]
            for jid in to_remove:
                j = self._jobs.pop(jid)
                self._remove_dedupe_for_job(jid)
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

    # -- queue_size ------------------------------------------------------------

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

    # -- queued_work -----------------------------------------------------------

    async def queued_work(self, entrypoints: list[str]) -> int:
        ep_set = set(entrypoints)
        return sum(
            1 for j in self._jobs.values() if j["status"] == "queued" and j["entrypoint"] in ep_set
        )

    # -- queue_log -------------------------------------------------------------

    async def queue_log(self) -> list[models.Log]:
        return [models.Log.model_validate(entry) for entry in self._log]

    # -- update_heartbeat ------------------------------------------------------

    async def update_heartbeat(self, job_ids: list[JobId]) -> None:
        now = _utc_now()
        unique_ids = {int(jid) for jid in job_ids}
        for jid in unique_ids:
            if jid in self._jobs:
                self._jobs[jid]["heartbeat"] = now

    # -- job_status ------------------------------------------------------------

    async def job_status(
        self,
        ids: list[JobId],
    ) -> list[tuple[JobId, models.JOB_STATUS]]:
        id_set = {int(jid) for jid in ids}
        # Scan log in reverse; keep latest per job_id
        latest: dict[int, models.JOB_STATUS] = {}
        for entry in reversed(self._log):
            jid = entry["job_id"]
            if jid in id_set and jid not in latest:
                latest[jid] = entry["status"]
        return [(JobId(jid), st) for jid, st in latest.items()]

    # -- log_statistics --------------------------------------------------------

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        # Step 1: aggregate un-aggregated log entries into _statistics
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

        # Step 2: filter and return
        result = list(self._statistics)

        if last is not None:
            cutoff = _utc_now() - last
            result = [r for r in result if r["created"] >= cutoff]

        # Sort by id DESC
        result.sort(key=lambda r: r["id"], reverse=True)

        if tail is not None:
            result = result[:tail]

        return [models.LogStatistics.model_validate(r) for r in result]

    # -- Notification methods --------------------------------------------------

    async def notify_entrypoint_rps(self, entrypoint_count: dict[str, int]) -> None:
        if entrypoint_count:
            event = models.RequestsPerSecondEvent(
                channel=self.qbq.settings.channel,
                entrypoint_count=entrypoint_count,
                sent_at=_utc_now(),
                type="requests_per_second_event",
            )
            self.driver.deliver(self.qbq.settings.channel, event.model_dump_json())

    async def notify_job_cancellation(self, ids: list[JobId]) -> None:
        event = models.CancellationEvent(
            channel=self.qbq.settings.channel,
            ids=ids,
            sent_at=_utc_now(),
            type="cancellation_event",
        )
        self.driver.deliver(self.qbq.settings.channel, event.model_dump_json())

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None:
        event = models.HealthCheckEvent(
            channel=self.qbq.settings.channel,
            sent_at=_utc_now(),
            type="health_check_event",
            id=health_check_event_id,
        )
        self.driver.deliver(self.qbq.settings.channel, event.model_dump_json())

    # -- Schedule methods ------------------------------------------------------

    async def insert_schedule(
        self,
        schedules: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> None:
        now = _utc_now()
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
        now = _utc_now()
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
        now = _utc_now()
        for sid in ids:
            s = self._schedules.get(int(sid))
            if s is not None:
                s["status"] = "queued"
                s["last_run"] = now
                s["updated"] = now

    async def update_schedule_heartbeat(self, ids: set[ScheduleId]) -> None:
        now = _utc_now()
        for sid in ids:
            s = self._schedules.get(int(sid))
            if s is not None:
                s["heartbeat"] = now
                s["updated"] = now

    async def peak_schedule(self) -> list[models.Schedule]:
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

    # -- Extra utility methods -------------------------------------------------

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

    # -- Private helpers -------------------------------------------------------

    def _remove_dedupe_for_job(self, job_id: int) -> None:
        """Remove dedupe_index entry pointing to *job_id*."""
        to_remove = [k for k, v in self._dedupe_index.items() if v == job_id]
        for k in to_remove:
            del self._dedupe_index[k]

    def _emit_table_changed(self, operation: models.OPERATIONS) -> None:
        """Construct and deliver a ``TableChangedEvent`` via the driver."""
        event = models.TableChangedEvent(
            channel=self.qbq.settings.channel,
            sent_at=_utc_now(),
            type="table_changed_event",
            operation=operation,
            table=self.qbe.settings.queue_table,
        )
        self.driver.deliver(self.qbq.settings.channel, event.model_dump_json())
