"""In-memory repository adapter for PgQueuer.

Satisfies all four port protocols (QueueRepositoryPort, ScheduleRepositoryPort,
NotificationPort, SchemaManagementPort) via structural subtyping — the same
pattern used by the ``Queries`` class.

Concurrency model: all repository methods are synchronous in body (no ``await``
on I/O), so they execute atomically from the event loop's perspective.  No
``asyncio.Lock`` is needed.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, overload

from pydantic_core import to_json

from pgqueuer.adapters.in_memory.driver import InMemoryDriver
from pgqueuer.adapters.persistence import qb, query_helpers
from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.core.helpers import utc_now
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, JobId


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclasses.dataclass
class InMemoryRepository:
    """In-memory implementation of all PgQueuer persistence ports."""

    _driver: InMemoryDriver

    qbe: qb.QueryBuilderEnvironment = dataclasses.field(
        default_factory=qb.QueryBuilderEnvironment,
    )

    # Internal storage — plain dicts for fast mutation; Pydantic validation
    # only at return boundaries (matching the Queries pattern).
    _jobs: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict)
    _logs: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    _statistics: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    _schedules: dict[int, dict[str, Any]] = dataclasses.field(default_factory=dict)
    _dedupe_index: dict[str, int] = dataclasses.field(default_factory=dict)

    _next_job_id: int = dataclasses.field(default=1)
    _next_schedule_id: int = dataclasses.field(default=1)
    _next_log_id: int = dataclasses.field(default=1)

    # ------------------------------------------------------------------
    # QueueRepositoryPort
    # ------------------------------------------------------------------

    async def dequeue(  # noqa: C901
        self,
        batch_size: int,
        entrypoints: dict[str, EntrypointExecutionParameter],
        queue_manager_id: uuid.UUID,
        global_concurrency_limit: int | None,
    ) -> list[models.Job]:
        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        now = _now()

        # Count jobs already picked by this queue manager (for global limit).
        qm_count = sum(
            1
            for j in self._jobs.values()
            if j["queue_manager_id"] == queue_manager_id and j["entrypoint"] in entrypoints
        )

        # Count jobs picked by this queue manager per entrypoint (for concurrency limit).
        qm_per_ep: dict[str, int] = {}
        for j in self._jobs.values():
            if j["queue_manager_id"] == queue_manager_id and j["entrypoint"] in entrypoints:
                qm_per_ep[j["entrypoint"]] = qm_per_ep.get(j["entrypoint"], 0) + 1

        selected_ids: list[int] = []
        # Track which entrypoints already have a job selected (for serialized).
        selected_eps: set[str] = set()

        # Phase 1: queued jobs ready for execution.
        candidates = []
        for jid, j in self._jobs.items():
            if (
                j["entrypoint"] in entrypoints
                and j["status"] == "queued"
                and j["execute_after"] < now
            ):
                candidates.append((j["priority"], jid, j))

        # Sort by priority DESC, id ASC.
        candidates.sort(key=lambda x: (-x[0], x[1]))

        for _pri, jid, j in candidates:
            if len(selected_ids) >= batch_size:
                break
            if global_concurrency_limit is not None and qm_count >= global_concurrency_limit:
                break

            ep = j["entrypoint"]
            params = entrypoints[ep]

            # Serialized check: no picked jobs for this entrypoint
            # (including jobs selected in this batch).
            if params.serialized:
                if ep in selected_eps:
                    continue
                has_picked = any(
                    oj["entrypoint"] == ep and oj["status"] == "picked"
                    for oj in self._jobs.values()
                )
                if has_picked:
                    continue

            # Concurrency limit check.
            if params.concurrency_limit > 0:
                current = qm_per_ep.get(ep, 0)
                if current >= params.concurrency_limit:
                    continue

            selected_ids.append(jid)
            selected_eps.add(ep)
            qm_count += 1
            qm_per_ep[ep] = qm_per_ep.get(ep, 0) + 1

        # Phase 2: stale picked jobs (retry).
        retry_candidates = []
        for jid, j in self._jobs.items():
            if jid in selected_ids:
                continue
            if j["entrypoint"] not in entrypoints:
                continue
            ep = j["entrypoint"]
            params = entrypoints[ep]
            if (
                j["status"] == "picked"
                and j["execute_after"] < now
                and params.retry_after > timedelta(0)
                and j["heartbeat"] < now - params.retry_after
            ):
                retry_candidates.append((j["heartbeat"], jid, j))

        retry_candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

        for _hb, jid, j in retry_candidates:
            if len(selected_ids) >= batch_size:
                break
            if global_concurrency_limit is not None and qm_count >= global_concurrency_limit:
                break

            ep = j["entrypoint"]
            params = entrypoints[ep]

            if params.serialized:
                has_picked = any(
                    oj["entrypoint"] == ep and oj["status"] == "picked"
                    for oj in self._jobs.values()
                )
                if has_picked:
                    continue

            if params.concurrency_limit > 0:
                current = qm_per_ep.get(ep, 0)
                if current >= params.concurrency_limit:
                    continue

            selected_ids.append(jid)
            qm_count += 1
            qm_per_ep[ep] = qm_per_ep.get(ep, 0) + 1

        # Update selected jobs to 'picked' and log the pick.
        result = []
        for jid in selected_ids:
            j = self._jobs[jid]
            j["status"] = "picked"
            j["updated"] = now
            j["heartbeat"] = now
            j["queue_manager_id"] = queue_manager_id

            self._log_entry(jid, j)
            result.append(j)

        # Sort result: priority DESC, id ASC.
        result.sort(key=lambda j: (-j["priority"], j["id"]))

        return [models.Job.model_validate(dict(j)) for j in result]

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
        normed = query_helpers.normalize_enqueue_params(
            entrypoint, payload, priority, execute_after, dedupe_key, headers
        )

        now = _now()
        ids: list[models.JobId] = []

        # Check dedupe keys first (all-or-nothing).
        for dk in normed.dedupe_key:
            if dk is not None and dk in self._dedupe_index:
                raise errors.DuplicateJobError(normed.dedupe_key)

        for i in range(len(normed.entrypoint)):
            jid = self._next_job_id
            self._next_job_id += 1

            ea = now + normed.execute_after[i]
            h = normed.headers[i]
            h_json = to_json(h).decode() if h is not None else None

            job_dict: dict[str, Any] = {
                "id": jid,
                "priority": normed.priority[i],
                "created": now,
                "updated": now,
                "heartbeat": now,
                "execute_after": ea,
                "status": "queued",
                "entrypoint": normed.entrypoint[i],
                "payload": normed.payload[i],
                "queue_manager_id": None,
                "headers": h_json,
                "dedupe_key": normed.dedupe_key[i],
            }
            self._jobs[jid] = job_dict

            dk = normed.dedupe_key[i]
            if dk is not None:
                self._dedupe_index[dk] = jid

            # Log the enqueue.
            self._log_entry(jid, job_dict, status_override="queued")

            ids.append(models.JobId(jid))

        # Fire table_changed_event notification.
        self._fire_table_changed("insert")

        return ids

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
        for job, status, tb in job_status:
            jid = int(job.id)
            job_dict = self._jobs.pop(jid, None)
            if job_dict is None:
                continue

            # Clean dedupe index.
            dk = job_dict.get("dedupe_key")
            if dk is not None and self._dedupe_index.get(dk) == jid:
                del self._dedupe_index[dk]

            # Insert into logs.
            log_id = self._next_log_id
            self._next_log_id += 1
            self._logs.append(
                {
                    "id": log_id,
                    "created": _now(),
                    "job_id": jid,
                    "status": status,
                    "priority": job_dict["priority"],
                    "entrypoint": job_dict["entrypoint"],
                    "traceback": tb.model_dump_json() if tb else None,
                    "aggregated": False,
                }
            )

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None:
        for jid_typed in ids:
            jid = int(jid_typed)
            job_dict = self._jobs.pop(jid, None)
            if job_dict is None:
                continue

            dk = job_dict.get("dedupe_key")
            if dk is not None and self._dedupe_index.get(dk) == jid:
                del self._dedupe_index[dk]

            log_id = self._next_log_id
            self._next_log_id += 1
            self._logs.append(
                {
                    "id": log_id,
                    "created": _now(),
                    "job_id": jid,
                    "status": "canceled",
                    "priority": job_dict["priority"],
                    "entrypoint": job_dict["entrypoint"],
                    "traceback": None,
                    "aggregated": False,
                }
            )

        await self.notify_job_cancellation(ids)

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None:
        now = _now()
        seen: set[int] = set()
        for jid_typed in job_ids:
            jid = int(jid_typed)
            if jid in seen:
                continue
            seen.add(jid)
            if jid in self._jobs:
                self._jobs[jid]["heartbeat"] = now

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            to_remove = [jid for jid, j in self._jobs.items() if j["entrypoint"] in eps]
        else:
            to_remove = list(self._jobs.keys())

        for jid in to_remove:
            job_dict = self._jobs.pop(jid)
            dk = job_dict.get("dedupe_key")
            if dk is not None and self._dedupe_index.get(dk) == jid:
                del self._dedupe_index[dk]

    async def queue_size(self) -> list[models.QueueStatistics]:
        groups: dict[tuple[str, int, str], int] = {}
        for j in self._jobs.values():
            key = (j["entrypoint"], j["priority"], j["status"])
            groups[key] = groups.get(key, 0) + 1

        result = [
            models.QueueStatistics(
                count=count,
                entrypoint=ep,
                priority=pri,
                status=st,
            )
            for (ep, pri, st), count in groups.items()
        ]
        result.sort(key=lambda s: (s.count, s.entrypoint, s.priority, s.status))
        return result

    async def queued_work(self, entrypoints: list[str]) -> int:
        return sum(
            1
            for j in self._jobs.values()
            if j["entrypoint"] in entrypoints and j["status"] == "queued"
        )

    async def queue_log(self) -> list[models.Log]:
        return [models.Log.model_validate(dict(entry)) for entry in self._logs]

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        # Aggregate non-aggregated logs into statistics (mirrors SQL behaviour).
        now = _now()
        agg: dict[tuple[str, int, str, str], int] = {}
        for entry in self._logs:
            if not entry["aggregated"]:
                created_trunc = entry["created"].replace(microsecond=0)
                key = (
                    entry["entrypoint"],
                    entry["priority"],
                    entry["status"],
                    created_trunc.isoformat(),
                )
                agg[key] = agg.get(key, 0) + 1
                entry["aggregated"] = True

        for (ep, pri, st, created_iso), count in agg.items():
            created_dt = datetime.fromisoformat(created_iso)
            # Try to merge with existing statistics entry.
            merged = False
            for stat in self._statistics:
                if (
                    stat["entrypoint"] == ep
                    and stat["priority"] == pri
                    and stat["status"] == st
                    and stat["created"] == created_dt
                ):
                    stat["count"] += count
                    merged = True
                    break
            if not merged:
                self._statistics.append(
                    {
                        "id": len(self._statistics) + 1,
                        "count": count,
                        "created": created_dt,
                        "entrypoint": ep,
                        "priority": pri,
                        "status": st,
                    }
                )

        # Filter and return.
        entries = list(self._statistics)
        if last is not None:
            cutoff = now - last
            entries = [e for e in entries if e["created"] > cutoff]

        # Sort by id DESC (most recent first).
        entries.sort(key=lambda e: e["id"], reverse=True)

        if tail is not None:
            entries = entries[:tail]

        return [
            models.LogStatistics(
                count=e["count"],
                created=e["created"],
                entrypoint=e["entrypoint"],
                priority=e["priority"],
                status=e["status"],
            )
            for e in entries
        ]

    async def job_status(
        self,
        ids: list[models.JobId],
    ) -> list[tuple[models.JobId, models.JOB_STATUS]]:
        id_set = {int(i) for i in ids}
        # Find the most recent log entry per job_id.
        latest: dict[int, tuple[int, str, models.JOB_STATUS]] = {}
        for entry in self._logs:
            jid = entry["job_id"]
            if jid not in id_set:
                continue
            log_id = entry["id"]
            existing = latest.get(jid)
            if existing is None or log_id > existing[0]:
                latest[jid] = (log_id, entry["created"].isoformat(), entry["status"])

        return [(JobId(jid), status) for jid, (_, _, status) in sorted(latest.items())]

    # ------------------------------------------------------------------
    # ScheduleRepositoryPort
    # ------------------------------------------------------------------

    async def insert_schedule(
        self,
        schedules: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> None:
        now = _now()
        for cee, delay in schedules.items():
            # Upsert: skip if (expression, entrypoint) already exists.
            exists = any(
                s["expression"] == cee.expression and s["entrypoint"] == cee.entrypoint
                for s in self._schedules.values()
            )
            if exists:
                continue

            sid = self._next_schedule_id
            self._next_schedule_id += 1
            next_run = (now + delay).replace(microsecond=0)
            self._schedules[sid] = {
                "id": sid,
                "expression": cee.expression,
                "entrypoint": cee.entrypoint,
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
        now = _now()
        ep_set = {(cee.expression, cee.entrypoint): delay for cee, delay in entrypoints.items()}

        picked: list[dict[str, Any]] = []
        for s in self._schedules.values():
            key = (s["expression"], s["entrypoint"])
            if key not in ep_set:
                continue
            if (s["status"] == "queued" and s["next_run"] <= now) or (
                s["status"] == "picked" and now - s["heartbeat"] > timedelta(seconds=30)
            ):
                picked.append(s)

        for s in picked:
            key = (s["expression"], s["entrypoint"])
            delay = ep_set[key]
            s["status"] = "picked"
            s["updated"] = now
            s["next_run"] = (now + delay).replace(microsecond=0)

        return [models.Schedule.model_validate(dict(s)) for s in picked]

    async def set_schedule_queued(self, ids: set[models.ScheduleId]) -> None:
        now = _now()
        for sid_typed in ids:
            sid = int(sid_typed)
            if sid in self._schedules:
                self._schedules[sid]["status"] = "queued"
                self._schedules[sid]["last_run"] = now
                self._schedules[sid]["updated"] = now

    async def update_schedule_heartbeat(self, ids: set[models.ScheduleId]) -> None:
        now = _now()
        for sid_typed in ids:
            sid = int(sid_typed)
            if sid in self._schedules:
                self._schedules[sid]["heartbeat"] = now
                self._schedules[sid]["updated"] = now

    async def peak_schedule(self) -> list[models.Schedule]:
        entries = sorted(
            self._schedules.values(),
            key=lambda s: (s["last_run"] or datetime.min.replace(tzinfo=timezone.utc)),
        )
        return [models.Schedule.model_validate(dict(s)) for s in entries]

    async def delete_schedule(
        self,
        ids: set[models.ScheduleId],
        entrypoints: set[CronEntrypoint],
    ) -> None:
        id_set = {int(i) for i in ids}
        to_remove = [
            sid
            for sid, s in self._schedules.items()
            if sid in id_set or s["entrypoint"] in entrypoints
        ]
        for sid in to_remove:
            del self._schedules[sid]

    async def clear_schedule(self) -> None:
        self._schedules.clear()

    # ------------------------------------------------------------------
    # Utility methods (not in port protocols, but needed by benchmarks/tools)
    # ------------------------------------------------------------------

    async def clear_statistics_log(self, entrypoint: str | list[str] | None = None) -> None:
        """Clear the statistics (log) table."""
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            self._statistics = [s for s in self._statistics if s["entrypoint"] not in eps]
        else:
            self._statistics.clear()

    async def clear_queue_log(self, entrypoint: str | list[str] | None = None) -> None:
        """Clear the queue log table."""
        if entrypoint:
            eps = [entrypoint] if isinstance(entrypoint, str) else entrypoint
            self._logs = [l for l in self._logs if l["entrypoint"] not in eps]
        else:
            self._logs.clear()

    # ------------------------------------------------------------------
    # NotificationPort
    # ------------------------------------------------------------------

    async def notify_entrypoint_rps(self, entrypoint_count: dict[str, int]) -> None:
        if not entrypoint_count:
            return
        event = models.RequestsPerSecondEvent(
            channel=self.qbe.settings.channel,
            entrypoint_count=entrypoint_count,
            sent_at=utc_now(),
            type="requests_per_second_event",
        )
        self._push_notification(event.model_dump_json())

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None:
        event = models.CancellationEvent(
            channel=self.qbe.settings.channel,
            ids=ids,
            sent_at=utc_now(),
            type="cancellation_event",
        )
        self._push_notification(event.model_dump_json())

    async def notify_health_check(self, health_check_event_id: uuid.UUID) -> None:
        event = models.HealthCheckEvent(
            channel=self.qbe.settings.channel,
            sent_at=utc_now(),
            type="health_check_event",
            id=health_check_event_id,
        )
        self._push_notification(event.model_dump_json())

    # ------------------------------------------------------------------
    # SchemaManagementPort
    # ------------------------------------------------------------------

    async def install(self) -> None:
        pass

    async def uninstall(self) -> None:
        self._jobs.clear()
        self._logs.clear()
        self._statistics.clear()
        self._schedules.clear()
        self._dedupe_index.clear()

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log_entry(
        self,
        jid: int,
        job_dict: dict[str, Any],
        *,
        status_override: str | None = None,
    ) -> None:
        log_id = self._next_log_id
        self._next_log_id += 1
        self._logs.append(
            {
                "id": log_id,
                "created": _now(),
                "job_id": jid,
                "status": status_override or job_dict["status"],
                "priority": job_dict["priority"],
                "entrypoint": job_dict["entrypoint"],
                "traceback": None,
                "aggregated": False,
            }
        )

    def _fire_table_changed(self, operation: str) -> None:
        event = models.TableChangedEvent(
            channel=self.qbe.settings.channel,
            operation=operation,
            sent_at=utc_now(),
            table=self.qbe.settings.queue_table,
            type="table_changed_event",
        )
        self._push_notification(event.model_dump_json())

    def _push_notification(self, payload: str) -> None:
        channel = self.qbe.settings.channel
        for cb in self._driver._listeners.get(channel, []):
            cb(payload)
