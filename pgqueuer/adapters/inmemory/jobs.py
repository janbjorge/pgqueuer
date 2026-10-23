"""In-memory job rows, the dedupe index, and dequeue selection."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta
from typing import NamedTuple, assert_never

from pydantic_core import to_json

from pgqueuer.adapters.persistence.query_helpers import (
    NormedEnqueueParam,
    reject_repeated_dedupe_keys,
)
from pgqueuer.adapters.persistence.sqlstate import UNIQUE_VIOLATION
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import (
    JOB_STATUS,
    JobId,
    OnConflict,
    QueueEntrypoint,
    QueueManagerId,
    Slot,
    SortOrder,
)
from pgqueuer.ports.repository import EntrypointExecutionParameter


def job_status_rank(status: JOB_STATUS) -> int:
    """Declaration order of ``pgqueuer_status``, which is how PostgreSQL sorts it."""
    if status == "queued":
        return 0
    if status == "picked":
        return 1
    if status == "successful":
        return 2
    if status == "canceled":
        return 3
    if status == "deleted":
        return 4
    if status == "exception":
        return 5
    if status == "failed":
        return 6
    assert_never(status)


class UniqueViolation(Exception):
    """Unique violation of the dedupe index, SQLSTATE 23505."""

    def __init__(self, constraint_name: str) -> None:
        super().__init__(f'duplicate key value violates unique constraint "{constraint_name}"')
        self.sqlstate = UNIQUE_VIOLATION
        self.constraint_name = constraint_name


@dataclasses.dataclass
class JobRow:
    id: int
    priority: int
    created: datetime
    updated: datetime
    heartbeat: datetime
    execute_after: datetime
    status: JOB_STATUS
    entrypoint: QueueEntrypoint
    payload: bytes | None
    attempts: int
    queue_manager_id: QueueManagerId | None
    slot: Slot | None
    headers: str

    def to_model(self) -> models.Job:
        return models.Job.model_validate(
            {
                "id": self.id,
                "priority": self.priority,
                "created": self.created,
                "updated": self.updated,
                "heartbeat": self.heartbeat,
                "execute_after": self.execute_after,
                "status": self.status,
                "entrypoint": self.entrypoint,
                "payload": self.payload,
                "attempts": self.attempts,
                "queue_manager_id": self.queue_manager_id,
                "slot": self.slot,
                "headers": self.headers,
            }
        )


class QueueSizeGroup(NamedTuple):
    entrypoint: QueueEntrypoint
    priority: int
    status: JOB_STATUS


class PickedCounts(NamedTuple):
    per_entrypoint: dict[QueueEntrypoint, int]
    total: int


class InsertOutcome(NamedTuple):
    job_id: JobId | None
    row: JobRow | None


class EnqueueResult(NamedTuple):
    ids: list[JobId | None]
    created: list[JobRow]


@dataclasses.dataclass
class DedupeIndex:
    """Partial unique index on dedupe key.

    Failed jobs keep their key and drop out of the index until requeue.
    """

    index: dict[str, int] = dataclasses.field(default_factory=dict)
    key_by_job: dict[int, str] = dataclasses.field(default_factory=dict)

    def holder(self, key: str) -> int | None:
        return self.index.get(key)

    def key_for(self, job_id: int) -> str | None:
        return self.key_by_job.get(job_id)

    def remember(self, job_id: int, key: str) -> None:
        self.index[key] = job_id
        self.key_by_job[job_id] = key

    def forget(self, job_id: int) -> None:
        key = self.key_by_job.pop(job_id, None)
        if key is not None:
            self.index.pop(key, None)

    def unindex(self, job_id: int) -> None:
        key = self.key_by_job.get(job_id)
        if key is not None and self.index.get(key) == job_id:
            self.index.pop(key, None)

    def restore(self, job_id: int) -> None:
        key = self.key_by_job.get(job_id)
        if key is None:
            return
        holder = self.index.get(key)
        if holder is None or holder == job_id:
            self.index[key] = job_id

    def clear(self) -> None:
        self.index.clear()
        self.key_by_job.clear()


@dataclasses.dataclass
class JobTable:
    """Job rows and dequeue selection. Due work is a sort over the rows."""

    rows: dict[int, JobRow] = dataclasses.field(default_factory=dict)
    dedupe: DedupeIndex = dataclasses.field(default_factory=DedupeIndex)
    next_id: int = 1

    def insert(
        self,
        normed: NormedEnqueueParam,
        on_conflict: OnConflict,
        now: datetime,
    ) -> EnqueueResult:
        self.reject_conflicts(normed, on_conflict)
        ids: list[JobId | None] = []
        created: list[JobRow] = []
        for index in range(len(normed.entrypoint)):
            outcome = self.insert_one(normed, index, on_conflict, now)
            ids.append(outcome.job_id)
            if outcome.row is not None:
                created.append(outcome.row)
        return EnqueueResult(ids, created)

    def reject_conflicts(self, normed: NormedEnqueueParam, on_conflict: OnConflict) -> None:
        if on_conflict == "raise":
            seen: set[str] = set()
            for key in normed.dedupe_key:
                if key is None:
                    continue
                if self.dedupe.holder(key) is not None or key in seen:
                    # nextval is not undone when the insert rolls back.
                    self.next_id += len(normed.entrypoint)
                    raise errors.DuplicateJobError(normed.dedupe_key)
                seen.add(key)
            return
        if on_conflict == "skip":
            return
        if on_conflict == "update_priority":
            reject_repeated_dedupe_keys(normed.dedupe_key)
            return
        assert_never(on_conflict)

    def insert_one(
        self,
        normed: NormedEnqueueParam,
        index: int,
        on_conflict: OnConflict,
        now: datetime,
    ) -> InsertOutcome:
        key = normed.dedupe_key[index]
        holder = None if key is None else self.dedupe.holder(key)
        if holder is not None:
            return self.resolve_conflict(normed, index, on_conflict, holder)
        return self.append_job(normed, index, now, key)

    def resolve_conflict(
        self,
        normed: NormedEnqueueParam,
        index: int,
        on_conflict: OnConflict,
        holder: int,
    ) -> InsertOutcome:
        # A skipped or updated row still consumes a serial id.
        self.next_id += 1
        if on_conflict != "update_priority":
            return InsertOutcome(None, None)
        job = self.rows[holder]
        if normed.priority[index] > job.priority:
            job.priority = normed.priority[index]
        return InsertOutcome(JobId(job.id), None)

    def append_job(
        self,
        normed: NormedEnqueueParam,
        index: int,
        now: datetime,
        key: str | None,
    ) -> InsertOutcome:
        job_id = self.next_id
        self.next_id += 1
        row = JobRow(
            id=job_id,
            priority=normed.priority[index],
            created=now,
            updated=now,
            heartbeat=now,
            execute_after=now + normed.execute_after[index],
            status="queued",
            entrypoint=QueueEntrypoint(normed.entrypoint[index]),
            payload=normed.payload[index],
            attempts=0,
            queue_manager_id=None,
            slot=None,
            headers=to_json(normed.headers[index]).decode(),
        )
        if key is not None:
            self.dedupe.remember(job_id, key)
        self.rows[job_id] = row
        return InsertOutcome(JobId(job_id), row)

    def picked_counts(
        self,
        queue_manager_id: QueueManagerId,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
    ) -> PickedCounts:
        per_entrypoint: dict[QueueEntrypoint, int] = {}
        total = 0
        for job in self.rows.values():
            if job.status != "picked":
                continue
            if job.queue_manager_id == queue_manager_id:
                total += 1
            if job.entrypoint in entrypoints:
                per_entrypoint[job.entrypoint] = per_entrypoint.get(job.entrypoint, 0) + 1
        return PickedCounts(per_entrypoint, total)

    def take(
        self,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
        picked_per_entrypoint: dict[QueueEntrypoint, int],
        remaining: int,
        queue_manager_id: QueueManagerId,
        heartbeat_timeout: timedelta,
        now: datetime,
    ) -> list[JobRow]:
        fresh = self.due_jobs(entrypoints, picked_per_entrypoint, remaining, now)
        stale = self.stale_picks(entrypoints, heartbeat_timeout, now)
        # Stale picks transfer an existing claim, so they skip the per-entrypoint cap.
        merged = sorted([*fresh, *stale], key=lambda job: (-job.priority, job.id))
        selected = merged[:remaining]
        self.mark_picked(selected, queue_manager_id, now)
        return selected

    def due_jobs(
        self,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
        picked_per_entrypoint: dict[QueueEntrypoint, int],
        remaining: int,
        now: datetime,
    ) -> list[JobRow]:
        chosen: list[JobRow] = []
        for entrypoint, params in entrypoints.items():
            cap = fresh_cap(params, picked_per_entrypoint.get(entrypoint, 0), remaining)
            if cap <= 0:
                continue
            due = [
                job
                for job in self.rows.values()
                if job.status == "queued"
                and job.entrypoint == entrypoint
                and job.execute_after <= now
            ]
            due.sort(key=lambda job: (-job.priority, job.id))
            chosen.extend(due[:cap])
        return chosen

    def stale_picks(
        self,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
        heartbeat_timeout: timedelta,
        now: datetime,
    ) -> list[JobRow]:
        return [
            job
            for job in self.rows.values()
            if job.status == "picked"
            and job.entrypoint in entrypoints
            and now - job.heartbeat >= heartbeat_timeout
        ]

    def mark_picked(
        self,
        selected: list[JobRow],
        queue_manager_id: QueueManagerId,
        now: datetime,
    ) -> None:
        for row in selected:
            row.status = "picked"
            row.queue_manager_id = queue_manager_id
            row.updated = now
            row.heartbeat = now

    def owned_by(self, job: models.Job) -> bool:
        """False only when the row is held by a different queue manager."""
        row = self.rows.get(int(job.id))
        return (
            job.queue_manager_id is None
            or row is None
            or row.queue_manager_id == job.queue_manager_id
        )

    def finish(self, job: models.Job, status: JOB_STATUS, now: datetime) -> bool:
        job_id = int(job.id)
        # A job cancelled while it ran already has its terminal log row.
        if job_id not in self.rows or not self.owned_by(job):
            return False
        if status == "failed":
            row = self.rows[job_id]
            row.status = "failed"
            row.updated = now
            row.queue_manager_id = None
            # Failed rows leave the unique index and keep the key.
            self.dedupe.unindex(job_id)
        else:
            self.rows.pop(job_id)
            self.dedupe.forget(job_id)
        return True

    def retry(self, job: models.Job, delay: timedelta, now: datetime) -> JobRow | None:
        row = self.rows.get(int(job.id))
        if row is None or not self.owned_by(job):
            return None
        row.status = "queued"
        row.execute_after = now + delay
        row.attempts += 1
        row.updated = now
        row.queue_manager_id = None
        return row

    def claim_requeue(self, ids: list[JobId], constraint: str) -> list[JobRow]:
        claimed: set[str] = set()
        pending: list[JobRow] = []
        seen: set[int] = set()
        for raw_id in ids:
            job_id = int(raw_id)
            if job_id in seen:
                continue
            seen.add(job_id)
            row = self.rows.get(job_id)
            if row is None or row.status != "failed":
                continue
            self.reserve_requeue_key(row, claimed, constraint)
            pending.append(row)
        return pending

    def reserve_requeue_key(self, row: JobRow, claimed: set[str], constraint: str) -> None:
        key = self.dedupe.key_for(row.id)
        if key is None:
            return
        holder = self.dedupe.holder(key)
        if (holder is not None and holder != row.id) or key in claimed:
            # One conflict aborts the call. Earlier rows stay failed.
            raise UniqueViolation(constraint)
        claimed.add(key)

    def apply_requeue(self, row: JobRow, now: datetime) -> None:
        row.status = "queued"
        row.execute_after = now
        row.updated = now
        row.attempts = 0
        row.queue_manager_id = None
        self.dedupe.restore(row.id)

    def cancel(self, ids: list[JobId]) -> list[JobRow]:
        removed: list[JobRow] = []
        for raw_id in ids:
            row = self.rows.pop(int(raw_id), None)
            if row is None:
                continue
            self.dedupe.forget(row.id)
            removed.append(row)
        return removed

    def clear(self, entrypoints: set[str] | None) -> list[JobRow]:
        if entrypoints is None:
            self.rows.clear()
            self.dedupe.clear()
            return []
        removed = [row for row in self.rows.values() if row.entrypoint in entrypoints]
        for row in removed:
            del self.rows[row.id]
            self.dedupe.forget(row.id)
        return removed

    def touch(self, ids: list[JobId], now: datetime) -> None:
        for job_id in {int(raw_id) for raw_id in ids}:
            row = self.rows.get(job_id)
            if row is not None:
                row.heartbeat = now

    def has_queued(self, entrypoints: list[QueueEntrypoint]) -> bool:
        wanted = set(entrypoints)
        return any(
            job.status == "queued" and job.entrypoint in wanted for job in self.rows.values()
        )

    def has_due(self, entrypoints: list[QueueEntrypoint], now: datetime) -> bool:
        wanted = set(entrypoints)
        return any(
            job.status == "queued" and job.entrypoint in wanted and job.execute_after <= now
            for job in self.rows.values()
        )

    def next_deferred_eta(
        self,
        entrypoints: list[QueueEntrypoint],
        now: datetime,
    ) -> timedelta | None:
        wanted = set(entrypoints)
        candidates = [
            job.execute_after
            for job in self.rows.values()
            if job.status == "queued" and job.entrypoint in wanted and job.execute_after > now
        ]
        if not candidates:
            return None
        return min(candidates) - now

    def queue_size(self) -> list[models.QueueStatistics]:
        counts: dict[QueueSizeGroup, int] = {}
        for job in self.rows.values():
            group = QueueSizeGroup(job.entrypoint, job.priority, job.status)
            counts[group] = counts.get(group, 0) + 1
        ordered = sorted(
            counts,
            key=lambda group: (
                counts[group],
                group.entrypoint,
                group.priority,
                job_status_rank(group.status),
            ),
        )
        return [
            models.QueueStatistics(
                count=counts[group],
                entrypoint=group.entrypoint,
                priority=group.priority,
                status=group.status,
            )
            for group in ordered
        ]

    def list_failed(self, limit: int, order: SortOrder) -> list[models.Job]:
        failed = [job for job in self.rows.values() if job.status == "failed"]
        if order == "ASC":
            reverse = False
        elif order == "DESC":
            reverse = True
        else:
            assert_never(order)
        failed.sort(key=lambda job: job.created, reverse=reverse)
        return [job.to_model() for job in failed[:limit]]

    def browse(
        self,
        limit: int,
        offset: int,
        statuses: list[JOB_STATUS] | None,
        entrypoints: list[str] | None,
    ) -> list[models.Job]:
        rows = [
            job
            for job in self.rows.values()
            if (statuses is None or job.status in statuses)
            and (entrypoints is None or job.entrypoint in entrypoints)
        ]
        rows.sort(key=lambda job: (-job.priority, job.id))
        return [job.to_model() for job in rows[offset : offset + limit]]

    def queue_age(self, now: datetime) -> list[models.QueueAgeStats]:
        grouped: dict[QueueEntrypoint, list[datetime]] = {}
        for job in self.rows.values():
            if job.status == "queued":
                grouped.setdefault(job.entrypoint, []).append(job.created)
        stats = [
            models.QueueAgeStats(
                entrypoint=entrypoint,
                queued_count=len(created),
                oldest_created=min(created),
                oldest_age_seconds=(now - min(created)).total_seconds(),
                avg_age_seconds=(
                    sum((now - moment).total_seconds() for moment in created) / len(created)
                ),
            )
            for entrypoint, created in grouped.items()
        ]
        stats.sort(key=lambda row: row.entrypoint)
        return stats

    def active_workers(self) -> list[models.ActiveWorker]:
        grouped: dict[QueueManagerId, list[JobRow]] = {}
        for job in self.rows.values():
            if job.status == "picked" and job.queue_manager_id is not None:
                grouped.setdefault(job.queue_manager_id, []).append(job)
        workers = [
            models.ActiveWorker(
                queue_manager_id=queue_manager_id,
                active_jobs=len(jobs),
                oldest_heartbeat=min(job.heartbeat for job in jobs),
                newest_heartbeat=max(job.heartbeat for job in jobs),
                entrypoints=sorted({job.entrypoint for job in jobs}),
            )
            for queue_manager_id, jobs in grouped.items()
        ]
        workers.sort(key=lambda worker: str(worker.queue_manager_id))
        return workers

    def stale_jobs(self, threshold: timedelta, limit: int, now: datetime) -> list[models.StaleJob]:
        stale = [
            models.StaleJob(
                id=JobId(job.id),
                priority=job.priority,
                queue_manager_id=job.queue_manager_id,
                created=job.created,
                updated=job.updated,
                heartbeat=job.heartbeat,
                execute_after=job.execute_after,
                status=job.status,
                entrypoint=job.entrypoint,
                seconds_since_heartbeat=(now - job.heartbeat).total_seconds(),
            )
            for job in self.rows.values()
            if job.status == "picked" and job.heartbeat < now - threshold
        ]
        stale.sort(key=lambda job: job.heartbeat)
        return stale[:limit]


def fresh_cap(
    params: EntrypointExecutionParameter,
    already_picked: int,
    remaining: int,
) -> int:
    if params.concurrency_limit > 0:
        return min(remaining, params.concurrency_limit - already_picked)
    return remaining
