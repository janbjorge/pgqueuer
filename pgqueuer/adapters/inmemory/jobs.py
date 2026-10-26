"""In-memory job rows, the dedupe index, and dequeue selection."""

from __future__ import annotations

import bisect
import dataclasses
from datetime import datetime, timedelta
from typing import NamedTuple, assert_never

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


def job_status_rank(status: JOB_STATUS) -> int:  # noqa: PLR0911 - one return per status
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


class UniqueViolation(Exception):  # noqa: N818 - mirrors psycopg.errors.UniqueViolation
    """Unique violation of the dedupe index, SQLSTATE 23505."""

    def __init__(self, constraint_name: str) -> None:
        super().__init__(f'duplicate key value violates unique constraint "{constraint_name}"')
        self.sqlstate = UNIQUE_VIOLATION
        self.constraint_name = constraint_name


@dataclasses.dataclass(slots=True)
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
    headers: dict[str, object] | None
    # Bumped when the row leaves the ready lane, so the old entry is skipped.
    ready_token: int = 0

    def to_model(self) -> models.Job:
        return models.Job(
            id=JobId(self.id),
            priority=self.priority,
            created=self.created,
            updated=self.updated,
            heartbeat=self.heartbeat,
            execute_after=self.execute_after,
            status=self.status,
            entrypoint=self.entrypoint,
            payload=self.payload,
            attempts=self.attempts,
            queue_manager_id=self.queue_manager_id,
            slot=self.slot,
            headers=self.headers,
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


class Queued(NamedTuple):
    token: int
    row: JobRow
    order: tuple[int, int]


def queue_order(row: JobRow) -> tuple[int, int]:
    """Highest priority first, then the oldest id. Same order as dequeue."""
    return (-row.priority, row.id)


@dataclasses.dataclass
class ReadyLane:
    """Queued jobs for one entrypoint.

    A cursor walks past jobs already picked, so a drain reads the next
    batch instead of the rows taken earlier.
    """

    jobs: list[Queued] = dataclasses.field(default_factory=list)
    head: int = 0

    def add(self, row: JobRow) -> None:
        row.ready_token += 1
        # Freeze the sort key. A later priority bump must not move this entry.
        order = queue_order(row)
        entry = Queued(row.ready_token, row, order)
        if self.head >= len(self.jobs) or order >= self.jobs[-1].order:
            self.jobs.append(entry)
            return
        self.compact()
        bisect.insort(self.jobs, entry, key=lambda item: item.order)

    def take_due(self, cap: int, now: datetime) -> list[JobRow]:
        index = self.head
        limit = len(self.jobs)
        while index < limit and not self.pending(self.jobs[index]):
            index += 1
        self.head = index
        self.compact()
        index = self.head
        limit = len(self.jobs)
        chosen: list[JobRow] = []
        while index < limit and len(chosen) < cap:
            entry = self.jobs[index]
            index += 1
            if not self.pending(entry):
                continue
            if entry.row.execute_after > now:
                continue
            chosen.append(entry.row)
        # A deferred row at the cursor must not pin finished rows behind it.
        if index - self.head > len(chosen) and index - self.head > 32:
            self.jobs = [entry for entry in self.jobs[self.head :] if self.pending(entry)]
            self.head = 0
        return chosen

    def pending(self, entry: Queued) -> bool:
        return entry.token == entry.row.ready_token and entry.row.status == "queued"

    def compact(self) -> None:
        if self.head and self.head * 2 >= len(self.jobs):
            del self.jobs[: self.head]
            self.head = 0


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
    """Job rows and dequeue selection. Due work comes off a per-entrypoint lane."""

    rows: dict[int, JobRow] = dataclasses.field(default_factory=dict)
    dedupe: DedupeIndex = dataclasses.field(default_factory=DedupeIndex)
    next_id: int = 1
    ready: dict[QueueEntrypoint, ReadyLane] = dataclasses.field(default_factory=dict)
    picked: dict[int, JobRow] = dataclasses.field(default_factory=dict)
    picked_by_manager: dict[QueueManagerId, int] = dataclasses.field(default_factory=dict)
    picked_by_entrypoint: dict[QueueEntrypoint, int] = dataclasses.field(default_factory=dict)

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
            if job.status == "queued":
                self.lane(job.entrypoint).add(job)
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
        source = normed.headers[index]
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
            headers=None if source is None else dict(source),
        )
        if key is not None:
            self.dedupe.remember(job_id, key)
        self.rows[job_id] = row
        self.lane(row.entrypoint).add(row)
        return InsertOutcome(JobId(job_id), row)

    def lane(self, entrypoint: QueueEntrypoint) -> ReadyLane:
        ready = self.ready.get(entrypoint)
        if ready is None:
            ready = ReadyLane()
            self.ready[entrypoint] = ready
        return ready

    def picked_counts(
        self,
        queue_manager_id: QueueManagerId,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
    ) -> PickedCounts:
        per_entrypoint = {
            entrypoint: count
            for entrypoint, count in self.picked_by_entrypoint.items()
            if entrypoint in entrypoints and count > 0
        }
        return PickedCounts(per_entrypoint, self.picked_by_manager.get(queue_manager_id, 0))

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
            lane = self.ready.get(entrypoint)
            if lane is not None:
                chosen.extend(lane.take_due(cap, now))
        return chosen

    def stale_picks(
        self,
        entrypoints: dict[QueueEntrypoint, EntrypointExecutionParameter],
        heartbeat_timeout: timedelta,
        now: datetime,
    ) -> list[JobRow]:
        return [
            job
            for job in self.picked.values()
            if job.entrypoint in entrypoints and now - job.heartbeat >= heartbeat_timeout
        ]

    def mark_picked(
        self,
        selected: list[JobRow],
        queue_manager_id: QueueManagerId,
        now: datetime,
    ) -> None:
        for row in selected:
            if row.status == "picked":
                previous = row.queue_manager_id
                if previous is not None and previous != queue_manager_id:
                    self.picked_by_manager[previous] -= 1
                    self.picked_by_manager[queue_manager_id] = (
                        self.picked_by_manager.get(queue_manager_id, 0) + 1
                    )
            else:
                self.picked_by_entrypoint[row.entrypoint] = (
                    self.picked_by_entrypoint.get(row.entrypoint, 0) + 1
                )
                self.picked_by_manager[queue_manager_id] = (
                    self.picked_by_manager.get(queue_manager_id, 0) + 1
                )
                row.ready_token += 1
                self.picked[row.id] = row
            row.status = "picked"
            row.queue_manager_id = queue_manager_id
            row.updated = now
            row.heartbeat = now

    def release(self, row: JobRow) -> None:
        if row.status != "picked":
            return
        if row.queue_manager_id is not None:
            self.picked_by_manager[row.queue_manager_id] -= 1
        self.picked_by_entrypoint[row.entrypoint] -= 1
        self.picked.pop(row.id, None)

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
        # Gone (cleared) or owned by a newer picker: leave the existing log alone.
        if job_id not in self.rows or not self.owned_by(job):
            return False
        row = self.rows[job_id]
        self.release(row)
        if status == "failed":
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
        self.release(row)
        row.status = "queued"
        row.execute_after = now + delay
        row.attempts += 1
        row.updated = now
        row.queue_manager_id = None
        self.lane(row.entrypoint).add(row)
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
        self.lane(row.entrypoint).add(row)

    def cancel(self, ids: list[JobId]) -> list[JobRow]:
        removed: list[JobRow] = []
        for raw_id in ids:
            row = self.rows.get(int(raw_id))
            # A picked row keeps its slot and dedupe key until the worker logs.
            if row is None or row.status == "picked":
                continue
            self.rows.pop(row.id)
            self.release(row)
            row.ready_token += 1
            self.dedupe.forget(row.id)
            removed.append(row)
        return removed

    def clear(self, entrypoints: set[str] | None) -> list[JobRow]:
        if entrypoints is None:
            self.rows.clear()
            self.dedupe.clear()
            self.ready.clear()
            self.picked.clear()
            self.picked_by_manager.clear()
            self.picked_by_entrypoint.clear()
            return []
        removed = [row for row in self.rows.values() if row.entrypoint in entrypoints]
        for row in removed:
            self.release(row)
            del self.rows[row.id]
            self.dedupe.forget(row.id)
        for name in entrypoints:
            self.ready.pop(QueueEntrypoint(name), None)
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
