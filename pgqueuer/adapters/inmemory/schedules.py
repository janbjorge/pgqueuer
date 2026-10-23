"""In-memory cron schedules."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta

from pgqueuer.domain import models
from pgqueuer.domain.models import utc_now
from pgqueuer.domain.types import JOB_STATUS, CronEntrypoint, CronExpression, ScheduleId

# Picked schedules with a heartbeat older than this are claimed again.
RECLAIM_AFTER_SECONDS = 30


@dataclasses.dataclass
class ScheduleRow:
    id: int
    expression: CronExpression
    entrypoint: CronEntrypoint
    heartbeat: datetime
    created: datetime
    updated: datetime
    next_run: datetime
    last_run: datetime | None
    status: JOB_STATUS

    def to_model(self) -> models.Schedule:
        return models.Schedule(
            id=ScheduleId(self.id),
            expression=self.expression,
            heartbeat=self.heartbeat,
            created=self.created,
            updated=self.updated,
            next_run=self.next_run,
            last_run=self.last_run,
            status=self.status,
            entrypoint=self.entrypoint,
        )


@dataclasses.dataclass
class ScheduleTable:
    """Cron rows. Fetch claims a due schedule or a picked row with a stale heartbeat."""

    rows: dict[int, ScheduleRow] = dataclasses.field(default_factory=dict)
    next_id: int = 1

    def insert(self, schedules: dict[models.CronExpressionEntrypoint, timedelta]) -> None:
        now = utc_now()
        for key, delay in schedules.items():
            if any(
                row.expression == key.expression and row.entrypoint == key.entrypoint
                for row in self.rows.values()
            ):
                continue
            schedule_id = self.next_id
            self.next_id += 1
            self.rows[schedule_id] = ScheduleRow(
                id=schedule_id,
                expression=key.expression,
                entrypoint=key.entrypoint,
                heartbeat=now,
                created=now,
                updated=now,
                next_run=(now + delay).replace(microsecond=0),
                last_run=None,
                status="queued",
            )

    def fetch(
        self,
        entrypoints: dict[models.CronExpressionEntrypoint, timedelta],
    ) -> list[models.Schedule]:
        now = utc_now()
        delays = {(key.expression, key.entrypoint): delay for key, delay in entrypoints.items()}
        selected: list[ScheduleRow] = []
        for row in self.rows.values():
            delay = delays.get((row.expression, row.entrypoint))
            if delay is None:
                continue
            due = row.status == "queued" and row.next_run <= now
            stale = (
                row.status == "picked"
                and (now - row.heartbeat).total_seconds() > RECLAIM_AFTER_SECONDS
            )
            if not due and not stale:
                continue
            if row.status == "queued":
                row.status = "picked"
            row.heartbeat = now
            row.updated = now
            # Match date_trunc('seconds', now + delay).
            row.next_run = (now + delay).replace(microsecond=0)
            selected.append(row)
        return [row.to_model() for row in selected]

    def set_queued(self, ids: set[ScheduleId]) -> None:
        now = utc_now()
        for schedule_id in ids:
            row = self.rows.get(int(schedule_id))
            if row is None:
                continue
            row.status = "queued"
            row.last_run = now
            row.updated = now

    def touch(self, ids: set[ScheduleId]) -> None:
        now = utc_now()
        for schedule_id in ids:
            row = self.rows.get(int(schedule_id))
            if row is None:
                continue
            row.heartbeat = now
            row.updated = now

    def peek(self) -> list[models.Schedule]:
        # PostgreSQL `ORDER BY last_run ASC` puts nulls last.
        ordered = sorted(self.rows.values(), key=lambda row: (row.last_run is None, row.last_run))
        return [row.to_model() for row in ordered]

    def delete(self, ids: set[ScheduleId], entrypoints: set[CronEntrypoint]) -> None:
        id_ints = {int(schedule_id) for schedule_id in ids}
        removed = [
            schedule_id
            for schedule_id, row in self.rows.items()
            if schedule_id in id_ints or row.entrypoint in entrypoints
        ]
        for schedule_id in removed:
            del self.rows[schedule_id]

    def clear(self) -> None:
        self.rows.clear()
