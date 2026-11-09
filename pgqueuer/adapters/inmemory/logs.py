"""In-memory queue log and the per-second statistics rolled up from it."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from pgqueuer.adapters.inmemory.jobs import job_status_rank
from pgqueuer.domain import models
from pgqueuer.domain.models import utc_now
from pgqueuer.domain.types import JOB_STATUS, JobId, QueueEntrypoint


@dataclasses.dataclass(slots=True)
class LogRow:
    id: int
    created: datetime
    job_id: int
    status: JOB_STATUS
    priority: int
    entrypoint: QueueEntrypoint
    traceback: str | None
    aggregated: bool

    def to_model(self) -> models.Log:
        return models.Log.model_validate(
            {
                "created": self.created,
                "job_id": self.job_id,
                "status": self.status,
                "priority": self.priority,
                "entrypoint": self.entrypoint,
                "traceback": self.traceback,
                "aggregated": self.aggregated,
            }
        )


@dataclasses.dataclass
class StatisticsRow:
    id: int
    created: datetime
    count: int
    entrypoint: QueueEntrypoint
    priority: int
    status: JOB_STATUS

    def to_model(self) -> models.LogStatistics:
        return models.LogStatistics(
            count=self.count,
            created=self.created,
            entrypoint=self.entrypoint,
            priority=self.priority,
            status=self.status,
        )


class StatisticsBucket(NamedTuple):
    entrypoint: QueueEntrypoint
    priority: int
    status: JOB_STATUS
    created: datetime


class ThroughputGroup(NamedTuple):
    entrypoint: QueueEntrypoint
    status: JOB_STATUS


# Origin of the throughput buckets, as in build_throughput_timeseries_query.
BUCKET_ORIGIN = datetime(2001, 1, 1, tzinfo=UTC)


class TimeseriesGroup(NamedTuple):
    bucket: datetime
    entrypoint: QueueEntrypoint
    status: JOB_STATUS


def percentile_cont(values: list[float], fraction: float) -> float:
    """Linear-interpolated percentile matching PostgreSQL's ``percentile_cont``."""
    if not values:
        raise ValueError("percentile of an empty sequence")
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


@dataclasses.dataclass
class LogBook:
    """Queue log and the per-second statistics rolled up from it."""

    rows: list[LogRow] = dataclasses.field(default_factory=list)
    statistics: list[StatisticsRow] = dataclasses.field(default_factory=list)
    next_id: int = 1
    next_statistics_id: int = 1

    def append(
        self,
        *,
        created: datetime,
        job_id: int,
        status: JOB_STATUS,
        priority: int,
        entrypoint: QueueEntrypoint,
        traceback: str | None,
    ) -> None:
        self.rows.append(
            LogRow(
                id=self.next_id,
                created=created,
                job_id=job_id,
                status=status,
                priority=priority,
                entrypoint=entrypoint,
                traceback=traceback,
                aggregated=False,
            )
        )
        self.next_id += 1

    def to_models(self) -> list[models.Log]:
        return [row.to_model() for row in self.rows]

    def aggregate(self) -> None:
        counts: dict[StatisticsBucket, int] = {}
        for entry in self.rows:
            if entry.aggregated:
                continue
            bucket = StatisticsBucket(
                entry.entrypoint,
                entry.priority,
                entry.status,
                entry.created.replace(microsecond=0),
            )
            counts[bucket] = counts.get(bucket, 0) + 1
            entry.aggregated = True

        existing = {
            StatisticsBucket(row.entrypoint, row.priority, row.status, row.created): row
            for row in self.statistics
        }
        for bucket, count in counts.items():
            row = existing.get(bucket)
            if row is not None:
                row.count += count
                continue
            self.statistics.append(
                StatisticsRow(
                    id=self.next_statistics_id,
                    created=bucket.created,
                    count=count,
                    entrypoint=bucket.entrypoint,
                    priority=bucket.priority,
                    status=bucket.status,
                )
            )
            self.next_statistics_id += 1

    def log_statistics(
        self,
        limit: int | None,
        last: timedelta | None,
    ) -> list[models.LogStatistics]:
        self.aggregate()
        rows = list(self.statistics)
        if last is not None:
            cutoff = utc_now() - last
            rows = [row for row in rows if row.created > cutoff]
        rows.sort(key=lambda row: row.id, reverse=True)
        if limit is not None:
            rows = rows[:limit]
        return [row.to_model() for row in rows]

    def job_status(self, ids: list[JobId]) -> list[tuple[JobId, JOB_STATUS]]:
        wanted = {int(job_id) for job_id in ids}
        latest: dict[int, JOB_STATUS] = {}
        for entry in reversed(self.rows):
            if entry.job_id in wanted and entry.job_id not in latest:
                latest[entry.job_id] = entry.status
        # Match DISTINCT ON (job_id): newest status, rows ordered by job id.
        return [(JobId(job_id), latest[job_id]) for job_id in sorted(latest)]

    def clear_entries(self, entrypoints: set[str] | None) -> None:
        if entrypoints is None:
            self.rows.clear()
            return
        self.rows = [row for row in self.rows if row.entrypoint not in entrypoints]

    def clear_statistics(self, entrypoints: set[str] | None) -> None:
        if entrypoints is None:
            self.statistics.clear()
            return
        self.statistics = [row for row in self.statistics if row.entrypoint not in entrypoints]

    def wipe(self) -> None:
        self.rows.clear()
        self.statistics.clear()

    def unaggregated_count(self) -> int:
        return sum(1 for row in self.rows if not row.aggregated)

    def exception_logs(self, limit: int) -> list[models.Log]:
        entries = [entry for entry in self.rows if entry.status == "exception"]
        entries.sort(key=lambda entry: (entry.created, entry.id), reverse=True)
        return [entry.to_model() for entry in entries[:limit]]

    def history(self, job_id: int, limit: int) -> list[models.Log]:
        entries = [entry for entry in self.rows if entry.job_id == job_id]
        entries.sort(key=lambda entry: (entry.created, entry.id))
        return [entry.to_model() for entry in entries[:limit]]

    def duration_percentiles(self, last: timedelta) -> list[models.JobDurationStats]:
        cutoff = utc_now() - last
        ordered = sorted(
            (entry for entry in self.rows if entry.created > cutoff),
            key=lambda entry: (entry.job_id, entry.created, entry.id),
        )
        terminal: set[JOB_STATUS] = {"successful", "exception", "canceled", "failed"}
        durations: dict[QueueEntrypoint, list[float]] = {}
        previous: LogRow | None = None
        for entry in ordered:
            if (
                previous is not None
                and previous.job_id == entry.job_id
                and previous.status == "picked"
                and entry.status in terminal
            ):
                durations.setdefault(entry.entrypoint, []).append(
                    (entry.created - previous.created).total_seconds()
                )
            previous = entry
        stats = [
            models.JobDurationStats(
                entrypoint=entrypoint,
                completed=len(values),
                p50_seconds=percentile_cont(values, 0.5),
                p95_seconds=percentile_cont(values, 0.95),
                p99_seconds=percentile_cont(values, 0.99),
                max_seconds=max(values),
            )
            for entrypoint, values in durations.items()
        ]
        stats.sort(key=lambda row: row.p95_seconds, reverse=True)
        return stats

    def throughput_summary(self, last: timedelta | None) -> list[models.ThroughputStats]:
        self.aggregate()
        cutoff = utc_now() - last if last is not None else None
        totals: dict[ThroughputGroup, int] = {}
        for row in self.statistics:
            if cutoff is not None and row.created <= cutoff:
                continue
            group = ThroughputGroup(row.entrypoint, row.status)
            totals[group] = totals.get(group, 0) + row.count
        ordered = sorted(
            totals,
            key=lambda group: (group.entrypoint, job_status_rank(group.status)),
        )
        return [
            models.ThroughputStats(
                entrypoint=group.entrypoint,
                status=group.status,
                total_count=totals[group],
            )
            for group in ordered
        ]

    def throughput_timeseries(
        self,
        last: timedelta,
        bucket: timedelta = timedelta(minutes=1),
    ) -> list[models.ThroughputBucket]:
        self.aggregate()
        cutoff = utc_now() - last
        buckets: dict[TimeseriesGroup, int] = {}
        for row in self.statistics:
            if row.created <= cutoff:
                continue
            group = TimeseriesGroup(
                # Same as date_bin(bucket, created, BUCKET_ORIGIN) in the Postgres query.
                BUCKET_ORIGIN + (row.created - BUCKET_ORIGIN) // bucket * bucket,
                row.entrypoint,
                row.status,
            )
            buckets[group] = buckets.get(group, 0) + row.count
        return [
            models.ThroughputBucket(
                bucket=group.bucket,
                entrypoint=group.entrypoint,
                status=group.status,
                count=buckets[group],
            )
            for group in sorted(buckets)
        ]
