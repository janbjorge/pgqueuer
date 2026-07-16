"""Presentation-agnostic queue insights and management services.

These services are the single reusable API for queue introspection and job
actions. Frontends (web dashboard, MCP server, CLI, Prometheus exporters)
consume them instead of talking to SQL or ``Queries`` directly.
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from pgqueuer.domain import models
from pgqueuer.ports.repository import InsightsRepositoryPort, QueueRepositoryPort

DEFAULT_WINDOW = timedelta(hours=1)
MAX_WINDOW = timedelta(hours=24)
DEFAULT_STALE_THRESHOLD = timedelta(minutes=5)


def clamp_window(last: timedelta | None) -> timedelta:
    """Bound a look-back window to (0, MAX_WINDOW]; None means DEFAULT_WINDOW."""
    if last is None:
        return DEFAULT_WINDOW
    return max(min(last, MAX_WINDOW), timedelta(seconds=1))


@dataclasses.dataclass
class InsightsService:
    """Read-only queue insights over the repository port.

    Usage example::

        service = InsightsService(Queries(driver))
        snapshot = await service.overview()
    """

    repository: InsightsRepositoryPort

    async def queue_size(self) -> list[models.QueueStatistics]:
        return await self.repository.queue_size()

    async def queue_age(self) -> list[models.QueueAgeStats]:
        return await self.repository.queue_age()

    async def job_durations(self, last: timedelta | None = None) -> list[models.JobDurationStats]:
        return await self.repository.job_duration_percentiles(clamp_window(last))

    async def throughput(self, last: timedelta | None = None) -> list[models.ThroughputStats]:
        return await self.repository.throughput_summary(clamp_window(last))

    async def throughput_timeseries(
        self,
        last: timedelta | None = None,
    ) -> list[models.ThroughputBucket]:
        return await self.repository.throughput_timeseries(clamp_window(last))

    async def active_workers(self) -> list[models.ActiveWorker]:
        return await self.repository.active_workers()

    async def stale_jobs(
        self,
        threshold: timedelta | None = None,
        limit: int = 100,
    ) -> list[models.StaleJob]:
        return await self.repository.stale_jobs(threshold or DEFAULT_STALE_THRESHOLD, limit)

    async def exception_logs(self, limit: int = 100) -> list[models.Log]:
        return await self.repository.exception_logs(limit)

    async def failed_jobs(self, limit: int = 100) -> list[models.Job]:
        return await self.repository.list_failed_jobs(limit)

    async def browse_queue(
        self,
        limit: int = 50,
        offset: int = 0,
        statuses: list[models.JOB_STATUS] | None = None,
        entrypoints: list[str] | None = None,
    ) -> list[models.Job]:
        return await self.repository.browse_queue(limit, offset, statuses, entrypoints)

    async def job(self, id: models.JobId) -> models.Job | None:
        return await self.repository.queue_job_by_id(id)

    async def job_history(self, id: models.JobId) -> list[models.Log]:
        return await self.repository.job_log_history(id)

    async def schedules(self) -> list[models.Schedule]:
        return await self.repository.peek_schedule()

    async def schema_info(self) -> list[models.TableInfo]:
        return await self.repository.schema_info()

    async def unaggregated_log_count(self) -> int:
        return await self.repository.unaggregated_log_count()

    async def overview(self, last: timedelta | None = None) -> models.OverviewSnapshot:
        """Headline dashboard numbers; ``exceptions_recent`` counts within *last*."""
        sizes = await self.repository.queue_size()
        ages = await self.repository.queue_age()
        workers = await self.repository.active_workers()
        throughput = await self.repository.throughput_summary(clamp_window(last))
        return models.OverviewSnapshot(
            queued_total=sum(s.count for s in sizes if s.status == "queued"),
            picked_total=sum(s.count for s in sizes if s.status == "picked"),
            failed_total=sum(s.count for s in sizes if s.status == "failed"),
            active_workers=len(workers),
            oldest_queued_age_seconds=max(
                (a.oldest_age_seconds for a in ages),
                default=None,
            ),
            exceptions_recent=sum(t.total_count for t in throughput if t.status == "exception"),
        )

    async def entrypoint_stats(
        self,
        last: timedelta | None = None,
    ) -> list[models.EntrypointStat]:
        """One combined health row per entrypoint: depth, latency, durations, failures."""
        window = clamp_window(last)
        sizes = await self.repository.queue_size()
        ages = {a.entrypoint: a for a in await self.repository.queue_age()}
        durations = {
            d.entrypoint: d for d in await self.repository.job_duration_percentiles(window)
        }
        throughput = await self.repository.throughput_summary(window)
        timeseries = await self.repository.throughput_timeseries(window)

        entrypoints = sorted(
            {s.entrypoint for s in sizes}
            | set(ages)
            | set(durations)
            | {t.entrypoint for t in throughput}
        )

        terminal = {"successful", "exception", "canceled", "failed"}
        stats = []
        for ep in entrypoints:
            processed = sum(
                t.total_count for t in throughput if t.entrypoint == ep and t.status in terminal
            )
            exceptions = sum(
                t.total_count
                for t in throughput
                if t.entrypoint == ep and t.status in ("exception", "failed")
            )
            duration = durations.get(ep)
            age = ages.get(ep)
            stats.append(
                models.EntrypointStat(
                    entrypoint=ep,
                    queued=sum(
                        s.count for s in sizes if s.entrypoint == ep and s.status == "queued"
                    ),
                    picked=sum(
                        s.count for s in sizes if s.entrypoint == ep and s.status == "picked"
                    ),
                    oldest_age_seconds=age.oldest_age_seconds if age else None,
                    p50_seconds=duration.p50_seconds if duration else None,
                    p95_seconds=duration.p95_seconds if duration else None,
                    p99_seconds=duration.p99_seconds if duration else None,
                    completed=duration.completed if duration else 0,
                    failure_rate=(exceptions / processed) if processed else None,
                    sparkline=sparkline_buckets(timeseries, ep, window),
                )
            )
        return stats


def sparkline_buckets(
    timeseries: list[models.ThroughputBucket],
    entrypoint: str,
    window: timedelta,
    slots: int = 30,
) -> list[int]:
    """Terminal-status counts folded into *slots* equal time slices of *window*."""
    terminal = {"successful", "exception", "canceled", "failed"}
    relevant = [t for t in timeseries if t.entrypoint == entrypoint and t.status in terminal]
    counts = [0] * slots
    if not relevant:
        return counts
    end = max(t.bucket for t in relevant)
    start = end - window
    span = (end - start).total_seconds() or 1.0
    for t in relevant:
        offset = (t.bucket - start).total_seconds() / span
        slot = min(int(offset * slots), slots - 1)
        counts[slot] += t.count
    return counts


@dataclasses.dataclass
class QueueManagementService:
    """Job actions over the repository port: requeue held-failed jobs, cancel jobs.

    Usage example::

        service = QueueManagementService(Queries(driver))
        await service.requeue([JobId(42)])
    """

    repository: QueueRepositoryPort

    async def requeue(self, ids: list[models.JobId]) -> None:
        """Move held ``'failed'`` jobs back to queued; other statuses are unaffected."""
        await self.repository.requeue_jobs(ids)

    async def cancel(self, ids: list[models.JobId]) -> None:
        """Cancel jobs: logs ``'canceled'`` and notifies in-flight workers."""
        await self.repository.mark_job_as_cancelled(ids)
