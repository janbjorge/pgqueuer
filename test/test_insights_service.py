from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.adapters.inmemory.queries import percentile_cont
from pgqueuer.core.insights import (
    DEFAULT_WINDOW,
    MAX_WINDOW,
    InsightsService,
    QueueManagementService,
    clamp_window,
    sparkline_buckets,
)
from pgqueuer.domain import models
from pgqueuer.ports.repository import EntrypointExecutionParameter


async def dequeue_all(queries: InMemoryQueries, entrypoint: str) -> list[models.Job]:
    return await queries.dequeue(
        batch_size=100,
        entrypoints={entrypoint: EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


class TestPercentileCont:
    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError):
            percentile_cont([], 0.5)

    def test_single_value(self) -> None:
        assert percentile_cont([7.0], 0.5) == 7.0
        assert percentile_cont([7.0], 0.99) == 7.0

    def test_median_interpolates(self) -> None:
        assert percentile_cont([1.0, 2.0, 3.0, 4.0], 0.5) == 2.5

    def test_matches_postgres_definition(self) -> None:
        # percentile_cont(0.95) over [1..4]: rank = 3 * 0.95 = 2.85 -> 3 + 0.85
        assert percentile_cont([1.0, 2.0, 3.0, 4.0], 0.95) == pytest.approx(3.85)


class TestClampWindow:
    def test_none_is_default(self) -> None:
        assert clamp_window(None) == DEFAULT_WINDOW

    def test_capped_at_max(self) -> None:
        assert clamp_window(timedelta(days=30)) == MAX_WINDOW

    def test_floor_is_one_second(self) -> None:
        assert clamp_window(timedelta(0)) == timedelta(seconds=1)

    def test_in_range_passes_through(self) -> None:
        assert clamp_window(timedelta(hours=6)) == timedelta(hours=6)


class TestInsightsService:
    async def test_overview_counts_statuses(self, queries: InMemoryQueries) -> None:
        service = InsightsService(queries)
        await queries.enqueue(["ep_a", "ep_a", "ep_b"], [None] * 3, [0] * 3)
        await dequeue_all(queries, "ep_b")

        snapshot = await service.overview()
        assert snapshot.queued_total == 2
        assert snapshot.picked_total == 1
        assert snapshot.failed_total == 0
        assert snapshot.active_workers == 1
        assert snapshot.oldest_queued_age_seconds is not None

    async def test_overview_empty_queue(self, queries: InMemoryQueries) -> None:
        snapshot = await InsightsService(queries).overview()
        assert snapshot.queued_total == 0
        assert snapshot.oldest_queued_age_seconds is None

    async def test_queue_age_groups_by_entrypoint(self, queries: InMemoryQueries) -> None:
        await queries.enqueue(["ep_a", "ep_a", "ep_b"], [None] * 3, [0] * 3)
        ages = await InsightsService(queries).queue_age()
        by_ep = {a.entrypoint: a for a in ages}
        assert by_ep["ep_a"].queued_count == 2
        assert by_ep["ep_b"].queued_count == 1
        assert all(a.oldest_age_seconds >= 0 for a in ages)

    async def test_job_durations_from_transitions(self, queries: InMemoryQueries) -> None:
        await queries.enqueue("ep", b"x")
        (job,) = await dequeue_all(queries, "ep")
        await queries.log_jobs([(job, "successful", None)])

        durations = await InsightsService(queries).job_durations()
        assert len(durations) == 1
        assert durations[0].entrypoint == "ep"
        assert durations[0].completed == 1
        assert durations[0].p50_seconds >= 0
        assert durations[0].max_seconds >= durations[0].p50_seconds

    async def test_entrypoint_stats_failure_rate(self, queries: InMemoryQueries) -> None:
        await queries.enqueue(["ep", "ep"], [None, None], [0, 0])
        jobs = await dequeue_all(queries, "ep")
        await queries.log_jobs([(jobs[0], "successful", None), (jobs[1], "exception", None)])

        (stat,) = await InsightsService(queries).entrypoint_stats()
        assert stat.entrypoint == "ep"
        assert stat.completed == 2
        assert stat.failure_rate == pytest.approx(0.5)
        assert sum(stat.sparkline) == 2

    async def test_stale_jobs_threshold(self, queries: InMemoryQueries) -> None:
        await queries.enqueue("ep", None)
        await dequeue_all(queries, "ep")
        service = InsightsService(queries)
        assert await service.stale_jobs(timedelta(minutes=5)) == []
        stale = await service.stale_jobs(timedelta(seconds=-1))
        assert len(stale) == 1
        assert stale[0].seconds_since_heartbeat >= 0

    async def test_job_and_history(self, queries: InMemoryQueries) -> None:
        (job_id,) = await queries.enqueue("ep", b"payload")
        service = InsightsService(queries)
        job = await service.job(job_id)
        assert job is not None and job.payload == b"payload"
        history = await service.job_history(job_id)
        assert [h.status for h in history] == ["queued"]
        assert await service.job(models.JobId(999_999)) is None

    async def test_browse_queue_filters(self, queries: InMemoryQueries) -> None:
        await queries.enqueue(["ep_a", "ep_b"], [None, None], [0, 0])
        service = InsightsService(queries)
        assert len(await service.browse_queue()) == 2
        only_a = await service.browse_queue(entrypoints=["ep_a"])
        assert [j.entrypoint for j in only_a] == ["ep_a"]
        assert await service.browse_queue(statuses=["picked"]) == []

    async def test_exception_logs_and_failed_jobs(self, queries: InMemoryQueries) -> None:
        await queries.enqueue(["ep", "ep"], [None, None], [0, 0])
        jobs = await dequeue_all(queries, "ep")
        record = models.TracebackRecord.from_exception(ValueError("boom"), jobs[0].id)
        await queries.log_jobs([(jobs[0], "exception", record), (jobs[1], "failed", None)])

        service = InsightsService(queries)
        exceptions = await service.exception_logs()
        assert len(exceptions) == 1
        assert exceptions[0].traceback is not None
        assert exceptions[0].traceback.exception_type == "ValueError"

        held = await service.failed_jobs()
        assert [j.id for j in held] == [jobs[1].id]

    async def test_unaggregated_count_drops_after_stats(self, queries: InMemoryQueries) -> None:
        await queries.enqueue("ep", None)
        service = InsightsService(queries)
        assert await service.unaggregated_log_count() == 1
        await service.throughput()
        assert await service.unaggregated_log_count() == 0

    async def test_schema_info_lists_tables(self, queries: InMemoryQueries) -> None:
        tables = await InsightsService(queries).schema_info()
        assert len(tables) == 4


class TestSparklineBuckets:
    def test_empty_series(self) -> None:
        assert sparkline_buckets([], "ep", timedelta(hours=1)) == [0] * 30

    def test_counts_are_conserved(self) -> None:
        now = models.utc_now()
        series = [
            models.ThroughputBucket(bucket=now, entrypoint="ep", status="successful", count=3),
            models.ThroughputBucket(
                bucket=now - timedelta(minutes=30),
                entrypoint="ep",
                status="exception",
                count=2,
            ),
            models.ThroughputBucket(bucket=now, entrypoint="other", status="successful", count=9),
        ]
        buckets = sparkline_buckets(series, "ep", timedelta(hours=1))
        assert sum(buckets) == 5


class TestQueueManagementService:
    async def test_requeue_flips_failed_only(self, queries: InMemoryQueries) -> None:
        await queries.enqueue("ep", None)
        (job,) = await dequeue_all(queries, "ep")
        await queries.log_jobs([(job, "failed", None)])

        await QueueManagementService(queries).requeue([job.id])
        refreshed = await queries.queue_job_by_id(job.id)
        assert refreshed is not None
        assert refreshed.status == "queued"
        assert refreshed.attempts == 0

    async def test_cancel_removes_and_logs(self, queries: InMemoryQueries) -> None:
        (job_id,) = await queries.enqueue("ep", None)
        await QueueManagementService(queries).cancel([job_id])
        assert await queries.queue_job_by_id(job_id) is None
        history = await queries.job_log_history(job_id)
        assert [h.status for h in history] == ["queued", "canceled"]
