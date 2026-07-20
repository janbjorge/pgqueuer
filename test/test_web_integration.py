from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.web.routes import create_web_router
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain import models
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries


@pytest_asyncio.fixture
async def web_app(apgdriver: AsyncpgDriver) -> FastAPI:
    app = FastAPI()
    app.state.pgq_queries = Queries(apgdriver)
    broadcaster = Broadcaster(
        driver=apgdriver,
        channel=qb.DBSettings().channel,
        debounce=timedelta(milliseconds=10),
    )
    await broadcaster.start()
    app.state.pgq_broadcaster = broadcaster
    app.include_router(create_web_router())
    return app


@pytest_asyncio.fixture
async def client(web_app: FastAPI) -> AsyncGenerator[httpx.AsyncClient, None]:
    transport = httpx.ASGITransport(app=web_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


async def dequeue_all(queries: Queries, entrypoint: str) -> list[models.Job]:
    return await queries.dequeue(
        batch_size=100,
        entrypoints={entrypoint: EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


@pytest.mark.parametrize(
    "path",
    ["/", "/entrypoints", "/jobs", "/failures", "/workers", "/schedules", "/system", "/healthz"],
)
async def test_pages_render_against_real_db(client: httpx.AsyncClient, path: str) -> None:
    assert (await client.get(path)).status_code == 200


async def test_enqueued_job_appears_and_detail_renders(
    client: httpx.AsyncClient, apgdriver: AsyncpgDriver
) -> None:
    q = Queries(apgdriver)
    (job_id,) = await q.enqueue("integration_ep", b'{"answer": 42}')

    listing = await client.get("/jobs")
    assert "integration_ep" in listing.text

    detail = await client.get(f"/jobs/{job_id}")
    assert detail.status_code == 200
    assert "integration_ep" in detail.text
    assert "42" in detail.text


async def test_cancel_removes_job_and_logs(
    client: httpx.AsyncClient, apgdriver: AsyncpgDriver
) -> None:
    q = Queries(apgdriver)
    (job_id,) = await q.enqueue("cancel_ep", None)

    response = await client.post(f"/jobs/{job_id}/cancel")
    assert response.status_code == 204

    assert await q.queue_job_by_id(job_id) is None
    history = await q.job_log_history(job_id)
    assert [h.status for h in history] == ["queued", "canceled"]


async def test_requeue_failed_job_via_endpoint(
    client: httpx.AsyncClient, apgdriver: AsyncpgDriver
) -> None:
    q = Queries(apgdriver)
    await q.enqueue("requeue_ep", None)
    (job,) = await dequeue_all(q, "requeue_ep")
    await q.log_jobs([(job, "failed", None)])

    failures = await client.get("/failures")
    assert "requeue_ep" in failures.text

    response = await client.post("/jobs/requeue", data={"ids": str(job.id)})
    assert response.status_code == 204

    refreshed = await q.queue_job_by_id(job.id)
    assert refreshed is not None
    assert refreshed.status == "queued"
    assert refreshed.attempts == 0


async def test_exception_traceback_shown_on_failures_page(
    client: httpx.AsyncClient, apgdriver: AsyncpgDriver
) -> None:
    q = Queries(apgdriver)
    await q.enqueue("exc_ep", None)
    (job,) = await dequeue_all(q, "exc_ep")
    record = models.TracebackRecord.from_exception(RuntimeError("integration boom"), job.id)
    await q.log_jobs([(job, "exception", record)])

    response = await client.get("/failures")
    assert "RuntimeError: integration boom" in response.text


async def test_trigger_notify_reaches_sse_stream(apgdriver: AsyncpgDriver) -> None:
    """An enqueue fires the existing table trigger; the broadcaster turns it into a frame."""
    q = Queries(apgdriver)
    broadcaster = Broadcaster(
        driver=apgdriver,
        channel=qb.DBSettings().channel,
        debounce=timedelta(milliseconds=10),
    )
    await broadcaster.start()

    stream = broadcaster.stream()
    assert (await anext(stream)).startswith("retry:")

    await q.enqueue("sse_ep", None)
    frame = await asyncio.wait_for(anext(stream), timeout=5)
    assert "queue-change" in frame
    await stream.aclose()


async def test_job_duration_percentiles_exact(apgdriver: AsyncpgDriver) -> None:
    """Synthetic picked→successful transitions produce exact percentile_cont values."""
    q = Queries(apgdriver)
    log_table = qb.DBSettings().queue_table_log
    for job_id, seconds in enumerate([1, 2, 3, 4], start=1):
        await apgdriver.execute(
            f"""INSERT INTO {log_table} (created, job_id, status, priority, entrypoint)
            VALUES
              (NOW() - $2::interval, $1, 'picked', 0, 'timed_ep'),
              (NOW(), $1, 'successful', 0, 'timed_ep')
            """,
            job_id,
            timedelta(seconds=seconds),
        )

    (stats,) = await q.job_duration_percentiles(timedelta(hours=1))
    assert stats.entrypoint == "timed_ep"
    assert stats.completed == 4
    assert stats.p50_seconds == pytest.approx(2.5)
    assert stats.p95_seconds == pytest.approx(3.85)
    assert stats.p99_seconds == pytest.approx(3.97)
    assert stats.max_seconds == pytest.approx(4.0)


async def test_job_duration_percentiles_window_excludes_old_rows(
    apgdriver: AsyncpgDriver,
) -> None:
    q = Queries(apgdriver)
    log_table = qb.DBSettings().queue_table_log
    await apgdriver.execute(
        f"""INSERT INTO {log_table} (created, job_id, status, priority, entrypoint)
        VALUES
          (NOW() - interval '3 hours', 1, 'picked', 0, 'old_ep'),
          (NOW() - interval '2 hours', 1, 'successful', 0, 'old_ep')
        """,
    )
    assert await q.job_duration_percentiles(timedelta(hours=1)) == []
    (stats,) = await q.job_duration_percentiles(timedelta(hours=4))
    assert stats.completed == 1
    assert stats.p50_seconds == pytest.approx(3600.0, rel=0.01)


async def test_browse_queue_filters_against_db(apgdriver: AsyncpgDriver) -> None:
    q = Queries(apgdriver)
    await q.enqueue(["ep_a", "ep_b"], [None, None], [0, 0])
    await dequeue_all(q, "ep_b")

    assert {j.entrypoint for j in await q.browse_queue()} == {"ep_a", "ep_b"}
    assert [j.entrypoint for j in await q.browse_queue(statuses=["queued"])] == ["ep_a"]
    assert [j.entrypoint for j in await q.browse_queue(entrypoints=["ep_b"])] == ["ep_b"]
    assert await q.browse_queue(statuses=["failed"]) == []


async def test_insights_queries_shapes_against_db(apgdriver: AsyncpgDriver) -> None:
    q = Queries(apgdriver)
    await q.enqueue("shapes_ep", None)
    (job,) = await dequeue_all(q, "shapes_ep")

    workers = await q.active_workers()
    assert len(workers) == 1
    assert workers[0].active_jobs == 1
    assert workers[0].entrypoints == ["shapes_ep"]

    assert await q.stale_jobs(timedelta(minutes=5)) == []
    stale = await q.stale_jobs(timedelta(seconds=-1))
    assert [s.id for s in stale] == [job.id]

    await q.log_jobs([(job, "successful", None)])
    summary = await q.throughput_summary(timedelta(hours=1))
    assert any(t.status == "successful" and t.entrypoint == "shapes_ep" for t in summary)
    series = await q.throughput_timeseries(timedelta(hours=1))
    assert any(t.entrypoint == "shapes_ep" for t in series)

    tables = await q.schema_info()
    assert len(tables) == 4
    assert await q.unaggregated_log_count() == 0
