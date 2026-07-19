from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
from fastapi import Depends, FastAPI

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.web.auth import PASSWORD_ENV, USER_ENV, create_basic_auth_dependency
from pgqueuer.adapters.web.payload import render_payload
from pgqueuer.adapters.web.routes import (
    PAGE_SIZE,
    create_web_router,
    fmt_age,
    fmt_dt,
    fmt_duration,
    fmt_rate,
    sparkline_svg,
)
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.domain import models
from pgqueuer.ports.repository import EntrypointExecutionParameter


def build_app(queries: InMemoryQueries) -> FastAPI:
    app = FastAPI()
    app.state.pgq_queries = queries
    app.state.pgq_broadcaster = Broadcaster(
        driver=queries.driver,
        channel=queries.qbq.settings.channel,
        debounce=timedelta(milliseconds=10),
    )
    app.include_router(create_web_router())
    return app


@pytest_asyncio.fixture
async def client(queries: InMemoryQueries) -> AsyncGenerator[httpx.AsyncClient, None]:
    transport = httpx.ASGITransport(app=build_app(queries))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


async def dequeue_all(queries: InMemoryQueries, entrypoint: str) -> list[models.Job]:
    return await queries.dequeue(
        batch_size=100,
        entrypoints={entrypoint: EntrypointExecutionParameter(concurrency_limit=0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )


class TestPages:
    @pytest.mark.parametrize(
        "path",
        ["/", "/entrypoints", "/jobs", "/failures", "/workers", "/schedules", "/system"],
    )
    async def test_pages_render(self, client: httpx.AsyncClient, path: str) -> None:
        response = await client.get(path)
        assert response.status_code == 200
        assert "<html" in response.text

    async def test_healthz(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    async def test_static_files_served(self, client: httpx.AsyncClient) -> None:
        for filename in ("htmx.min.js", "idiomorph-ext.min.js", "sse.js", "style.css"):
            response = await client.get(f"/static/{filename}")
            assert response.status_code == 200

    async def test_live_regions_morph_and_throttle(self, client: httpx.AsyncClient) -> None:
        """Live dashboards must morph the DOM and throttle SSE refreshes to avoid flicker."""
        for path in ("/", "/failures", "/workers"):
            page = (await client.get(path)).text
            assert 'hx-swap="morph:innerHTML"' in page
            assert "sse:queue-change throttle:2s" in page
        # Jobs browser morphs but does not live-refresh (it would yank the reader).
        jobs = (await client.get("/jobs")).text
        assert 'hx-swap="morph:innerHTML"' in jobs
        assert "sse:queue-change" not in jobs
        assert 'hx-swap="morph:innerHTML"' in (await client.get("/entrypoints")).text

    async def test_jobs_filter_auto_submits_without_button(self, client: httpx.AsyncClient) -> None:
        """The jobs filter applies on change/typing; there is no Filter button."""
        page = (await client.get("/jobs")).text
        assert 'hx-trigger="change, keyup changed delay:400ms' in page
        assert ">Filter</button>" not in page

    async def test_live_rows_have_stable_ids(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        """Rows carry stable ids so idiomorph pins them across refreshes."""
        await queries.enqueue("ep", None)
        assert 'id="job-' in (await client.get("/jobs")).text

    async def test_theme_control_present(self, client: httpx.AsyncClient) -> None:
        page = (await client.get("/")).text
        assert 'onchange="pgqSetTheme(this.value)"' in page
        assert "data-theme" in page

    async def test_static_rejects_unknown(self, client: httpx.AsyncClient) -> None:
        assert (await client.get("/static/evil.js")).status_code == 404

    async def test_partials_are_fragments(self, client: httpx.AsyncClient) -> None:
        for path in (
            "/partials/overview",
            "/partials/entrypoints",
            "/partials/jobs",
            "/partials/failures",
            "/partials/workers",
        ):
            response = await client.get(path)
            assert response.status_code == 200
            assert "<html" not in response.text

    async def test_jobs_lists_enqueued(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await queries.enqueue("my_entrypoint", b"data")
        response = await client.get("/jobs")
        assert "my_entrypoint" in response.text

    async def test_jobs_status_filter(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await queries.enqueue("queued_ep", None)
        response = await client.get("/jobs", params={"status": "picked"})
        assert "queued_ep" not in response.text

    async def test_job_detail_renders_payload(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        (job_id,) = await queries.enqueue("ep", b'{"key": "value"}')
        response = await client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        assert "&#34;key&#34;: &#34;value&#34;" in response.text

    async def test_job_detail_unknown_404(self, client: httpx.AsyncClient) -> None:
        assert (await client.get("/jobs/999999")).status_code == 404

    async def test_failures_shows_held_and_exceptions(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await queries.enqueue(["held_ep", "exc_ep"], [None, None], [0, 0])
        held_job = (await dequeue_all(queries, "held_ep"))[0]
        exc_job = (await dequeue_all(queries, "exc_ep"))[0]
        record = models.TracebackRecord.from_exception(ValueError("kaboom"), exc_job.id)
        await queries.log_jobs([(held_job, "failed", None), (exc_job, "exception", record)])

        response = await client.get("/failures")
        assert "held_ep" in response.text
        assert "exc_ep" in response.text
        assert "ValueError: kaboom" in response.text

    async def test_job_detail_exception_panel_no_table_details(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        """Exceptions render as a dedicated panel, not expandable table cells."""
        await queries.enqueue("exc_ep", None)
        (job,) = await dequeue_all(queries, "exc_ep")
        record = models.TracebackRecord.from_exception(RuntimeError("nope"), job.id)
        await queries.log_jobs([(job, "exception", record)])

        page = (await client.get(f"/jobs/{job.id}")).text
        assert 'class="exc"' in page
        assert "RuntimeError: nope" in page
        assert "<details" not in page

    async def test_jobs_excludes_held_failed_failures_owns_them(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        """/jobs shows only active queue rows; held-failed jobs belong to /failures."""
        await queries.enqueue(["active_ep", "held_ep"], [None, None], [0, 0])
        (held,) = await dequeue_all(queries, "held_ep")
        await queries.log_jobs([(held, "failed", None)])

        jobs_page = (await client.get("/jobs")).text
        assert "active_ep" in jobs_page
        assert "held_ep" not in jobs_page
        assert ">failed</option>" not in jobs_page

        failures_page = (await client.get("/failures")).text
        assert "held_ep" in failures_page


class TestActions:
    async def test_cancel_job(self, client: httpx.AsyncClient, queries: InMemoryQueries) -> None:
        (job_id,) = await queries.enqueue("ep", None)
        response = await client.post(f"/jobs/{job_id}/cancel")
        assert response.status_code == 204
        assert response.headers["HX-Refresh"] == "true"
        assert await queries.queue_job_by_id(job_id) is None

    async def test_requeue_selected(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await queries.enqueue("ep", None)
        (job,) = await dequeue_all(queries, "ep")
        await queries.log_jobs([(job, "failed", None)])

        response = await client.post("/jobs/requeue", data={"ids": str(job.id)})
        assert response.status_code == 204
        refreshed = await queries.queue_job_by_id(job.id)
        assert refreshed is not None and refreshed.status == "queued"

    async def test_requeue_without_ids_is_noop(self, client: httpx.AsyncClient) -> None:
        assert (await client.post("/jobs/requeue", data={})).status_code == 204

    async def test_cross_site_post_rejected(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        (job_id,) = await queries.enqueue("ep", None)
        response = await client.post(
            f"/jobs/{job_id}/cancel",
            headers={"sec-fetch-site": "cross-site"},
        )
        assert response.status_code == 403
        assert await queries.queue_job_by_id(job_id) is not None

    async def test_same_origin_post_accepted(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        (job_id,) = await queries.enqueue("ep", None)
        response = await client.post(
            f"/jobs/{job_id}/cancel",
            headers={"sec-fetch-site": "same-origin"},
        )
        assert response.status_code == 204


class TestBasicAuth:
    def test_no_env_means_no_dependency(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(USER_ENV, raising=False)
        monkeypatch.delenv(PASSWORD_ENV, raising=False)
        assert create_basic_auth_dependency() is None

    async def test_auth_matrix(
        self, monkeypatch: pytest.MonkeyPatch, queries: InMemoryQueries
    ) -> None:
        monkeypatch.setenv(USER_ENV, "admin")
        monkeypatch.setenv(PASSWORD_ENV, "hunter2")
        auth = create_basic_auth_dependency()
        assert auth is not None

        app = FastAPI()
        app.state.pgq_queries = queries
        app.include_router(create_web_router(dependencies=[Depends(auth)], include_sse=False))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            assert (await client.get("/")).status_code == 401
            assert (await client.get("/", auth=("admin", "wrong"))).status_code == 401
            assert (await client.get("/", auth=("admin", "hunter2"))).status_code == 200


class TestCreateWebApp:
    def test_warns_when_unauthenticated(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        from pgqueuer.adapters.web.app import create_web_app

        monkeypatch.delenv(USER_ENV, raising=False)
        monkeypatch.delenv(PASSWORD_ENV, raising=False)
        with caplog.at_level("WARNING", logger="pgqueuer"):
            create_web_app()
        assert any("WITHOUT authentication" in r.getMessage() for r in caplog.records)

    def test_no_warning_when_auth_configured(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        from pgqueuer.adapters.web.app import create_web_app

        monkeypatch.setenv(USER_ENV, "admin")
        monkeypatch.setenv(PASSWORD_ENV, "hunter2")
        with caplog.at_level("WARNING", logger="pgqueuer"):
            create_web_app()
        assert not any("WITHOUT authentication" in r.getMessage() for r in caplog.records)


class TestRouterWithoutSse:
    async def test_events_route_absent(self, queries: InMemoryQueries) -> None:
        app = FastAPI()
        app.state.pgq_queries = queries
        app.include_router(create_web_router(include_sse=False))
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            assert (await client.get("/events")).status_code == 404
            assert (await client.get("/")).status_code == 200


class TestBroadcaster:
    async def test_debounced_fanout(self, queries: InMemoryQueries) -> None:
        driver: InMemoryDriver = queries.driver
        broadcaster = Broadcaster(
            driver=driver,
            channel=queries.qbq.settings.channel,
            debounce=timedelta(milliseconds=10),
        )
        await broadcaster.start()

        stream = broadcaster.stream()
        assert (await anext(stream)).startswith("retry:")

        await queries.enqueue(["ep"] * 3, [None] * 3, [0] * 3)
        frame = await asyncio.wait_for(anext(stream), timeout=2)
        assert "queue-change" in frame

        # The burst above collapsed into one pending signal at most.
        assert broadcaster.subscribers and all(q.qsize() <= 1 for q in broadcaster.subscribers)
        await stream.aclose()
        assert not broadcaster.subscribers


class TestRenderPayload:
    def test_none_is_empty(self) -> None:
        assert render_payload(None).kind == "empty"

    def test_json_pretty_printed(self) -> None:
        rendered = render_payload(b'{"a":1}')
        assert rendered.kind == "json"
        assert rendered.text == '{\n  "a": 1\n}'

    def test_plain_text(self) -> None:
        rendered = render_payload(b"hello world")
        assert rendered.kind == "text"
        assert rendered.text == "hello world"

    def test_binary_hex_preview(self) -> None:
        rendered = render_payload(bytes(range(256)) * 4)
        assert rendered.kind == "binary"
        assert "bytes total" in rendered.text


class TestSparklineSvg:
    def test_empty_values(self) -> None:
        assert sparkline_svg([]) == ""

    def test_bars_rendered(self) -> None:
        svg = sparkline_svg([0, 1, 2])
        assert svg.startswith("<svg")
        assert svg.count("<rect") == 3


class TestTemplateFormatters:
    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (None, "–"),
            (5, "5s"),
            (65, "1m 5s"),
            (3660, "1h 1m"),
            (90000, "1d 1h"),
        ],
    )
    def test_fmt_age(self, seconds: float | None, expected: str) -> None:
        assert fmt_age(seconds) == expected

    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (None, "–"),
            (0.25, "250ms"),
            (5.5, "5.50s"),
            (65, "1m 5s"),
        ],
    )
    def test_fmt_duration(self, seconds: float | None, expected: str) -> None:
        assert fmt_duration(seconds) == expected

    def test_fmt_dt(self) -> None:
        assert fmt_dt(None) == "–"
        aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        assert fmt_dt(aware) == "2026-01-02 03:04:05"

    def test_fmt_rate(self) -> None:
        assert fmt_rate(None) == "–"
        assert fmt_rate(0.123) == "12.3%"


class TestJobsPagination:
    async def fill(self, queries: InMemoryQueries, count: int) -> None:
        for _ in range(count):
            await queries.enqueue("page_ep", None)

    async def test_first_page_links_forward_only(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await self.fill(queries, PAGE_SIZE + 1)
        response = await client.get("/partials/jobs")
        assert response.status_code == 200
        assert "older »" in response.text
        assert "page=2" in response.text
        assert "« newer" not in response.text

    async def test_second_page_links_back(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await self.fill(queries, PAGE_SIZE + 1)
        response = await client.get("/partials/jobs", params={"page": 2})
        assert response.status_code == 200
        assert "« newer" in response.text
        assert "older »" not in response.text
        assert response.text.count("<tr id=") == 1

    async def test_page_clamped_to_one(
        self, client: httpx.AsyncClient, queries: InMemoryQueries
    ) -> None:
        await self.fill(queries, 1)
        response = await client.get("/partials/jobs", params={"page": 0})
        assert response.status_code == 200
        assert "page 1" in response.text
