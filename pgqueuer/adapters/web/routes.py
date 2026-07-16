from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

try:
    from fastapi import APIRouter, Depends, HTTPException, Request, Response, params
    from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
    from fastapi.templating import Jinja2Templates
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

from pgqueuer.adapters.web.deps import get_broadcaster, get_insights, get_management
from pgqueuer.adapters.web.payload import render_payload
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.core.insights import (
    DEFAULT_STALE_THRESHOLD,
    InsightsService,
    QueueManagementService,
)
from pgqueuer.domain import models

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"

STATIC_FILES = {
    "htmx.min.js": "application/javascript",
    "idiomorph-ext.min.js": "application/javascript",
    "sse.js": "application/javascript",
    "style.css": "text/css",
}

WINDOWS = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
}

ACTIVE_STATUSES: tuple[models.JOB_STATUS, ...] = ("queued", "picked")

PAGE_SIZE = 50


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "–"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    return f"{s // 86400}d {(s % 86400) // 3600}h"


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "–"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    return fmt_age(seconds)


def fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "–"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_rate(rate: float | None) -> str:
    if rate is None:
        return "–"
    return f"{rate * 100:.1f}%"


def sparkline_svg(values: Sequence[int], width: int = 120, height: int = 24) -> str:
    """Inline-SVG bar sparkline; no client-side charting library needed."""
    if not values:
        return ""
    peak = max(values) or 1
    bar_width = width / len(values)
    bars = []
    for i, value in enumerate(values):
        bar_height = max((value / peak) * height, 1 if value else 0)
        bars.append(
            f'<rect x="{i * bar_width:.1f}" y="{height - bar_height:.1f}" '
            f'width="{max(bar_width - 1, 1):.1f}" height="{bar_height:.1f}"/>'
        )
    return (
        f'<svg class="sparkline" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}">{"".join(bars)}</svg>'
    )


def create_templates() -> Jinja2Templates:
    templates = Jinja2Templates(directory=TEMPLATES_DIR)
    templates.env.filters["age"] = fmt_age
    templates.env.filters["duration"] = fmt_duration
    templates.env.filters["dt"] = fmt_dt
    templates.env.filters["rate"] = fmt_rate
    templates.env.filters["sparkline"] = sparkline_svg
    return templates


def verify_same_origin(request: Request) -> None:
    """Reject cross-site POSTs when the browser sends Sec-Fetch-Site."""
    site = request.headers.get("sec-fetch-site")
    if site is not None and site not in ("same-origin", "none"):
        raise HTTPException(status_code=403, detail="cross-site request rejected")


def parse_window(name: str) -> timedelta:
    return WINDOWS.get(name, WINDOWS["1h"])


def parse_statuses(status: str) -> list[models.JOB_STATUS]:
    """Held-failed jobs are owned by the failures page; the browser only shows active rows."""
    return [s for s in ACTIVE_STATUSES if s == status] or list(ACTIVE_STATUSES)


def create_web_router(  # noqa: C901
    *,
    dependencies: Sequence[params.Depends] | None = None,
    include_sse: bool = True,
) -> APIRouter:
    """Build the dashboard router; repository is read from ``app.state.pgq_queries``.

    Usage example::

        from contextlib import asynccontextmanager
        from fastapi import FastAPI
        from pgqueuer.web import create_web_router
        from pgqueuer.queries import Queries

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            app.state.pgq_queries = Queries(driver)
            yield

        app = FastAPI(lifespan=lifespan)
        app.include_router(create_web_router(include_sse=False), prefix="/pgqueuer")

    Pass ``dependencies=[Depends(my_auth)]`` to guard every route with custom auth.
    With ``include_sse=False`` live regions fall back to their polling triggers,
    and no broadcaster is required on ``app.state``.
    """
    router = APIRouter(dependencies=list(dependencies) if dependencies else None)
    templates = create_templates()

    def render(
        request: Request,
        name: str,
        context: dict[str, object],
        status_code: int = 200,
    ) -> HTMLResponse:
        context.setdefault("sse_enabled", include_sse)
        return templates.TemplateResponse(request, name, context, status_code=status_code)

    @router.get("/static/{filename}", name="web_static", include_in_schema=False)
    async def web_static(filename: str) -> FileResponse:
        if filename not in STATIC_FILES:
            raise HTTPException(status_code=404)
        return FileResponse(STATIC_DIR / filename, media_type=STATIC_FILES[filename])

    @router.get("/healthz", name="healthz", include_in_schema=False)
    async def healthz(
        insights: InsightsService = Depends(get_insights),
    ) -> dict[str, str]:
        await insights.queue_size()
        return {"status": "ok"}

    if include_sse:

        @router.get("/events", name="sse_events", include_in_schema=False)
        async def sse_events(
            broadcaster: Broadcaster = Depends(get_broadcaster),
        ) -> StreamingResponse:
            return StreamingResponse(
                broadcaster.stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

    async def overview_context(insights: InsightsService) -> dict[str, object]:
        return {
            "snapshot": await insights.overview(),
            "ages": await insights.queue_age(),
        }

    @router.get("/", name="overview", response_class=HTMLResponse)
    async def overview(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "overview.html", await overview_context(insights))

    @router.get("/partials/overview", name="partial_overview", response_class=HTMLResponse)
    async def partial_overview(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "partials/overview.html", await overview_context(insights))

    async def entrypoints_context(
        insights: InsightsService,
        window: str,
    ) -> dict[str, object]:
        return {
            "stats": await insights.entrypoint_stats(parse_window(window)),
            "window": window if window in WINDOWS else "1h",
            "windows": list(WINDOWS),
        }

    @router.get("/entrypoints", name="entrypoints", response_class=HTMLResponse)
    async def entrypoints(
        request: Request,
        window: str = "1h",
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "entrypoints.html", await entrypoints_context(insights, window))

    @router.get(
        "/partials/entrypoints",
        name="partial_entrypoints",
        response_class=HTMLResponse,
    )
    async def partial_entrypoints(
        request: Request,
        window: str = "1h",
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "partials/entrypoints.html",
            await entrypoints_context(insights, window),
        )

    async def jobs_context(
        request: Request,
        insights: InsightsService,
        status: str,
        entrypoint: str,
        page: int,
    ) -> dict[str, object]:
        page = max(page, 1)
        jobs = await insights.browse_queue(
            limit=PAGE_SIZE + 1,
            offset=(page - 1) * PAGE_SIZE,
            statuses=parse_statuses(status),
            entrypoints=[entrypoint] if entrypoint else None,
        )
        has_next = len(jobs) > PAGE_SIZE
        query = {"status": status, "entrypoint": entrypoint}

        def partial(page_no: int) -> str:
            return str(request.url_for("partial_jobs").include_query_params(**query, page=page_no))

        return {
            "jobs": jobs[:PAGE_SIZE],
            "status": status,
            "entrypoint": entrypoint,
            "statuses": ACTIVE_STATUSES,
            "page": page,
            "filter_url": str(request.url_for("partial_jobs")),
            "prev_url": partial(page - 1) if page > 1 else None,
            "next_url": partial(page + 1) if has_next else None,
        }

    @router.get("/jobs", name="jobs", response_class=HTMLResponse)
    async def jobs(
        request: Request,
        status: str = "",
        entrypoint: str = "",
        page: int = 1,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "jobs.html",
            await jobs_context(request, insights, status, entrypoint, page),
        )

    @router.get("/partials/jobs", name="partial_jobs", response_class=HTMLResponse)
    async def partial_jobs(
        request: Request,
        status: str = "",
        entrypoint: str = "",
        page: int = 1,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "partials/jobs_table.html",
            await jobs_context(request, insights, status, entrypoint, page),
        )

    @router.get("/jobs/{job_id}", name="job_detail", response_class=HTMLResponse)
    async def job_detail(
        request: Request,
        job_id: int,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        job = await insights.job(models.JobId(job_id))
        history = await insights.job_history(models.JobId(job_id))
        if job is None and not history:
            raise HTTPException(status_code=404, detail=f"job {job_id} not found")
        return render(
            request,
            "job_detail.html",
            {
                "job_id": job_id,
                "job": job,
                "history": history,
                "payload": render_payload(job.payload) if job else None,
            },
        )

    async def failures_context(insights: InsightsService) -> dict[str, object]:
        return {
            "held": await insights.failed_jobs(),
            "exceptions": await insights.exception_logs(),
        }

    @router.get("/failures", name="failures", response_class=HTMLResponse)
    async def failures(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "failures.html", await failures_context(insights))

    @router.get("/partials/failures", name="partial_failures", response_class=HTMLResponse)
    async def partial_failures(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "partials/failures.html", await failures_context(insights))

    async def workers_context(
        insights: InsightsService,
        stale_after: int,
    ) -> dict[str, object]:
        threshold = timedelta(seconds=stale_after) if stale_after > 0 else DEFAULT_STALE_THRESHOLD
        return {
            "workers": await insights.active_workers(),
            "stale": await insights.stale_jobs(threshold),
            "stale_after": int(threshold.total_seconds()),
        }

    @router.get("/workers", name="workers", response_class=HTMLResponse)
    async def workers(
        request: Request,
        stale_after: int = 0,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(request, "workers.html", await workers_context(insights, stale_after))

    @router.get("/partials/workers", name="partial_workers", response_class=HTMLResponse)
    async def partial_workers(
        request: Request,
        stale_after: int = 0,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "partials/workers.html",
            await workers_context(insights, stale_after),
        )

    @router.get("/schedules", name="schedules", response_class=HTMLResponse)
    async def schedules(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "schedules.html",
            {"schedules": await insights.schedules(), "now": datetime.now(timezone.utc)},
        )

    @router.get("/system", name="system", response_class=HTMLResponse)
    async def system(
        request: Request,
        insights: InsightsService = Depends(get_insights),
    ) -> HTMLResponse:
        return render(
            request,
            "system.html",
            {
                "tables": await insights.schema_info(),
                "unaggregated": await insights.unaggregated_log_count(),
            },
        )

    @router.post(
        "/jobs/{job_id}/cancel",
        name="cancel_job",
        dependencies=[Depends(verify_same_origin)],
    )
    async def cancel_job(
        job_id: int,
        management: QueueManagementService = Depends(get_management),
    ) -> Response:
        await management.cancel([models.JobId(job_id)])
        return Response(status_code=204, headers={"HX-Refresh": "true"})

    @router.post(
        "/jobs/requeue",
        name="requeue_jobs_action",
        dependencies=[Depends(verify_same_origin)],
    )
    async def requeue_jobs_action(
        request: Request,
        management: QueueManagementService = Depends(get_management),
    ) -> Response:
        form = await request.form()
        ids = [models.JobId(int(raw)) for raw in form.getlist("ids") if isinstance(raw, str)]
        if ids:
            await management.requeue(ids)
        return Response(status_code=204, headers={"HX-Refresh": "true"})

    return router
