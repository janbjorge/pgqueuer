from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter
from fastapi.responses import Response

from pgqueuer.metrics.prometheus import MetricNames, QueriesProtocol, collect_metrics


def create_metrics_router(
    queries: QueriesProtocol,
    *,
    metric_names: MetricNames | None = None,
    last: timedelta = timedelta(minutes=5),
    path: str = "/metrics",
) -> APIRouter:
    """
    Create a FastAPI router with a Prometheus metrics endpoint.

    Example:
        >>> from fastapi import FastAPI
        >>> from pgqueuer.metrics.fastapi import create_metrics_router
        >>>
        >>> app = FastAPI()
        >>> app.include_router(create_metrics_router(queries))
    """
    router = APIRouter()

    @router.get(path)
    async def metrics() -> Response:
        content = await collect_metrics(queries, metric_names=metric_names, last=last)
        return Response(content=content, media_type="text/plain")

    return router
