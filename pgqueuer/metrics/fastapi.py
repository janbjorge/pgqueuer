from __future__ import annotations

from datetime import timedelta

try:
    from fastapi import APIRouter
    from fastapi.responses import Response
except ImportError as e:
    raise ImportError("fastapi is required for this module.") from e

from pgqueuer.metrics.prometheus import MetricNames, collect_metrics
from pgqueuer.ports.repository import QueueRepositoryPort


def create_metrics_router(
    repository: QueueRepositoryPort,
    *,
    metric_names: MetricNames | None = None,
    last: timedelta = timedelta(minutes=5),
    path: str = "/metrics",
) -> APIRouter:
    """
    Create a FastAPI router with a Prometheus metrics endpoint.

    Example:
        >>> from contextlib import asynccontextmanager
        >>> from fastapi import FastAPI
        >>> from pgqueuer.metrics.fastapi import create_metrics_router
        >>> from pgqueuer.adapters.persistence.queries import Queries
        >>>
        >>> @asynccontextmanager
        >>> async def lifespan(app: FastAPI):
        ...     app.state.repository = Queries(driver)
        ...     yield
        >>>
        >>> app = FastAPI(lifespan=lifespan)
        >>> app.include_router(create_metrics_router(app.state.repository))
    """
    router = APIRouter()

    @router.get(path)
    async def metrics() -> Response:
        content = await collect_metrics(repository, metric_names=metric_names, last=last)
        return Response(content=content, media_type="text/plain")

    return router
