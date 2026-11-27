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
    """Create a FastAPI router with a Prometheus metrics endpoint.

    The router keeps *repository*, so call this function after the repository exists.

    Usage example::

        import asyncio
        import uvicorn
        from fastapi import FastAPI
        from pgqueuer import AsyncpgPoolDriver, Queries
        from pgqueuer.adapters.connections import create_asyncpg_pool
        from pgqueuer.metrics.fastapi import create_metrics_router

        async def main() -> None:
            async with create_asyncpg_pool() as pool:
                app = FastAPI()
                app.include_router(create_metrics_router(Queries(AsyncpgPoolDriver(pool))))
                await uvicorn.Server(uvicorn.Config(app)).serve()

        asyncio.run(main())
    """
    router = APIRouter()

    @router.get(path)
    async def metrics() -> Response:
        content = await collect_metrics(repository, metric_names=metric_names, last=last)
        return Response(content=content, media_type="text/plain")

    return router
