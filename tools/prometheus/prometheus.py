from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import Response

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.prometheus import MetricNames, collect_metrics
from pgqueuer.queries import Queries


def get_queries(request: Request) -> Queries:
    """Retrieve the Queries object from the FastAPI application state."""
    pgq_queries = request.app.extra["pgq_queries"]
    assert isinstance(pgq_queries, Queries)
    return pgq_queries


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Context manager for initializing and tearing down resources on app startup and shutdown."""
    connection = await asyncpg.connect()
    app.extra["pgq_queries"] = Queries(AsyncpgDriver(connection))
    try:
        yield
    finally:
        await connection.close()


def create_metrics_router() -> APIRouter:
    """Create an API router that includes the metrics endpoint."""
    router = APIRouter()

    @router.get("/metrics", response_class=Response)
    async def metrics(queries: Annotated[Queries, Depends(get_queries)]) -> Response:
        prefix = os.environ.get("PGQUEUER_PREFIX", "")
        metric_names = MetricNames(
            queue_count=f"{prefix}pgqueuer_queue_count",
            log_count=f"{prefix}pgqueuer_logs_count",
        )
        return Response(
            content=await collect_metrics(queries, metric_names=metric_names),
            media_type="text/plain",
        )

    @router.get("/health", response_class=Response)
    async def health() -> Response:
        return Response(status_code=200)

    return router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application with all routes and lifecycle events."""
    app = FastAPI(lifespan=lifespan)
    metrics_router = create_metrics_router()
    app.include_router(metrics_router)
    return app
