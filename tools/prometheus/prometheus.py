from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import timedelta
from itertools import groupby
from statistics import mean, median
from typing import AsyncGenerator, Callable, Generator, Iterable, TypeAlias

import asyncpg
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import Response

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import LogStatistics, QueueStatistics
from pgqueuer.qb import add_prefix
from pgqueuer.queries import Queries

ReduceFn: TypeAlias = Callable[[Iterable[float]], float]


def get_queries(request: Request) -> Queries:
    """Retrieve the Queries object from the FastAPI application state."""
    return request.app.extra["pgq_queries"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Context manager for initializing and tearing down resources on app startup and shutdown."""
    connection = await asyncpg.connect()
    app.extra["pgq_queries"] = Queries(AsyncpgDriver(connection))
    try:
        yield
    finally:
        await connection.close()


def prometheus_format(
    metric_name: str,
    labels: dict[str, str],
    value: float | int,
) -> str:
    """Format metric data into a Prometheus-compatible string."""
    label_parts = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f"{add_prefix(metric_name)}{{{label_parts}}} {value}"


def aggregated_queue_statistics(
    queue_statistics: list[QueueStatistics],
) -> Generator[str, None, None]:
    """Generate Prometheus-formatted strings for aggregated queue statistics."""
    aggregated = [
        (entrypoint, status, tuple(items))
        for (entrypoint, status), items in groupby(
            sorted(queue_statistics, key=lambda x: (x.entrypoint, x.status)),
            key=lambda x: (x.entrypoint, x.status),
        )
    ]
    for entrypoint, status, items in aggregated:
        yield prometheus_format(
            metric_name="pgqueuer_queue_count",
            labels={"aggregation": "sum", "entrypoint": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )


def aggregated_log_statistics(
    log_statistics: list[LogStatistics],
) -> Generator[str, None, None]:
    """Generate Prometheus-formatted strings for aggregated log
    statistics, including time in queue metrics."""
    aggregated_log_statistics = [
        (entrypoint, status, tuple(items))
        for (entrypoint, status), items in groupby(
            sorted(log_statistics, key=lambda x: (x.entrypoint, x.status)),
            key=lambda x: (x.entrypoint, x.status),
        )
    ]

    for entrypoint, status, items in aggregated_log_statistics:
        yield prometheus_format(
            metric_name="pgqueuer_logs_count",
            labels={"aggregation": "sum", "entrypoini": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )

    aggregations: tuple[tuple[ReduceFn, str], ...] = (
        (min, "min"),
        (max, "max"),
        (mean, "mean"),
        (median, "median"),
    )
    for fn, name in aggregations:
        for entrypoint, status, items in aggregated_log_statistics:
            yield prometheus_format(
                metric_name="pgqueuer_logs_time_in_queue_seconds",
                labels={"aggregation": name, "entrypoint": entrypoint, "status": status},
                value=round(fn(x.time_in_queue.total_seconds() for x in items), 3),
            )


def aggregated_statistics(
    queue_statistics: list[QueueStatistics],
    log_statistics: list[LogStatistics],
) -> Generator[str, None, None]:
    """Combine and generate Prometheus metrics for both queue and log statistics."""
    yield from aggregated_queue_statistics(queue_statistics)
    yield from aggregated_log_statistics(log_statistics)


def create_metrics_router() -> APIRouter:
    """Create an API router that includes the metrics endpoint."""
    router = APIRouter()

    @router.get("/metrics", response_class=Response)
    async def metrics(queries: Queries = Depends(get_queries)) -> Response:
        queue_statistics = await queries.queue_size()
        log_statistics = await queries.log_statistics(
            tail=None,
            last=timedelta(minutes=5),
        )
        return Response(
            content="\n".join(aggregated_statistics(queue_statistics, log_statistics)),
            media_type="text/plain",
        )

    return router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application with all routes and lifecycle events."""
    app = FastAPI(lifespan=lifespan)
    metrics_router = create_metrics_router()
    app.include_router(metrics_router)
    return app
