"""
Prometheus metrics integration for PGQueuer.

This module provides a FastAPI router factory for exposing Prometheus-compatible
metrics about queue and log statistics. The router can be integrated into existing
FastAPI applications.
"""

from __future__ import annotations

from datetime import timedelta
from itertools import groupby
from typing import Generator

from fastapi import APIRouter
from fastapi.responses import Response

from pgqueuer.db import Driver
from pgqueuer.models import LogStatistics, QueueStatistics
from pgqueuer.qb import add_prefix
from pgqueuer.queries import Queries


def prometheus_format(
    metric_name: str,
    labels: dict[str, str],
    value: float | int,
) -> str:
    """
    Format metric data into a Prometheus-compatible string.

    Args:
        metric_name: The name of the metric.
        labels: A dictionary of label key-value pairs.
        value: The numeric value of the metric.

    Returns:
        A Prometheus-formatted metric string.

    Example:
        >>> prometheus_format("queue_count", {"status": "queued"}, 42)
        'queue_count{status="queued"} 42'
    """
    label_parts = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f"{metric_name}{{{label_parts}}} {value}"


def aggregated_queue_statistics(
    queue_statistics: list[QueueStatistics],
    metric_name: str,
) -> Generator[str, None, None]:
    """
    Generate Prometheus-formatted strings for aggregated queue statistics.

    Groups queue statistics by entrypoint and status, then yields formatted
    metrics for each group.

    Args:
        queue_statistics: List of queue statistics to aggregate.
        metric_name: The name to use for the metric.

    Yields:
        Prometheus-formatted metric strings.
    """
    aggregated = (
        (entrypoint, status, tuple(items))
        for (entrypoint, status), items in groupby(
            sorted(queue_statistics, key=lambda x: (x.entrypoint, x.status)),
            key=lambda x: (x.entrypoint, x.status),
        )
    )
    for entrypoint, status, items in aggregated:
        yield prometheus_format(
            metric_name=metric_name,
            labels={"aggregation": "sum", "entrypoint": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )


def aggregated_log_statistics(
    log_statistics: list[LogStatistics],
    metric_name: str,
) -> Generator[str, None, None]:
    """
    Generate Prometheus-formatted strings for aggregated log statistics.

    Groups log statistics by entrypoint and status, then yields formatted
    metrics for each group.

    Args:
        log_statistics: List of log statistics to aggregate.
        metric_name: The name to use for the metric.

    Yields:
        Prometheus-formatted metric strings.
    """
    aggregated_log_statistics = (
        (entrypoint, status, tuple(items))
        for (entrypoint, status), items in groupby(
            sorted(log_statistics, key=lambda x: (x.entrypoint, x.status)),
            key=lambda x: (x.entrypoint, x.status),
        )
    )

    for entrypoint, status, items in aggregated_log_statistics:
        yield prometheus_format(
            metric_name=metric_name,
            labels={"aggregation": "sum", "entrypoint": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )


def aggregated_statistics(
    queue_statistics: list[QueueStatistics],
    log_statistics: list[LogStatistics],
    queue_count_metric_name: str,
    logs_count_metric_name: str,
) -> Generator[str, None, None]:
    """
    Combine and generate Prometheus metrics for both queue and log statistics.

    Args:
        queue_statistics: List of queue statistics.
        log_statistics: List of log statistics.
        queue_count_metric_name: Name for the queue count metric.
        logs_count_metric_name: Name for the logs count metric.

    Yields:
        Prometheus-formatted metric strings.
    """
    yield from aggregated_queue_statistics(queue_statistics, queue_count_metric_name)
    yield from aggregated_log_statistics(log_statistics, logs_count_metric_name)


def create_metrics_router(
    driver: Driver,
    *,
    queue_count_metric_name: str | None = None,
    logs_count_metric_name: str | None = None,
    log_statistics_last: timedelta = timedelta(minutes=5),
) -> APIRouter:
    """
    Create a FastAPI router with a /metrics endpoint for Prometheus.

    This function creates a router that exposes queue and log statistics
    in Prometheus format. The driver parameter is used to query the database
    for statistics.

    Args:
        driver: A database driver instance (e.g., AsyncpgDriver) for executing queries.
        queue_count_metric_name: Custom name for the queue count metric.
            Defaults to "pgqueuer_queue_count" (with prefix if configured).
        logs_count_metric_name: Custom name for the logs count metric.
            Defaults to "pgqueuer_logs_count" (with prefix if configured).
        log_statistics_last: Time window for log statistics (default: 5 minutes).

    Returns:
        An APIRouter instance with the /metrics endpoint configured.

    Example:
        >>> import asyncpg
        >>> from pgqueuer.db import AsyncpgDriver
        >>> from pgqueuer.metrics import create_metrics_router
        >>>
        >>> # In your FastAPI app setup
        >>> async def setup():
        ...     conn = await asyncpg.connect()
        ...     driver = AsyncpgDriver(conn)
        ...     metrics_router = create_metrics_router(driver)
        ...     app.include_router(metrics_router)
    """
    # Set default metric names with prefix support
    if queue_count_metric_name is None:
        queue_count_metric_name = add_prefix("pgqueuer_queue_count")
    if logs_count_metric_name is None:
        logs_count_metric_name = add_prefix("pgqueuer_logs_count")

    queries = Queries(driver)
    router = APIRouter()

    @router.get("/metrics", response_class=Response)
    async def metrics() -> Response:
        """Endpoint that returns Prometheus-formatted metrics."""
        queue_statistics = await queries.queue_size()
        log_statistics = await queries.log_statistics(
            tail=None,
            last=log_statistics_last,
        )
        return Response(
            content="\n".join(
                aggregated_statistics(
                    queue_statistics,
                    log_statistics,
                    queue_count_metric_name,
                    logs_count_metric_name,
                )
            ),
            media_type="text/plain",
        )

    return router
