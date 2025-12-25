from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from itertools import groupby
from typing import Callable, Generator, Iterable, Protocol, TypeAlias

from pgqueuer.models import LogStatistics, QueueStatistics
from pgqueuer.qb import add_prefix

ReduceFn: TypeAlias = Callable[[Iterable[float]], float]


class QueriesProtocol(Protocol):
    """Protocol defining the interface required by collect_metrics."""

    async def queue_size(self) -> list[QueueStatistics]: ...

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[LogStatistics]: ...


@dataclass
class MetricNames:
    """
    Configuration for Prometheus metric names.

    Allows customization of the metric names emitted by the collector.
    Use this when you need to align with existing naming conventions
    or avoid conflicts with other metrics in your system.

    Example:
        >>> names = MetricNames(
        ...     queue_count="myapp_queue_size",
        ...     log_count="myapp_processed_jobs",
        ... )
    """

    queue_count: str = "pgqueuer_queue_count"
    log_count: str = "pgqueuer_logs_count"


def prometheus_format(
    metric_name: str,
    labels: dict[str, str],
    value: float | int,
) -> str:
    """
    Format a single metric into Prometheus exposition format.

    Example:
        >>> prometheus_format("queue_size", {"entrypoint": "send_email"}, 42)
        'queue_size{entrypoint="send_email"} 42'
    """
    label_parts = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f"{add_prefix(metric_name)}{{{label_parts}}} {value}"


def aggregated_queue_statistics(
    queue_statistics: list[QueueStatistics],
    metric_names: MetricNames,
) -> Generator[str, None, None]:
    """
    Generate Prometheus-formatted lines for queue statistics.

    Aggregates queue statistics by entrypoint and status, summing
    the counts for each group.
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
            metric_name=metric_names.queue_count,
            labels={"aggregation": "sum", "entrypoint": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )


def aggregated_log_statistics(
    log_statistics: list[LogStatistics],
    metric_names: MetricNames,
) -> Generator[str, None, None]:
    """
    Generate Prometheus-formatted lines for log statistics.

    Aggregates log statistics by entrypoint and status, summing
    the counts for each group. This reflects job processing history
    over a configurable time window.
    """
    aggregated = (
        (entrypoint, status, tuple(items))
        for (entrypoint, status), items in groupby(
            sorted(log_statistics, key=lambda x: (x.entrypoint, x.status)),
            key=lambda x: (x.entrypoint, x.status),
        )
    )

    for entrypoint, status, items in aggregated:
        yield prometheus_format(
            metric_name=metric_names.log_count,
            labels={"aggregation": "sum", "entrypoint": entrypoint, "status": status},
            value=sum(x.count for x in items),
        )


def aggregated_statistics(
    queue_statistics: list[QueueStatistics],
    log_statistics: list[LogStatistics],
    metric_names: MetricNames,
) -> Generator[str, None, None]:
    """Combine queue and log statistics into a single metrics stream."""
    yield from aggregated_queue_statistics(queue_statistics, metric_names)
    yield from aggregated_log_statistics(log_statistics, metric_names)


async def collect_metrics(
    queries: QueriesProtocol,
    *,
    metric_names: MetricNames | None = None,
    last: timedelta = timedelta(minutes=5),
) -> str:
    """
    Collect and format all PGQueuer metrics for Prometheus.

    This is the main entry point for metrics collection. It queries the
    database for current queue sizes and recent job processing statistics,
    then formats them into Prometheus exposition format.

    Example:
        >>> from pgqueuer.db import AsyncpgDriver
        >>> from pgqueuer.queries import Queries
        >>> import asyncpg
        >>>
        >>> async def get_metrics():
        ...     conn = await asyncpg.connect()
        ...     queries = Queries(AsyncpgDriver(conn))
        ...     return await collect_metrics(queries)
    """
    queue_statistics = await queries.queue_size()
    log_statistics = await queries.log_statistics(tail=None, last=last)
    names = metric_names or MetricNames()
    return "\n".join(aggregated_statistics(queue_statistics, log_statistics, names))
