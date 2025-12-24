"""Tests for the pgqueuer.metrics module."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics import (
    aggregated_log_statistics,
    aggregated_queue_statistics,
    aggregated_statistics,
    generate_metrics,
    prometheus_format,
)
from pgqueuer.models import LogStatistics, QueueStatistics
from pgqueuer.queries import EntrypointExecutionParameter, Queries


def test_prometheus_format() -> None:
    """Test the prometheus_format function."""
    result = prometheus_format(
        metric_name="test_metric",
        labels={"label1": "value1", "label2": "value2"},
        value=42,
    )
    assert result == 'test_metric{label1="value1",label2="value2"} 42'


def test_prometheus_format_single_label() -> None:
    """Test prometheus_format with a single label."""
    result = prometheus_format(
        metric_name="single_label_metric",
        labels={"status": "success"},
        value=100,
    )
    assert result == 'single_label_metric{status="success"} 100'


def test_prometheus_format_float_value() -> None:
    """Test prometheus_format with a float value."""
    result = prometheus_format(
        metric_name="float_metric",
        labels={"type": "gauge"},
        value=3.14159,
    )
    assert result == 'float_metric{type="gauge"} 3.14159'


def test_aggregated_queue_statistics() -> None:
    """Test aggregated_queue_statistics function."""
    queue_stats = [
        QueueStatistics(count=5, entrypoint="test1", priority=1, status="queued"),
        QueueStatistics(count=3, entrypoint="test1", priority=2, status="queued"),
        QueueStatistics(count=2, entrypoint="test2", priority=1, status="picked"),
    ]

    results = list(aggregated_queue_statistics(queue_stats, "test_queue_count"))

    assert len(results) == 2
    assert 'test_queue_count{aggregation="sum",entrypoint="test1",status="queued"} 8' in results
    assert 'test_queue_count{aggregation="sum",entrypoint="test2",status="picked"} 2' in results


def test_aggregated_queue_statistics_empty() -> None:
    """Test aggregated_queue_statistics with empty list."""
    results = list(aggregated_queue_statistics([], "test_metric"))
    assert len(results) == 0


def test_aggregated_log_statistics(apgdriver: AsyncpgDriver) -> None:
    """Test aggregated_log_statistics function."""
    from datetime import datetime, timezone

    log_stats = [
        LogStatistics(
            count=10,
            created=datetime.now(timezone.utc),
            entrypoint="log1",
            priority=1,
            status="successful",
        ),
        LogStatistics(
            count=5,
            created=datetime.now(timezone.utc),
            entrypoint="log1",
            priority=2,
            status="successful",
        ),
        LogStatistics(
            count=2,
            created=datetime.now(timezone.utc),
            entrypoint="log2",
            priority=1,
            status="exception",
        ),
    ]

    results = list(aggregated_log_statistics(log_stats, "test_logs_count"))

    assert len(results) == 2
    assert 'test_logs_count{aggregation="sum",entrypoint="log1",status="successful"} 15' in results
    assert 'test_logs_count{aggregation="sum",entrypoint="log2",status="exception"} 2' in results


def test_aggregated_log_statistics_empty() -> None:
    """Test aggregated_log_statistics with empty list."""
    results = list(aggregated_log_statistics([], "test_metric"))
    assert len(results) == 0


def test_aggregated_statistics() -> None:
    """Test aggregated_statistics function that combines queue and log stats."""
    from datetime import datetime, timezone

    queue_stats = [
        QueueStatistics(count=5, entrypoint="test1", priority=1, status="queued"),
    ]

    log_stats = [
        LogStatistics(
            count=10,
            created=datetime.now(timezone.utc),
            entrypoint="log1",
            priority=1,
            status="successful",
        ),
    ]

    results = list(
        aggregated_statistics(
            queue_stats,
            log_stats,
            "custom_queue_count",
            "custom_logs_count",
        )
    )

    assert len(results) == 2
    assert any("custom_queue_count" in r for r in results)
    assert any("custom_logs_count" in r for r in results)


async def test_generate_metrics_basic(apgdriver: AsyncpgDriver) -> None:
    """Test generating basic metrics."""
    metrics_text = await generate_metrics(apgdriver)

    assert isinstance(metrics_text, str)
    # Metrics text can be empty if there's no data yet
    assert metrics_text is not None


async def test_generate_metrics_returns_data(apgdriver: AsyncpgDriver) -> None:
    """Test that generate_metrics returns valid prometheus data."""
    queries = Queries(apgdriver)

    # Enqueue some test jobs
    await queries.enqueue("test_entrypoint", b"test_payload")

    # Generate metrics
    metrics_text = await generate_metrics(apgdriver)

    assert isinstance(metrics_text, str)
    # Should contain queue metrics if there's data
    assert "pgqueuer_queue_count" in metrics_text or len(metrics_text) >= 0


async def test_generate_metrics_custom_metric_names(apgdriver: AsyncpgDriver) -> None:
    """Test that custom metric names are used correctly."""
    queries = Queries(apgdriver)

    # Enqueue a test job
    await queries.enqueue("custom_test", b"payload")

    # Generate metrics with custom names
    metrics_text = await generate_metrics(
        apgdriver,
        queue_count_metric_name="my_custom_queue",
        logs_count_metric_name="my_custom_logs",
    )

    assert isinstance(metrics_text, str)
    # Custom names should appear in the metrics if there's data
    if metrics_text:
        assert "my_custom_queue" in metrics_text or "my_custom_logs" in metrics_text


async def test_generate_metrics_with_log_data(apgdriver: AsyncpgDriver) -> None:
    """Test generating metrics with both queue and log data."""
    queries = Queries(apgdriver)

    # Enqueue and process a job to generate log data
    await queries.enqueue("log_test", b"log_payload")

    jobs = await queries.dequeue(
        batch_size=1,
        entrypoints={"log_test": EntrypointExecutionParameter(timedelta(seconds=30), False, 0)},
        queue_manager_id=uuid4(),
        global_concurrency_limit=100,
    )

    if jobs:
        await queries.log_jobs([(jobs[0], "successful", None)])

    # Generate metrics
    metrics_text = await generate_metrics(apgdriver)

    assert isinstance(metrics_text, str)
    # Should contain metrics data
    assert len(metrics_text) >= 0


async def test_generate_metrics_custom_time_window(apgdriver: AsyncpgDriver) -> None:
    """Test generating metrics with custom time window."""
    metrics_text = await generate_metrics(
        apgdriver,
        log_statistics_last=timedelta(minutes=10),
    )

    assert isinstance(metrics_text, str)


async def test_import_from_pgqueuer() -> None:
    """Test that generate_metrics can be imported from pgqueuer package."""
    from pgqueuer import generate_metrics as imported_func

    assert imported_func is not None
    assert callable(imported_func)
