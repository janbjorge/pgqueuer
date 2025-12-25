from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from flask import Flask

from pgqueuer.metrics import prometheus as metrics
from pgqueuer.metrics.fastapi import create_metrics_router
from pgqueuer.metrics.flask import create_metrics_blueprint
from pgqueuer.models import LogStatistics, QueueStatistics


class FakeQueries:
    """A fake Queries implementation for testing without database access."""

    def __init__(
        self,
        queue_stats: list[QueueStatistics] | None = None,
        log_stats: list[LogStatistics] | None = None,
    ) -> None:
        self.queue_stats = queue_stats or []
        self.log_stats = log_stats or []

    async def queue_size(self) -> list[QueueStatistics]:
        return self.queue_stats

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[LogStatistics]:
        return self.log_stats


def test_prometheus_format_includes_labels() -> None:
    formatted = metrics.prometheus_format(
        metric_name="pgqueuer_queue_count",
        labels={"entrypoint": "default", "status": "pending"},
        value=3,
    )
    assert formatted == 'pgqueuer_queue_count{entrypoint="default",status="pending"} 3'


def test_custom_metric_names_are_used() -> None:
    metric_names = metrics.MetricNames(
        queue_count="custom_queue_count",
        log_count="custom_log_count",
    )
    queue_stats = [QueueStatistics(entrypoint="main", status="queued", count=2, priority=0)]
    log_stats = [
        LogStatistics(
            entrypoint="main",
            status="successful",
            count=5,
            priority=0,
            created=datetime.now(tz=timezone.utc),
        )
    ]

    queue_output = list(metrics.aggregated_queue_statistics(queue_stats, metric_names))
    log_output = list(metrics.aggregated_log_statistics(log_stats, metric_names))

    assert queue_output == [
        'custom_queue_count{aggregation="sum",entrypoint="main",status="queued"} 2'
    ]
    assert log_output == [
        'custom_log_count{aggregation="sum",entrypoint="main",status="successful"} 5'
    ]


async def test_collect_metrics_returns_payload() -> None:
    queries = FakeQueries()

    metrics_payload = await metrics.collect_metrics(queries)

    assert isinstance(metrics_payload, str)


async def test_collect_metrics_with_statistics() -> None:
    queue_stats = [
        QueueStatistics(entrypoint="worker", status="queued", count=10, priority=0),
    ]
    log_stats = [
        LogStatistics(
            entrypoint="worker",
            status="successful",
            count=25,
            priority=0,
            created=datetime.now(tz=timezone.utc),
        ),
    ]

    queries = FakeQueries(queue_stats=queue_stats, log_stats=log_stats)

    metrics_payload = await metrics.collect_metrics(queries)

    assert (
        'pgqueuer_queue_count{aggregation="sum",entrypoint="worker",status="queued"} 10'
        in metrics_payload
    )
    assert (
        'pgqueuer_logs_count{aggregation="sum",entrypoint="worker",status="successful"} 25'
        in metrics_payload
    )


def test_fastapi_create_metrics_router() -> None:
    queries = FakeQueries()
    router = create_metrics_router(queries)

    app = FastAPI()
    app.include_router(router)

    with TestClient(app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_fastapi_create_metrics_router_custom_path() -> None:
    queries = FakeQueries()
    router = create_metrics_router(queries, path="/custom/metrics")

    app = FastAPI()
    app.include_router(router)

    with TestClient(app) as client:
        response = client.get("/custom/metrics")

    assert response.status_code == 200


def test_flask_create_metrics_blueprint() -> None:
    queries = FakeQueries()
    blueprint = create_metrics_blueprint(queries)

    app = Flask(__name__)
    app.register_blueprint(blueprint)

    with app.test_client() as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.content_type == "text/plain"


def test_flask_create_metrics_blueprint_with_prefix() -> None:
    queries = FakeQueries()
    blueprint = create_metrics_blueprint(queries, url_prefix="/monitoring")

    app = Flask(__name__)
    app.register_blueprint(blueprint)

    with app.test_client() as client:
        response = client.get("/monitoring/metrics")

    assert response.status_code == 200
