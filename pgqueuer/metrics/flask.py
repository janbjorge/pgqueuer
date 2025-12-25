from __future__ import annotations

import asyncio
from datetime import timedelta

from flask import Blueprint, Response

from pgqueuer.metrics.prometheus import MetricNames, QueriesProtocol, collect_metrics


def create_metrics_blueprint(
    queries: QueriesProtocol,
    *,
    metric_names: MetricNames | None = None,
    last: timedelta = timedelta(minutes=5),
    name: str = "pgqueuer_metrics",
    url_prefix: str = "",
) -> Blueprint:
    """
    Create a Flask blueprint with a Prometheus metrics endpoint.

    Example:
        >>> from flask import Flask
        >>> from pgqueuer.metrics.flask import create_metrics_blueprint
        >>>
        >>> app = Flask(__name__)
        >>> app.register_blueprint(create_metrics_blueprint(queries))
    """
    blueprint = Blueprint(name, __name__, url_prefix=url_prefix)

    @blueprint.route("/metrics")
    def metrics() -> Response:
        content = asyncio.run(collect_metrics(queries, metric_names=metric_names, last=last))
        return Response(content, status=200, content_type="text/plain")

    return blueprint
