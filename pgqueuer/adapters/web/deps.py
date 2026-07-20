from __future__ import annotations

try:
    from fastapi import Request
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.core.insights import InsightsService, QueueManagementService


def get_insights(request: Request) -> InsightsService:
    """Insights service over the repository stored at ``app.state.pgq_queries``."""
    return InsightsService(request.app.state.pgq_queries)


def get_management(request: Request) -> QueueManagementService:
    """Management service over the repository stored at ``app.state.pgq_queries``."""
    return QueueManagementService(request.app.state.pgq_queries)


def get_broadcaster(request: Request) -> Broadcaster:
    return request.app.state.pgq_broadcaster
