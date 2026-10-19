from __future__ import annotations

try:
    from fastapi import Request
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.core.insights import InsightsService, QueueManagementService


@asynccontextmanager
async def request_queries(request: Request) -> AsyncIterator[Queries]:
    """``app.state.pgq_queries``, or Queries over a connection borrowed from
    ``app.state.pgq_pool`` for this request when the app keeps a pool."""
    pool = getattr(request.app.state, "pgq_pool", None)
    if pool is None:
        yield request.app.state.pgq_queries
        return
    async with pool.acquire() as connection:
        yield Queries(AsyncpgDriver(connection), request.app.state.pgq_settings)


async def get_insights(request: Request) -> AsyncIterator[InsightsService]:
    async with request_queries(request) as queries:
        yield InsightsService(queries)


async def get_management(request: Request) -> AsyncIterator[QueueManagementService]:
    async with request_queries(request) as queries:
        yield QueueManagementService(queries)


def get_broadcaster(request: Request) -> Broadcaster:
    broadcaster: object = request.app.state.pgq_broadcaster
    if not isinstance(broadcaster, Broadcaster):
        raise RuntimeError("app.state.pgq_broadcaster is not a Broadcaster")
    return broadcaster
