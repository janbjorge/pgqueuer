from __future__ import annotations

try:
    from fastapi import Request
except ImportError as e:
    raise ImportError(
        "fastapi is required for this module. Install with: pip install pgqueuer[web]"
    ) from e

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.adapters.web.sse import Broadcaster
from pgqueuer.core.insights import InsightsService, QueueManagementService
from pgqueuer.domain.settings import DBSettings
from pgqueuer.ports.repository import InsightsRepositoryPort, QueueRepositoryPort

if TYPE_CHECKING:
    import asyncpg


class DashboardRepository(InsightsRepositoryPort, QueueRepositoryPort, Protocol):
    """What the dashboard routes read and act through."""


@dataclass(frozen=True)
class PooledQueries:
    """Queries over a connection borrowed from *pool* for each request."""

    pool: asyncpg.Pool
    settings: DBSettings


@asynccontextmanager
async def request_queries(request: Request) -> AsyncIterator[DashboardRepository]:
    """The repository at ``app.state.pgq_queries``, borrowing a connection when it is pooled."""
    source = request.app.state.pgq_queries
    if isinstance(source, PooledQueries):
        async with source.pool.acquire() as connection:
            yield Queries(AsyncpgDriver(connection), source.settings)
    else:
        yield source


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
