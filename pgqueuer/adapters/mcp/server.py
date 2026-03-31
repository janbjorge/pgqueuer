from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncIterator
from typing import Any

from mcp.server.fastmcp import FastMCP

from pgqueuer.adapters.drivers import dsn
from pgqueuer.adapters.persistence import qb, queries
from pgqueuer.core.helpers import add_schema_to_dsn


async def _create_queries(dsn_str: str) -> queries.Queries:
    settings = qb.DBSettings()
    with contextlib.suppress(ImportError):
        import asyncpg

        from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver

        return queries.Queries(
            AsyncpgDriver(await asyncpg.connect(dsn=dsn_str)),
            qbe=qb.QueryBuilderEnvironment(settings),
            qbq=qb.QueryQueueBuilder(settings),
            qbs=qb.QuerySchedulerBuilder(settings),
        )
    with contextlib.suppress(ImportError):
        import psycopg

        from pgqueuer.adapters.drivers.psycopg import PsycopgDriver

        return queries.Queries(
            PsycopgDriver(await psycopg.AsyncConnection.connect(dsn_str, autocommit=True)),
            qbe=qb.QueryBuilderEnvironment(settings),
            qbq=qb.QueryQueueBuilder(settings),
            qbs=qb.QuerySchedulerBuilder(settings),
        )
    raise RuntimeError("Neither asyncpg nor psycopg could be imported.")


def _resolve_dsn() -> str:
    dsn_str = os.environ.get("PGDSN", "") or dsn()
    schema = os.environ.get("PGSCHEMA", "")
    if schema:
        dsn_str = add_schema_to_dsn(dsn_str, schema)
    return dsn_str


@contextlib.asynccontextmanager
async def _lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    dsn_str = _resolve_dsn()
    q = await _create_queries(dsn_str)
    try:
        yield {"queries": q}
    finally:
        q.driver.shutdown.set()


def _get_queries(ctx: Any) -> queries.Queries:
    return ctx.request_context.lifespan_context["queries"]


def create_mcp_server() -> FastMCP:
    server = FastMCP(
        "pgqueuer",
        instructions=(
            "PgQueuer MCP server for managing and monitoring PostgreSQL-backed job queues. "
            "Use these tools to inspect queue status, manage jobs, view schedules, "
            "and diagnose errors."
        ),
        lifespan=_lifespan,
    )

    # Import tool modules to register tools on the server
    from pgqueuer.adapters.mcp.tools import jobs, queue, schedules, schema, statistics

    jobs.register(server)
    queue.register(server)
    schedules.register(server)
    schema.register(server)
    statistics.register(server)

    return server
