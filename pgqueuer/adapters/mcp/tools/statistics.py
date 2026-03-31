from __future__ import annotations

import json
from datetime import timedelta

from mcp.server.fastmcp import Context, FastMCP

from pgqueuer.adapters.mcp.server import _get_queries


def register(server: FastMCP) -> None:
    @server.tool()
    async def queue_statistics(
        ctx: Context,
        tail: int = 50,
        last_seconds: int | None = None,
    ) -> str:
        """Get aggregated job processing statistics over time.

        Args:
            tail: Number of recent entries to return (default 50).
            last_seconds: Only include entries from the last N seconds.
        """
        q = _get_queries(ctx)
        last = timedelta(seconds=last_seconds) if last_seconds is not None else None
        stats = await q.log_statistics(tail=tail, last=last)
        return json.dumps([s.model_dump(mode="json") for s in stats], default=str)

    @server.tool()
    async def error_log(
        ctx: Context,
        limit: int = 20,
        entrypoint: str | None = None,
    ) -> str:
        """Get recent job errors with tracebacks.

        Args:
            limit: Maximum number of error entries to return (default 20, max 500).
            entrypoint: Filter by entrypoint name.
        """
        q = _get_queries(ctx)
        clamped = min(max(limit, 1), 500)
        errors = await q.error_log(limit=clamped, entrypoint=entrypoint)
        return json.dumps([e.model_dump(mode="json") for e in errors], default=str)
