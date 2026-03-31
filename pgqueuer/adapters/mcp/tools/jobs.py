from __future__ import annotations

import json
from datetime import timedelta

from mcp.server.fastmcp import Context, FastMCP

from pgqueuer.adapters.mcp.server import _get_queries
from pgqueuer.domain.types import JobId


def register(server: FastMCP) -> None:
    @server.tool()
    async def enqueue_job(
        ctx: Context,
        entrypoint: str,
        payload: str | None = None,
        priority: int = 0,
        execute_after_seconds: float = 0,
    ) -> str:
        """Enqueue a new job into the queue.

        Args:
            entrypoint: The entrypoint (handler) name for the job.
            payload: Optional payload string (will be UTF-8 encoded).
            priority: Job priority (higher = processed first, default 0).
            execute_after_seconds: Delay in seconds before the job becomes eligible (default 0).
        """
        q = _get_queries(ctx)
        payload_bytes = payload.encode("utf-8") if payload else None
        execute_after = timedelta(seconds=execute_after_seconds) if execute_after_seconds else None
        ids = await q.enqueue(
            entrypoint=entrypoint,
            payload=payload_bytes,
            priority=priority,
            execute_after=execute_after,
        )
        return json.dumps({"job_ids": [int(i) for i in ids]})

    @server.tool()
    async def cancel_jobs(ctx: Context, job_ids: list[int]) -> str:
        """Cancel specific jobs by their IDs.

        Args:
            job_ids: List of job IDs to cancel.
        """
        q = _get_queries(ctx)
        typed_ids = [JobId(i) for i in job_ids]
        await q.mark_job_as_cancelled(typed_ids)
        return json.dumps({"cancelled": len(typed_ids)})
