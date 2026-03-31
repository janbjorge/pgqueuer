from __future__ import annotations

from mcp.server.fastmcp import Context, FastMCP

from pgqueuer.adapters.mcp.server import _get_queries


def register(server: FastMCP) -> None:
    @server.tool()
    async def queue_size(ctx: Context) -> str:
        """Get current queue depth grouped by entrypoint, status, and priority."""
        q = _get_queries(ctx)
        stats = await q.queue_size()
        return _to_json([s.model_dump() for s in stats])

    @server.tool()
    async def list_jobs(
        ctx: Context,
        entrypoint: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> str:
        """List jobs in the queue with optional filters.

        Args:
            entrypoint: Filter by entrypoint name.
            status: Filter by status (queued, picked, successful, exception, canceled, deleted).
            limit: Maximum number of jobs to return (default 50, max 500).
        """
        q = _get_queries(ctx)
        clamped = min(max(limit, 1), 500)
        jobs = await q.list_jobs(entrypoint=entrypoint, status=status, limit=clamped)
        return _to_json([j.model_dump(mode="json") for j in jobs])

    @server.tool()
    async def get_job(ctx: Context, job_id: int) -> str:
        """Get a specific job by ID.

        Args:
            job_id: The job ID to look up.
        """
        from pgqueuer.domain.types import JobId

        q = _get_queries(ctx)
        job = await q.get_job(JobId(job_id))
        if job is None:
            return '{"error": "Job not found"}'
        return _to_json(job.model_dump(mode="json"))


def _to_json(obj: object) -> str:
    import json

    return json.dumps(obj, default=str)
