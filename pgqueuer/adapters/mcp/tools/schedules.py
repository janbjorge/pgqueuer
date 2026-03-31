from __future__ import annotations

import json

from mcp.server.fastmcp import Context, FastMCP

from pgqueuer.adapters.mcp.server import _get_queries
from pgqueuer.domain.types import CronEntrypoint, ScheduleId


def register(server: FastMCP) -> None:
    @server.tool()
    async def list_schedules(ctx: Context) -> str:
        """List all cron schedules registered in PgQueuer."""
        q = _get_queries(ctx)
        scheds = await q.peak_schedule()
        return json.dumps([s.model_dump(mode="json") for s in scheds], default=str)

    @server.tool()
    async def delete_schedule(
        ctx: Context,
        schedule_ids: list[int] | None = None,
        entrypoint_names: list[str] | None = None,
    ) -> str:
        """Delete schedules by ID or entrypoint name.

        Args:
            schedule_ids: List of schedule IDs to delete.
            entrypoint_names: List of entrypoint names whose schedules should be deleted.
        """
        q = _get_queries(ctx)
        ids = {ScheduleId(i) for i in (schedule_ids or [])}
        names = {CronEntrypoint(n) for n in (entrypoint_names or [])}
        await q.delete_schedule(ids, names)
        return json.dumps({"deleted_ids": [int(i) for i in ids], "deleted_names": list(names)})
