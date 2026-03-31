from __future__ import annotations

import json

from mcp.server.fastmcp import Context, FastMCP

from pgqueuer.adapters.mcp.server import _get_queries
from pgqueuer.adapters.persistence.qb import DBSettings


def register(server: FastMCP) -> None:
    @server.tool()
    async def verify_schema(ctx: Context) -> str:
        """Check if the PgQueuer database schema is installed correctly."""
        q = _get_queries(ctx)
        settings = q.qbe.settings

        tables = {
            settings.queue_table: await q.has_table(settings.queue_table),
            settings.statistics_table: await q.has_table(settings.statistics_table),
            settings.schedules_table: await q.has_table(settings.schedules_table),
            settings.queue_table_log: await q.has_table(settings.queue_table_log),
        }
        function_ok = await q.has_function(settings.function)
        trigger_ok = await q.has_trigger(settings.trigger)

        divergence = []
        for table, exists in tables.items():
            if not exists:
                divergence.append(f"missing table '{table}'")
        if not function_ok:
            divergence.append(f"missing function '{settings.function}'")
        if not trigger_ok:
            divergence.append(f"missing trigger '{settings.trigger}'")

        return json.dumps({
            "installed": len(divergence) == 0,
            "tables": tables,
            "function": function_ok,
            "trigger": trigger_ok,
            "divergence": divergence,
        })

    @server.tool()
    async def get_schema_info(ctx: Context) -> str:
        """Get PgQueuer schema configuration (table names, channel, etc.)."""
        settings = DBSettings()
        return json.dumps({
            "queue_table": settings.queue_table,
            "queue_table_log": settings.queue_table_log,
            "statistics_table": settings.statistics_table,
            "schedules_table": settings.schedules_table,
            "channel": settings.channel,
            "function": settings.function,
            "trigger": settings.trigger,
            "queue_status_type": settings.queue_status_type,
        })
