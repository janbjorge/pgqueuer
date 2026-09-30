"""Lock the fields each MCP tool returns, as seen by a real MCP client.

The other MCP tests run the SQL builders directly. These call the tools
through the server, so a change to what a tool returns shows up here.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from pgqueuer.adapters.mcp.server import create_mcp_server
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import CronExpressionEntrypoint
from pgqueuer.domain.types import CronEntrypoint, CronExpression, QueueEntrypoint, QueueManagerId
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries


async def seed(driver: AsyncpgDriver) -> None:
    """Put at least one row behind every tool."""
    q = Queries(driver)
    for _ in range(3):
        await q.enqueue("ep", b"x")
    picked = await q.dequeue(
        2,
        {QueueEntrypoint("ep"): EntrypointExecutionParameter(0)},
        QueueManagerId(uuid.uuid4()),
        None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    await q.log_jobs([(picked[0], "exception", None)])
    await driver.execute(
        f"UPDATE {q.qbq.settings.queue_table} SET heartbeat = NOW() - interval '1 hour'"
    )
    await q.aggregate_logs()
    await q.insert_schedule(
        {
            CronExpressionEntrypoint(
                CronEntrypoint("cron"), CronExpression("* * * * *")
            ): timedelta(0)
        }
    )


async def call(dsn: str, tool: str) -> list[dict[str, object]]:
    server = create_mcp_server(dsn=dsn)
    async with create_connected_server_and_client_session(server, raise_exceptions=True) as client:
        result = await client.call_tool(tool, {})
    assert not result.isError, result.content
    assert result.structuredContent is not None
    rows = result.structuredContent["result"]
    assert isinstance(rows, list)
    return rows


class TestMcpToolOutput:
    log_fields = {
        "aggregated",
        "created",
        "entrypoint",
        "id",
        "job_id",
        "priority",
        "status",
        "traceback",
    }

    fields = {
        "queue_size": {"count", "entrypoint", "priority", "status"},
        "queue_table_info": {
            "attempts",
            "created",
            "dedupe_key",
            "entrypoint",
            "execute_after",
            "headers",
            "heartbeat",
            "id",
            "payload",
            "priority",
            "queue_manager_id",
            "slot",
            "status",
            "updated",
        },
        "queue_stats": {"count", "created", "entrypoint", "priority", "status"},
        "throughput_summary": {"entrypoint", "status", "total_count"},
        "failed_jobs": log_fields,
        "queue_log": log_fields,
        "schedules": {
            "created",
            "entrypoint",
            "expression",
            "heartbeat",
            "id",
            "last_run",
            "next_run",
            "status",
            "updated",
        },
        "stale_jobs": {
            "created",
            "entrypoint",
            "execute_after",
            "heartbeat",
            "id",
            "priority",
            "queue_manager_id",
            "seconds_since_heartbeat",
            "status",
            "updated",
        },
        "active_workers": {
            "active_jobs",
            "entrypoints",
            "newest_heartbeat",
            "oldest_heartbeat",
            "queue_manager_id",
        },
        "queue_age": {
            "avg_age_seconds",
            "entrypoint",
            "oldest_age_seconds",
            "oldest_created",
            "queued_count",
        },
        "schema_info": {"estimated_rows", "persistence", "table_name", "total_size"},
    }

    def test_every_tool_is_locked(self) -> None:
        server = create_mcp_server(dsn="postgresql://localhost/test")
        assert {t.name for t in server._tool_manager.list_tools()} == set(self.fields)

    @pytest.mark.parametrize("tool", sorted(fields))
    async def test_tool_returns_locked_fields(
        self, tool: str, dsn: str, apgdriver: AsyncpgDriver
    ) -> None:
        await seed(apgdriver)

        rows = await call(dsn, tool)

        assert rows, f"{tool} returned no rows; seed() must cover it"
        for row in rows:
            assert set(row) == self.fields[tool]
