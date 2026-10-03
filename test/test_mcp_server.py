"""Tests for the PgQueuer MCP server."""

from __future__ import annotations

from datetime import timedelta
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from pgqueuer.adapters.mcp.server import (
    PgQueuerDatabase,
    _parse_interval,
    create_mcp_server,
)
from pgqueuer.adapters.persistence.qb import DBSettings
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import ConnectionSettings
from pgqueuer.queries import Queries


@pytest_asyncio.fixture(scope="function")
async def mcpdb(dsn: str) -> AsyncGenerator[PgQueuerDatabase, None]:
    async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
        yield PgQueuerDatabase(pool, DBSettings())


class TestParseInterval:
    def test_none_returns_none(self) -> None:
        assert _parse_interval(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_interval("") is None

    def test_one_hour(self) -> None:
        assert _parse_interval("PT1H") == timedelta(hours=1)

    def test_thirty_minutes(self) -> None:
        assert _parse_interval("PT30M") == timedelta(minutes=30)

    def test_one_day(self) -> None:
        assert _parse_interval("P1D") == timedelta(days=1)

    def test_seven_days(self) -> None:
        assert _parse_interval("P7D") == timedelta(days=7)

    def test_complex_duration(self) -> None:
        assert _parse_interval("P2DT3H30M15S") == timedelta(days=2, hours=3, minutes=30, seconds=15)

    def test_case_insensitive(self) -> None:
        assert _parse_interval("pt1h") == timedelta(hours=1)

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot parse duration"):
            _parse_interval("not-a-duration")

    def test_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="Duration must be positive"):
            _parse_interval("PT0S")


class TestCreateMcpServer:
    def test_factory_returns_fastmcp(self) -> None:
        server = create_mcp_server(
            dsn="postgresql://localhost/test",
            connection_settings=ConnectionSettings(pool_min_size=1, pool_max_size=2),
        )
        assert server.name == "pgqueuer"


class TestPgQueuerDatabase:
    async def test_fetch_returns_dicts(self, mcpdb: PgQueuerDatabase) -> None:
        rows = await mcpdb.fetch("SELECT 1 AS val")
        assert rows == [{"val": 1}]


class TestMcpToolsIntegration:
    """Integration tests that exercise the static queries against a real PgQueuer DB."""

    async def test_queue_table_browse(
        self, mcpdb: PgQueuerDatabase, apgdriver: AsyncpgDriver
    ) -> None:
        q = Queries(apgdriver)
        await q.enqueue("browse_ep", b"data", priority=5)

        rows = await mcpdb.fetch(mcpdb.qbq.build_queue_table_browse_query(), 10, 0)
        assert len(rows) >= 1
        assert any(r["entrypoint"] == "browse_ep" for r in rows)

    async def test_queue_log_after_enqueue(
        self, mcpdb: PgQueuerDatabase, apgdriver: AsyncpgDriver
    ) -> None:
        q = Queries(apgdriver)
        await q.enqueue("log_test", b"data", priority=0)

        rows = await mcpdb.fetch(mcpdb.qbq.build_queue_log_query(), 100)
        assert len(rows) >= 1
        assert any(r["entrypoint"] == "log_test" for r in rows)

    async def test_failed_jobs_empty(self, mcpdb: PgQueuerDatabase) -> None:
        rows = await mcpdb.fetch(mcpdb.qbq.build_failed_jobs_query(), 50)
        assert rows == []

    async def test_stats_aggregation(
        self, mcpdb: PgQueuerDatabase, apgdriver: AsyncpgDriver
    ) -> None:
        q = Queries(apgdriver)
        await q.enqueue("stats_ep", b"x", priority=0)

        await mcpdb.fetch(mcpdb.qbq.build_aggregate_log_data_to_statistics_query())
        stats_query = mcpdb.qbq.build_log_statistics_query(limit=50, last=None)
        rows = await mcpdb.fetch(stats_query.sql, *stats_query.args)
        assert len(rows) >= 1

    async def test_throughput_summary_empty(self, mcpdb: PgQueuerDatabase) -> None:
        rows = await mcpdb.fetch(mcpdb.qbq.build_throughput_summary_query(), None)
        assert rows == []

    async def test_throughput_summary_after_enqueue(
        self, mcpdb: PgQueuerDatabase, apgdriver: AsyncpgDriver
    ) -> None:
        q = Queries(apgdriver)
        await q.enqueue("tp_test", b"x", priority=0)

        await mcpdb.fetch(mcpdb.qbq.build_aggregate_log_data_to_statistics_query())
        rows = await mcpdb.fetch(mcpdb.qbq.build_throughput_summary_query(), None)
        assert len(rows) >= 1
        assert any(r["entrypoint"] == "tp_test" for r in rows)
