"""Tests for the PgQueuer MCP server."""

from __future__ import annotations

from datetime import timedelta

import pytest

from pgqueuer.adapters.mcp.server import (
    PgQueuerDatabase,
    _parse_interval,
    create_mcp_server,
)
from pgqueuer.adapters.persistence.qb import DBSettings
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries


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
        server = create_mcp_server(dsn="postgresql://localhost/test")
        assert server.name == "pgqueuer"

    def test_factory_with_custom_settings(self) -> None:
        settings = DBSettings()
        server = create_mcp_server(dsn="postgresql://localhost/test", settings=settings)
        assert server.name == "pgqueuer"

    def test_factory_registers_all_tools(self) -> None:
        server = create_mcp_server(dsn="postgresql://localhost/test")
        tool_names = {t.name for t in server._tool_manager.list_tools()}
        expected = {
            "queue_size",
            "queue_table_info",
            "queue_stats",
            "throughput_summary",
            "failed_jobs",
            "queue_log",
            "schedules",
            "stale_jobs",
            "active_workers",
            "queue_age",
            "schema_info",
        }
        assert expected == tool_names


class TestPgQueuerDatabase:
    async def test_fetch_returns_dicts(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch("SELECT 1 AS val")
            assert rows == [{"val": 1}]


class TestMcpToolsIntegration:
    """Integration tests that exercise the static queries against a real PgQueuer DB."""

    async def _make_db(self, dsn: str) -> tuple[PgQueuerDatabase, object]:
        import asyncpg

        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
        return PgQueuerDatabase(pool, DBSettings()), pool

    async def test_queue_size_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_size_query())
            assert rows == []

    async def test_queue_size_with_jobs(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("test_ep", b"payload", priority=0)
        await q.enqueue("test_ep", b"payload2", priority=1)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_size_query())
            assert len(rows) >= 1
            counts = [r["count"] for r in rows]
            assert all(isinstance(c, int) for c in counts)
            assert sum(counts) == 2  # type: ignore[arg-type]

    async def test_queue_table_browse(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("browse_ep", b"data", priority=5)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_table_browse_query(), 10, 0)
            assert len(rows) >= 1
            assert any(r["entrypoint"] == "browse_ep" for r in rows)

    async def test_queue_log_after_enqueue(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("log_test", b"data", priority=0)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_log_query(), 100)
            assert len(rows) >= 1
            assert any(r["entrypoint"] == "log_test" for r in rows)

    async def test_failed_jobs_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_failed_jobs_query(), 50)
            assert rows == []

    async def test_schedules_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbs.build_peak_schedule_query())
            assert rows == []

    async def test_stats_aggregation(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("stats_ep", b"x", priority=0)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            await db.fetch(db.qbq.build_aggregate_log_data_to_statistics_query())
            rows = await db.fetch(db.qbq.build_log_statistics_query(), 50, None)
            assert len(rows) >= 1

    async def test_stale_jobs_none_when_fresh(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("stale_test", b"x", priority=0)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_stale_jobs_query(), timedelta(minutes=5), 50)
            assert rows == []

    async def test_active_workers_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_active_workers_query())
            assert rows == []

    async def test_queue_age_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_age_query())
            assert rows == []

    async def test_queue_age_with_queued_jobs(self, dsn: str, apgdriver: AsyncpgDriver) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("age_test", b"x", priority=0)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_queue_age_query())
            assert len(rows) == 1
            assert rows[0]["entrypoint"] == "age_test"
            assert rows[0]["queued_count"] == 1

    async def test_throughput_summary_empty(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_throughput_summary_query(), None)
            assert rows == []

    async def test_throughput_summary_after_enqueue(
        self, dsn: str, apgdriver: AsyncpgDriver
    ) -> None:
        import asyncpg

        q = Queries(apgdriver)
        await q.enqueue("tp_test", b"x", priority=0)

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            await db.fetch(db.qbq.build_aggregate_log_data_to_statistics_query())
            rows = await db.fetch(db.qbq.build_throughput_summary_query(), None)
            assert len(rows) >= 1
            assert any(r["entrypoint"] == "tp_test" for r in rows)

    async def test_schema_info(self, dsn: str) -> None:
        import asyncpg

        async with asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2) as pool:
            db = PgQueuerDatabase(pool, DBSettings())
            rows = await db.fetch(db.qbq.build_schema_info_query())
            table_names = {r["table_name"] for r in rows}
            assert "pgqueuer" in table_names
            assert "pgqueuer_log" in table_names
            assert "pgqueuer_statistics" in table_names
            assert "pgqueuer_schedules" in table_names
