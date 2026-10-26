"""Regression tests for defects found in the October 2026 code audit.

Each test pins one defect and names its issue. Until the fix lands the test
is a strict xfail, so a fix that makes it pass fails the run until the
marker is removed.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import timedelta
from typing import cast

import asyncpg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.inmemory.jobs import job_status_rank
from pgqueuer.adapters.mcp.server import create_mcp_server
from pgqueuer.core.buffers import JobLogEntry
from pgqueuer.core.insights import sparkline_buckets
from pgqueuer.core.qm import QueueManager
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain import models
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import (
    JOB_STATUS,
    QueueEntrypoint,
    QueueExecutionMode,
    QueueManagerId,
)
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries


def known_bug(issue: str) -> pytest.MarkDecorator:
    return pytest.mark.xfail(strict=True, reason=f"open defect, see {issue}")


class LogTableDown(InMemoryQueries):
    """In-memory queries whose job-log writes always fail."""

    async def log_jobs(self, job_status: list[JobLogEntry]) -> None:
        raise RuntimeError("log table down")


async def test_queue_manager_keeps_job_ends_it_could_not_write() -> None:
    """A job end that never reached the log is left on the manager for the supervisor."""
    queries = LogTableDown(driver=InMemoryDriver())
    qm = QueueManager(queries)

    @qm.entrypoint("ep")
    async def ep(job: models.Job) -> None:
        return None

    (job_id,) = await queries.enqueue("ep", None)
    await asyncio.wait_for(
        qm.run(mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(seconds=0.1)),
        timeout=10,
    )

    assert [(job.id, status) for job, status, _ in qm.unwritten_job_logs] == [
        (job_id, "successful")
    ]


async def test_mcp_throughput_summary_counts_logs_not_yet_aggregated(
    dsn: str, apgdriver: AsyncpgDriver
) -> None:
    """The MCP tool sees the same totals as Queries.throughput_summary."""
    await Queries(apgdriver).enqueue("tp", None)

    server = create_mcp_server(dsn=dsn)
    async with create_connected_server_and_client_session(server, raise_exceptions=True) as client:
        result = await client.call_tool("throughput_summary", {})
    assert not result.isError, result.content
    assert result.structuredContent is not None
    rows = result.structuredContent["result"]

    assert [(r["entrypoint"], r["status"], int(r["total_count"])) for r in rows] == [
        ("tp", "queued", 1)
    ]


@known_bug("#1044")
async def test_eligible_probe_and_dequeue_agree_at_execute_after_now(dsn: str) -> None:
    """A job due exactly at NOW() is either eligible and dequeued, or neither."""
    connection = await asyncpg.connect(dsn)
    try:
        queries = Queries(AsyncpgDriver(connection))
        # NOW() is fixed for the whole transaction, so execute_after == NOW() holds.
        async with connection.transaction():
            await queries.enqueue("ep", None)
            eligible = await queries.eligible_queued_work([QueueEntrypoint("ep")])
            picked = await queries.dequeue(
                10,
                {QueueEntrypoint("ep"): EntrypointExecutionParameter(0)},
                QueueManagerId(uuid.uuid4()),
                None,
                heartbeat_timeout=timedelta(seconds=30),
            )
            assert (eligible > 0) == (len(picked) > 0)
    finally:
        await connection.close()


@known_bug("#1045")
async def test_requeue_of_a_failed_job_notifies_listeners(
    dsn: str, apgdriver: AsyncpgDriver
) -> None:
    """Requeued work is due now, so an idle worker must hear about it."""
    queries = Queries(apgdriver)
    channel = queries.qbq.settings.channel
    operations: asyncio.Queue[str] = asyncio.Queue()

    def on_notify(_conn: object, _pid: int, _channel: str, payload: object) -> None:
        assert isinstance(payload, str)
        operations.put_nowait(json.loads(payload)["operation"])

    async def heard_until_marker(marker: str) -> list[str]:
        # Notifications arrive in commit order, so a marker sent after a
        # statement arrives after everything that statement emitted.
        await apgdriver.execute(
            "SELECT pg_notify($1, $2)", channel, json.dumps({"operation": marker})
        )
        heard: list[str] = []
        while (operation := await asyncio.wait_for(operations.get(), timeout=5)) != marker:
            heard.append(operation)
        return heard

    listener = await asyncpg.connect(dsn)
    try:
        await listener.add_listener(channel, on_notify)
        await queries.enqueue("ep", None)
        # The listener works: the enqueue INSERT is heard.
        assert await heard_until_marker("after-enqueue") == ["insert"]

        (job,) = await queries.dequeue(
            10,
            {QueueEntrypoint("ep"): EntrypointExecutionParameter(0)},
            QueueManagerId(uuid.uuid4()),
            None,
            heartbeat_timeout=timedelta(seconds=30),
        )
        await queries.log_jobs([(job, "failed", None)])
        await heard_until_marker("before-requeue")

        await queries.requeue_jobs([job.id])

        assert await heard_until_marker("after-requeue") == ["update"]
    finally:
        await listener.close()


def test_sparkline_window_ends_now_like_the_overview_chart() -> None:
    """A burst 45 minutes ago sits near the start of the last hour, not at its end."""
    # 45 minutes puts the burst mid-slot; on a slot edge the clock read inside
    # sparkline_buckets, a moment later, would tip it into the slot before.
    burst = models.ThroughputBucket(
        bucket=models.utc_now() - timedelta(minutes=45),
        entrypoint=QueueEntrypoint("ep"),
        status="successful",
        count=3,
    )

    buckets = sparkline_buckets([burst], "ep", timedelta(hours=1), slots=6)

    assert buckets == [0, 3, 0, 0, 0, 0]


@known_bug("#1047")
def test_inmemory_status_rank_follows_the_postgres_enum() -> None:
    """In-memory sorts statuses the way PostgreSQL sorts pgqueuer_status."""
    (status_enum,) = target(DBSettings()).enums

    labels = cast(list[JOB_STATUS], list(status_enum.labels))

    assert sorted(labels, key=job_status_rank) == labels
