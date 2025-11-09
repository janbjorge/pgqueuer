#!/usr/bin/env python3
"""Benchmark script to measure trigger performance improvement."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import typer
from tabulate import tabulate

from pgqueuer.db import AsyncpgDriver, dsn
from pgqueuer.qb import QueryBuilderEnvironment
from pgqueuer.queries import Queries

app = typer.Typer(add_completion=False)


@dataclass
class BenchmarkResult:
    """Results from a trigger performance benchmark."""

    trigger_type: str
    jobs_created: int
    heartbeat_updates: int
    notifications_received: int
    elapsed_seconds: float


async def install_old_trigger(queries: Queries) -> None:
    """Install the old trigger that notifies on all updates."""
    settings = queries.qbe.settings

    old_trigger_sql = f"""
DROP TRIGGER IF EXISTS {settings.trigger}_truncate ON {settings.queue_table};
DROP TRIGGER IF EXISTS {settings.trigger} ON {settings.queue_table};

CREATE OR REPLACE FUNCTION {settings.function}() RETURNS TRIGGER AS $$
DECLARE
    to_emit BOOLEAN := false;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
        to_emit := true;
    ELSIF TG_OP IN ('DELETE', 'INSERT', 'TRUNCATE') THEN
        to_emit := true;
    END IF;

    IF to_emit THEN
        PERFORM pg_notify(
            '{settings.channel}',
            json_build_object(
                'channel', '{settings.channel}',
                'operation', lower(TG_OP),
                'sent_at', NOW(),
                'table', TG_TABLE_NAME,
                'type', 'table_changed_event'
            )::text
        );
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER {settings.trigger}
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {settings.queue_table}
EXECUTE FUNCTION {settings.function}();
"""
    await queries.driver.execute(old_trigger_sql)


async def install_new_trigger(queries: Queries) -> None:
    """Install the new trigger that filters heartbeat-only updates."""
    await queries.driver.execute(
        QueryBuilderEnvironment(queries.qbe.settings).build_notify_triggers_reinstall_query()
    )


async def run_benchmark(
    queries: Queries,
    trigger_type: str,
    num_jobs: int,
    num_heartbeats: int,
) -> BenchmarkResult:
    """Run benchmark with the specified trigger."""
    await queries.clear_queue()

    notifications_count = 0
    notification_listener = queries.driver.conn  # type: ignore

    def notification_callback(*args: object) -> None:
        nonlocal notifications_count
        notifications_count += 1

    if hasattr(notification_listener, "add_listener"):
        await notification_listener.add_listener(
            queries.qbe.settings.channel, notification_callback
        )

    start_time = time.time()

    try:
        # Insert jobs
        await queries.enqueue(
            ["benchmark_job"] * num_jobs,
            [f"job_{i}".encode() for i in range(num_jobs)],
            [0] * num_jobs,
        )
        await asyncio.sleep(0.1)

        # Perform heartbeat updates
        result = await queries.driver.fetch(
            f"SELECT id FROM {queries.qbe.settings.queue_table} WHERE status = 'queued'"
        )
        job_ids = [row["id"] for row in result]

        batch_size = min(10, len(job_ids))
        for i in range(num_heartbeats):
            ids_to_update = [job_ids[j % len(job_ids)] for j in range(i, i + batch_size)]
            await queries.update_heartbeat(ids_to_update)

        await asyncio.sleep(0.2)

    finally:
        if hasattr(notification_listener, "remove_listener"):
            await notification_listener.remove_listener(
                queries.qbe.settings.channel, notification_callback
            )

    elapsed = time.time() - start_time

    return BenchmarkResult(
        trigger_type=trigger_type,
        jobs_created=num_jobs,
        heartbeat_updates=num_heartbeats * batch_size,
        notifications_received=notifications_count,
        elapsed_seconds=elapsed,
    )


@app.command()
def main(
    num_jobs: int = typer.Option(100, help="Number of jobs to insert"),
    num_heartbeats: int = typer.Option(1000, help="Number of heartbeat operations"),
) -> None:
    """Benchmark trigger performance: old vs new trigger."""

    async def run_all_benchmarks() -> None:
        import asyncpg

        conn = await asyncpg.connect(dsn=dsn())
        queries = Queries(AsyncpgDriver(conn))

        try:
            await queries.install()
        except Exception:
            pass

        try:
            # Benchmark old trigger
            await install_old_trigger(queries)
            old_result = await run_benchmark(queries, "old", num_jobs, num_heartbeats)

            # Benchmark new trigger
            await install_new_trigger(queries)
            new_result = await run_benchmark(queries, "new", num_jobs, num_heartbeats)

            # Display results
            reduction = (
                (old_result.notifications_received - new_result.notifications_received)
                / old_result.notifications_received
                * 100
                if old_result.notifications_received > 0
                else 0
            )

            print(
                "\n"
                + tabulate(
                    [
                        [
                            "Old Trigger",
                            old_result.jobs_created,
                            old_result.heartbeat_updates,
                            old_result.notifications_received,
                            f"{old_result.elapsed_seconds:.3f}s",
                        ],
                        [
                            "New Trigger",
                            new_result.jobs_created,
                            new_result.heartbeat_updates,
                            new_result.notifications_received,
                            f"{new_result.elapsed_seconds:.3f}s",
                        ],
                        [
                            "Reduction",
                            "-",
                            "-",
                            f"{reduction:.1f}%",
                            "-",
                        ],
                    ],
                    headers=["Trigger", "Jobs", "Heartbeats", "Notifications", "Time"],
                    tablefmt="grid",
                )
            )

        finally:
            await install_new_trigger(queries)
            await conn.close()

    asyncio.run(run_all_benchmarks())


if __name__ == "__main__":
    app()
