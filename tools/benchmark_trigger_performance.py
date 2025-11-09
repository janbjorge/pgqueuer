#!/usr/bin/env python3
"""
Benchmark script to measure trigger performance improvement.

This script compares the old trigger (which notifies on all updates including heartbeat-only)
versus the new trigger (which filters out heartbeat-only updates).

The key metric is notification overhead - the new trigger should produce significantly fewer
notifications when heartbeat updates are frequent, leading to better performance.

Usage:
    python tools/benchmark_trigger_performance.py --num-jobs 100 --num-heartbeats 1000

This will enqueue 100 jobs and perform 1000 heartbeat updates, comparing notification
counts between the old and new trigger implementations.
"""

from __future__ import annotations

import asyncio
import sys
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

    trigger_type: str  # "old" or "new"
    jobs_created: int
    heartbeat_updates: int
    notifications_received: int
    elapsed_seconds: float
    notification_reduction_pct: float | None = None

    def pretty_print(self) -> None:
        """Print results in a formatted table."""
        rows = [
            ["Trigger Type", self.trigger_type.upper()],
            ["Jobs Created", f"{self.jobs_created:,}"],
            ["Heartbeat Updates", f"{self.heartbeat_updates:,}"],
            ["Notifications Received", f"{self.notifications_received:,}"],
            ["Elapsed Time (s)", f"{self.elapsed_seconds:.3f}"],
        ]
        if self.notification_reduction_pct is not None:
            rows.append(
                [
                    "Notification Reduction",
                    f"{self.notification_reduction_pct:.1f}%",
                ]
            )
        print(
            tabulate(
                rows,
                headers=["Metric", "Value"],
                tablefmt="pretty",
                colalign=("left", "right"),
            )
        )


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
    -- Check operation type and set the emit flag accordingly
    IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
        to_emit := true;
    ELSIF TG_OP = 'DELETE' THEN
        to_emit := true;
    ELSIF TG_OP = 'INSERT' THEN
        to_emit := true;
    ELSIF TG_OP = 'TRUNCATE' THEN
        to_emit := true;
    END IF;

    -- Perform notification if the emit flag is set
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

    -- Return appropriate value based on the operation
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
    print("✓ Installed old trigger (notifies on all updates)")


async def install_new_trigger(queries: Queries) -> None:
    """Install the new trigger that filters heartbeat-only updates."""
    await queries.driver.execute(
        QueryBuilderEnvironment(queries.qbe.settings).build_notify_triggers_reinstall_query()
    )
    print("✓ Installed new trigger (filters heartbeat-only updates)")


async def run_benchmark(
    queries: Queries,
    trigger_type: str,
    num_jobs: int,
    num_heartbeats: int,
) -> BenchmarkResult:
    """
    Run benchmark with the specified trigger.

    This benchmark:
    1. Inserts jobs into the queue
    2. Performs many heartbeat-only updates
    3. Counts how many notifications are sent

    The new trigger should send far fewer notifications since it filters
    heartbeat-only updates.

    Args:
        queries: Database queries instance
        trigger_type: "old" or "new"
        num_jobs: Number of jobs to insert
        num_heartbeats: Number of heartbeat update operations to perform
    """
    print(f"\nRunning benchmark with {trigger_type.upper()} trigger...")

    # Clear the queue
    await queries.clear_queue()

    # Setup notification listener
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
        # Step 1: Insert jobs (this will trigger notifications)
        print(f"  - Inserting {num_jobs} jobs...")
        await queries.enqueue(
            ["benchmark_job"] * num_jobs,
            [f"job_{i}".encode() for i in range(num_jobs)],
            [0] * num_jobs,
        )
        await asyncio.sleep(0.1)  # Let notifications propagate

        # Step 2: Perform heartbeat updates
        print(f"  - Performing {num_heartbeats} heartbeat updates...")
        # Get all job IDs
        result = await queries.driver.fetch(
            f"SELECT id FROM {queries.qbe.settings.queue_table} WHERE status = 'queued'"
        )
        job_ids = [row["id"] for row in result]

        # Perform heartbeat updates in batches
        batch_size = min(10, len(job_ids))
        for i in range(num_heartbeats):
            # Cycle through jobs
            ids_to_update = [job_ids[j % len(job_ids)] for j in range(i, i + batch_size)]
            await queries.update_heartbeat(ids_to_update)

        # Give notifications time to propagate
        await asyncio.sleep(0.2)

    finally:
        # Cleanup listener
        if hasattr(notification_listener, "remove_listener"):
            await notification_listener.remove_listener(
                queries.qbe.settings.channel, notification_callback
            )

    elapsed = time.time() - start_time

    print(f"  ✓ Completed in {elapsed:.3f}s, {notifications_count:,} notifications")

    return BenchmarkResult(
        trigger_type=trigger_type,
        jobs_created=num_jobs,
        heartbeat_updates=num_heartbeats * batch_size,
        notifications_received=notifications_count,
        elapsed_seconds=elapsed,
    )


@app.command()
def main(
    num_jobs: int = typer.Option(
        100,
        help="Number of jobs to insert into the queue",
    ),
    num_heartbeats: int = typer.Option(
        1000,
        help="Number of heartbeat update operations to perform",
    ),
) -> None:
    """
    Benchmark trigger performance comparing old vs new trigger implementations.

    The new trigger filters out heartbeat-only updates, which should significantly
    reduce notification overhead. This benchmark measures that improvement by:

    1. Inserting jobs (triggers notifications with both implementations)
    2. Performing many heartbeat-only updates
    3. Counting notifications sent

    Expected: New trigger sends ~num_jobs notifications (for inserts only)
             Old trigger sends ~num_jobs + num_heartbeats notifications
    """

    async def run_all_benchmarks() -> None:
        import asyncpg

        # Setup database connection
        conn = await asyncpg.connect(dsn=dsn())
        queries = Queries(AsyncpgDriver(conn))

        # Ensure schema is installed
        try:
            await queries.install()
        except Exception:
            pass  # Schema might already exist

        try:
            print("=" * 60)
            print("TRIGGER PERFORMANCE BENCHMARK")
            print("=" * 60)
            print(f"\nConfiguration:")
            print(f"  Jobs to insert: {num_jobs}")
            print(f"  Heartbeat operations: {num_heartbeats}")
            print(f"  Expected heartbeat updates: ~{num_heartbeats * 10}")

            # Benchmark with old trigger
            print("\n" + "-" * 60)
            print("PHASE 1: OLD TRIGGER (notifies on all updates)")
            print("-" * 60)
            await install_old_trigger(queries)
            old_result = await run_benchmark(
                queries,
                "old",
                num_jobs,
                num_heartbeats,
            )

            # Benchmark with new trigger
            print("\n" + "-" * 60)
            print("PHASE 2: NEW TRIGGER (filters heartbeat-only updates)")
            print("-" * 60)
            await install_new_trigger(queries)
            new_result = await run_benchmark(
                queries,
                "new",
                num_jobs,
                num_heartbeats,
            )

            # Calculate improvement
            if old_result.notifications_received > 0:
                reduction = (
                    (old_result.notifications_received - new_result.notifications_received)
                    / old_result.notifications_received
                    * 100
                )
                new_result.notification_reduction_pct = reduction

            # Print results
            print("\n" + "=" * 60)
            print("RESULTS")
            print("=" * 60)

            print("\n--- Old Trigger Results ---")
            old_result.pretty_print()

            print("\n--- New Trigger Results ---")
            new_result.pretty_print()

            print("\n" + "=" * 60)
            print("ANALYSIS")
            print("=" * 60)

            if new_result.notification_reduction_pct is not None:
                print(f"\nNotification Reduction: {new_result.notification_reduction_pct:.1f}%")
                print(
                    f"  • Old trigger: {old_result.notifications_received:,} notifications"
                )
                print(
                    f"  • New trigger: {new_result.notifications_received:,} notifications"
                )
                print(
                    f"  • Saved: {old_result.notifications_received - new_result.notifications_received:,} notifications"
                )

                if new_result.notification_reduction_pct > 70:
                    print(
                        f"\n✅ EXCELLENT! The new trigger reduces notification overhead by "
                        f"{new_result.notification_reduction_pct:.1f}%."
                    )
                    print(
                        "   This significantly improves performance by eliminating unnecessary"
                    )
                    print("   notification spam from heartbeat-only updates.")
                elif new_result.notification_reduction_pct > 40:
                    print(
                        f"\n✅ GOOD! The new trigger reduces notification overhead by "
                        f"{new_result.notification_reduction_pct:.1f}%."
                    )
                    print("   This provides meaningful performance improvement.")
                elif new_result.notification_reduction_pct > 10:
                    print(
                        f"\n✓ The new trigger reduces notification overhead by "
                        f"{new_result.notification_reduction_pct:.1f}%."
                    )
                else:
                    print(
                        f"\nℹ️  The new trigger reduces notification overhead by "
                        f"{new_result.notification_reduction_pct:.1f}%."
                    )
                    print(
                        "   Note: Increase --num-heartbeats for more pronounced difference."
                    )

                print(
                    f"\n💡 In production with frequent heartbeat updates, this reduction"
                )
                print("   translates to less CPU overhead and improved scalability.")

        finally:
            # Restore new trigger
            await install_new_trigger(queries)
            await conn.close()

    asyncio.run(run_all_benchmarks())


if __name__ == "__main__":
    app()
