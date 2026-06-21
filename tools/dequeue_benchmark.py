"""Compare old global-scan dequeue against the new per-entrypoint LATERAL (#668).

Seeds a large multi-entrypoint backlog, then measures both queries with
EXPLAIN (ANALYZE, BUFFERS) inside a rolled-back transaction so each run sees
identical state. Reports median execution time, buffers, and the queue-table
scan node. Connection comes from standard PG env vars (PGHOST/PGPORT/...).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import uuid
from datetime import timedelta

import asyncpg

from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.db import AsyncpgDriver

NEW_START = "-- Entrypoints with free capacity"
NEXT_STALE = "-- Stale picked jobs whose heartbeat"


def old_dequeue_query() -> str:
    """Reconstruct the pre-#668 dequeue (global JOIN scan) by swapping the
    LATERAL block back to a single global next_queued CTE."""
    new = qb.QueryQueueBuilder().build_dequeue_query()
    t = qb.DBSettings().queue_table
    old_block = f"""-- New queued jobs (uses partial index WHERE status = 'queued').
next_queued AS (
    SELECT q.id
    FROM {t} q
    JOIN      params p  ON p.entrypoint  = q.entrypoint
    LEFT JOIN picked pk ON pk.entrypoint = q.entrypoint
    WHERE q.status = 'queued'
      AND q.execute_after < NOW()
      AND ($5::BIGINT IS NULL
           OR (SELECT total FROM worker_load) < $5)
      AND (p.concurrency_limit <= 0
           OR COALESCE(pk.total, 0) < p.concurrency_limit)
    ORDER BY q.priority DESC, q.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $1
),

"""
    start, end = new.index(NEW_START), new.index(NEXT_STALE)
    if start >= end:
        raise SystemExit("new dequeue shape moved; update NEW_START/NEXT_STALE")
    return new[:start] + old_block + new[end:]


async def seed(
    conn: asyncpg.Connection, table: str, n: int, entrypoints: int, picked_per_ep: int
) -> None:
    await conn.execute(f"TRUNCATE {table};")
    await conn.execute(
        f"""
        INSERT INTO {table} (priority, entrypoint, status, execute_after)
        SELECT g % 32, 'ep_' || (g % {entrypoints}), 'queued', NOW() - interval '1 second'
        FROM generate_series(1, {n}) g;
        """
    )
    # Put each entrypoint at its concurrency limit so the gate excludes it; this
    # is the steady-state the LATERAL short-circuits and the old scan walks past.
    if picked_per_ep:
        await conn.execute(
            f"""
            INSERT INTO {table}
                (priority, entrypoint, status, execute_after, queue_manager_id, heartbeat)
            SELECT 31, 'ep_' || (e % {entrypoints}), 'picked', NOW() - interval '1 second',
                   gen_random_uuid(), NOW()
            FROM generate_series(1, {entrypoints * picked_per_ep}) e;
            """
        )
    await conn.execute(f"ANALYZE {table};")


async def measure(conn: asyncpg.Connection, sql: str, args: tuple, repeats: int) -> dict:
    times: list[float] = []
    reads: list[int] = []
    scan = ""
    for _ in range(repeats):
        tx = conn.transaction()
        await tx.start()
        try:
            rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, *args)
        finally:
            await tx.rollback()
        raw = rows[0]["QUERY PLAN"]
        plan = json.loads(raw) if isinstance(raw, str) else raw
        root = plan[0]
        times.append(root["Execution Time"])
        nodes: list[tuple[str, int]] = []

        def walk(n: dict) -> None:
            nodes.append((n.get("Node Type", ""), n.get("Shared Read Blocks", 0)))
            for c in n.get("Plans", []) or []:
                walk(c)

        walk(root["Plan"])
        reads.append(sum(r for _, r in nodes))
        scan = next((t for t, _ in nodes if "Seq Scan" in t), "") or next(
            (t for t, _ in nodes if "Index" in t), ""
        )
    return {
        "median_ms": statistics.median(times),
        "min_ms": min(times),
        "median_read_blocks": int(statistics.median(reads)),
        "scan": scan,
    }


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--entrypoints", type=int, default=18)
    ap.add_argument("--batch-size", type=int, default=10)
    ap.add_argument("--concurrency-limit", type=int, default=1)
    ap.add_argument("--picked-per-entrypoint", type=int, default=1)
    ap.add_argument("--repeats", type=int, default=11)
    a = ap.parse_args()

    table = qb.DBSettings().queue_table
    conn = await asyncpg.connect()
    try:
        q = Queries(AsyncpgDriver(conn))
        if not await q.has_table(table):
            await q.install()

        await seed(conn, table, a.rows, a.entrypoints, a.picked_per_entrypoint)

        eps = [f"ep_{i}" for i in range(a.entrypoints)]
        limits = [a.concurrency_limit] * a.entrypoints
        args = (a.batch_size, eps, limits, uuid.uuid4(), None, timedelta(seconds=30))

        old = await measure(conn, old_dequeue_query(), args, a.repeats)
        new = await measure(conn, qb.QueryQueueBuilder().build_dequeue_query(), args, a.repeats)
    finally:
        await conn.close()

    print(
        f"\nrows={a.rows} entrypoints={a.entrypoints} batch_size={a.batch_size} repeats={a.repeats}"
    )
    print(f"{'variant':8} {'median_ms':>10} {'min_ms':>8} {'read_blocks':>12}  scan")
    for name, r in (("OLD", old), ("NEW", new)):
        print(
            f"{name:8} {r['median_ms']:>10.2f} {r['min_ms']:>8.2f} "
            f"{r['median_read_blocks']:>12}  {r['scan']}"
        )
    print(f"\nspeedup (median): {old['median_ms'] / new['median_ms']:.1f}x")


if __name__ == "__main__":
    asyncio.run(main())
