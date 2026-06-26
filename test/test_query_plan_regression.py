"""EXPLAIN guards that the dequeue path still hits the entrypoint-leading indexes (#668)."""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb

QUEUE_TABLE = qb.DBSettings().queue_table
EP_PRIO_ID_IDX = f"{QUEUE_TABLE}_ep_prio_id_idx"
EP_EA_IDX = f"{QUEUE_TABLE}_ep_ea_idx"

ENTRYPOINTS = ["a", "b", "c", "d"]
# Large enough that the planner prefers the index over a Seq Scan.
SEED = 3000


def _flatten(node: dict) -> list[dict]:
    out = [node]
    children = node.get("Plans")
    if isinstance(children, list):
        for child in children:
            out.extend(_flatten(child))
    return out


async def _plan_nodes(driver: db.Driver, sql: str, *args: object) -> list[dict]:
    rows = await driver.fetch("EXPLAIN (FORMAT JSON) " + sql, *args)
    raw = rows[0]["QUERY PLAN"]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    return _flatten(plan[0]["Plan"])


async def _analyze_nodes(driver: db.Driver, sql: str, *args: object) -> list[dict]:
    # EXPLAIN ANALYZE executes the query; dequeue mutates (UPDATE + log INSERT),
    # so run inside a transaction we roll back to keep the guard side-effect-free.
    await driver.execute("BEGIN")
    try:
        rows = await driver.fetch("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, *args)
    finally:
        await driver.execute("ROLLBACK")
    raw = rows[0]["QUERY PLAN"]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    return _flatten(plan[0]["Plan"])


async def _seed(driver: db.Driver) -> queries.Queries:
    q = queries.Queries(driver)
    eps = [ENTRYPOINTS[i % len(ENTRYPOINTS)] for i in range(SEED)]
    await q.enqueue(eps, [None] * SEED, [i % 7 for i in range(SEED)])
    await driver.execute(f"ANALYZE {QUEUE_TABLE};")
    return q


# Bulk backlog large enough that a regressed global scan is unmistakable next to a
# proportional per-entrypoint plan. Raw INSERT (not enqueue) keeps seeding cheap.
BULK_ROWS = 50_000
BULK_EPS = 8
BATCH = 10


async def _bulk_seed(driver: db.Driver, rows: int, n_eps: int) -> None:
    await driver.execute(f"TRUNCATE {QUEUE_TABLE};")
    await driver.execute(
        f"""
        INSERT INTO {QUEUE_TABLE} (priority, entrypoint, status, execute_after)
        SELECT g % 32, 'ep_' || (g % {n_eps}), 'queued', NOW() - interval '1 second'
        FROM generate_series(1, {rows}) g;
        """
    )
    await driver.execute(f"ANALYZE {QUEUE_TABLE};")


def _queue_scans(nodes: list[dict]) -> list[dict]:
    return [n for n in nodes if n.get("Relation Name") == QUEUE_TABLE]


def _rows_scanned(nodes: list[dict]) -> int:
    return sum(int(n.get("Actual Rows") or 0) for n in _queue_scans(nodes))


async def test_dequeue_uses_entrypoint_priority_index(apgdriver: db.Driver) -> None:
    """Dequeue's LATERAL reads the (entrypoint, priority, id) index, not a Seq Scan (#667)."""
    q = await _seed(apgdriver)
    nodes = await _plan_nodes(
        apgdriver,
        q.qbq.build_dequeue_query(),
        10,
        ENTRYPOINTS,
        [0] * len(ENTRYPOINTS),
        uuid.uuid4(),
        1000,
        timedelta(seconds=30),
    )
    used = [
        n
        for n in nodes
        if "Index" in (n.get("Node Type") or "") and n.get("Index Name") == EP_PRIO_ID_IDX
    ]
    assert used, (
        f"dequeue plan no longer uses {EP_PRIO_ID_IDX}; "
        f"nodes={[(n.get('Node Type'), n.get('Index Name')) for n in nodes]}"
    )


async def test_next_deferred_eta_uses_execute_after_index(apgdriver: db.Driver) -> None:
    """next_deferred_eta reads the (entrypoint, execute_after) index with a LIMIT (#668)."""
    q = await _seed(apgdriver)
    await q.enqueue(
        ENTRYPOINTS,
        [None] * len(ENTRYPOINTS),
        [0] * len(ENTRYPOINTS),
        [timedelta(seconds=60)] * len(ENTRYPOINTS),
    )
    await apgdriver.execute(f"ANALYZE {QUEUE_TABLE};")
    nodes = await _plan_nodes(apgdriver, q.qbq.build_next_deferred_eta_query(), ENTRYPOINTS)
    assert any(n.get("Index Name") == EP_EA_IDX for n in nodes), (
        f"eta plan no longer uses {EP_EA_IDX}; "
        f"nodes={[(n.get('Node Type'), n.get('Index Name')) for n in nodes]}"
    )
    assert any(n.get("Node Type") == "Limit" for n in nodes), "eta lost its LIMIT shape"


async def test_has_queued_work_is_existence_probe(apgdriver: db.Driver) -> None:
    """has_queued_work keeps its LIMIT 1 existence shape, not a full COUNT(*) (#668)."""
    q = await _seed(apgdriver)
    nodes = await _plan_nodes(apgdriver, q.qbq.build_has_queued_work(), ENTRYPOINTS)
    assert any(n.get("Node Type") == "Limit" for n in nodes), (
        "has_queued_work lost its LIMIT 1 existence-probe shape; "
        f"nodes={[n.get('Node Type') for n in nodes]}"
    )


async def test_dequeue_plan_scans_proportional_to_batch(apgdriver: db.Driver) -> None:
    """Dequeue scans O(entrypoints*batch) rows, not the whole backlog (#668)."""
    await _bulk_seed(apgdriver, BULK_ROWS, BULK_EPS)
    q = queries.Queries(apgdriver)
    eps = [f"ep_{i}" for i in range(BULK_EPS)]
    nodes = await _analyze_nodes(
        apgdriver,
        q.qbq.build_dequeue_query(),
        BATCH,
        eps,
        [0] * BULK_EPS,
        uuid.uuid4(),
        None,
        timedelta(seconds=30),
    )

    seq = [n for n in _queue_scans(nodes) if "Seq Scan" in (n.get("Node Type") or "")]
    assert not seq, (
        f"dequeue fell back to a Seq Scan on {QUEUE_TABLE}; "
        f"nodes={[(n.get('Node Type'), n.get('Actual Rows')) for n in _queue_scans(nodes)]}"
    )

    scanned = _rows_scanned(nodes)
    row_bound = BULK_EPS * BATCH * 4
    assert scanned <= row_bound, (
        f"dequeue scanned {scanned} rows (bound {row_bound}); a proportional plan reads "
        f"~entrypoints*batch, a regressed global scan reads ~{BULK_ROWS}. "
        f"nodes={[(n.get('Node Type'), n.get('Actual Rows')) for n in _queue_scans(nodes)]}"
    )


async def test_dequeue_gate_skips_saturated_entrypoints(apgdriver: db.Driver) -> None:
    """Saturated entrypoints are gated out by the CTE, not by scanning their queued rows (#668)."""
    await _bulk_seed(apgdriver, BULK_ROWS, BULK_EPS)
    # One picked job per entrypoint puts each at concurrency_limit=1, so the
    # `available` CTE empties and the per-entrypoint LATERAL is never entered.
    await apgdriver.execute(
        f"""
        INSERT INTO {QUEUE_TABLE}
            (priority, entrypoint, status, execute_after, queue_manager_id, heartbeat)
        SELECT 31, 'ep_' || (e % {BULK_EPS}), 'picked', NOW() - interval '1 second',
               gen_random_uuid(), NOW()
        FROM generate_series(1, {BULK_EPS}) e;
        """
    )
    await apgdriver.execute(f"ANALYZE {QUEUE_TABLE};")

    q = queries.Queries(apgdriver)
    eps = [f"ep_{i}" for i in range(BULK_EPS)]
    nodes = await _analyze_nodes(
        apgdriver,
        q.qbq.build_dequeue_query(),
        BATCH,
        eps,
        [1] * BULK_EPS,
        uuid.uuid4(),
        None,
        timedelta(seconds=30),
    )

    # Only the picked aggregate and stale probe touch the table (~BULK_EPS rows each);
    # the queued LATERAL must read nothing because every entrypoint is gated out.
    scanned = _rows_scanned(nodes)
    bound = BULK_EPS * 4
    assert scanned <= bound, (
        f"saturated-gate dequeue scanned {scanned} queue rows (bound {bound}); "
        f"the concurrency gate should exclude every entrypoint before the LATERAL fires. "
        f"nodes={[(n.get('Node Type'), n.get('Actual Rows')) for n in _queue_scans(nodes)]}"
    )
