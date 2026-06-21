"""Plan-shape regression guards for the dequeue path (commit e58ff3c, #668).

Functional tests prove the queries return the right rows; they are blind to
the planner silently reverting to a Seq Scan over the whole queue (issue #667).
These tests seed a backlog, ANALYZE, then assert via EXPLAIN that the hot
queries still hit the entrypoint-leading indexes / keep their O(1) shape.

EXPLAIN (without ANALYZE) only plans — it neither executes nor claims rows —
so the seeded jobs stay queued and the plans are stable.

Note: a Seq Scan on the queue table is NOT asserted-against globally — the
``claimed`` UPDATE step legitimately seq-scans a tiny id-list at small N. The
assertions target the specific hot-path nodes that carried the #668 win.
"""

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
SEED = 3000  # large enough that the planner prefers the index over a Seq Scan


def _flatten(node: dict) -> list[dict]:
    """Depth-first list of every plan node."""
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


async def _seed(driver: db.Driver) -> queries.Queries:
    q = queries.Queries(driver)
    eps = [ENTRYPOINTS[i % len(ENTRYPOINTS)] for i in range(SEED)]
    await q.enqueue(eps, [None] * SEED, [i % 7 for i in range(SEED)])
    await driver.execute(f"ANALYZE {QUEUE_TABLE};")
    return q


async def test_dequeue_uses_entrypoint_priority_index(apgdriver: db.Driver) -> None:
    """The per-entrypoint LATERAL must read the (entrypoint, priority, id)
    partial index, not Seq-Scan + Sort the whole queue (issue #667)."""
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
    index_scans = [
        n
        for n in nodes
        if "Index" in (n.get("Node Type") or "") and n.get("Index Name") == EP_PRIO_ID_IDX
    ]
    assert index_scans, (
        f"dequeue plan no longer uses {EP_PRIO_ID_IDX}; "
        f"nodes={[(n.get('Node Type'), n.get('Index Name')) for n in nodes]}"
    )


async def test_next_deferred_eta_uses_execute_after_index(apgdriver: db.Driver) -> None:
    """next_deferred_eta must read the (entrypoint, execute_after) index with a
    LIMIT, not aggregate (MIN) over a full scan."""
    q = await _seed(apgdriver)
    # Defer some jobs so there is an eligible deferred row to plan against.
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
    # The MIN->ORDER BY LIMIT 1 rewrite must keep a Limit node.
    assert any(n.get("Node Type") == "Limit" for n in nodes), "eta lost its LIMIT shape"


async def test_has_queued_work_is_existence_probe(apgdriver: db.Driver) -> None:
    """has_queued_work must keep the LIMIT 1 existence shape, not revert to a
    full COUNT(*) aggregate over every queued row."""
    q = await _seed(apgdriver)
    nodes = await _plan_nodes(apgdriver, q.qbq.build_has_queued_work(), ENTRYPOINTS)
    assert any(n.get("Node Type") == "Limit" for n in nodes), (
        "has_queued_work lost its LIMIT 1 existence-probe shape; "
        f"nodes={[n.get('Node Type') for n in nodes]}"
    )
