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


async def _seed(driver: db.Driver) -> queries.Queries:
    q = queries.Queries(driver)
    eps = [ENTRYPOINTS[i % len(ENTRYPOINTS)] for i in range(SEED)]
    await q.enqueue(eps, [None] * SEED, [i % 7 for i in range(SEED)])
    await driver.execute(f"ANALYZE {QUEUE_TABLE};")
    return q


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
