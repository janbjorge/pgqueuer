"""Dequeue SQL shape follows the concurrency gates in use.

The rendered SQL of every shape is snapshotted under test/query_shapes/ so a
change to the composition machinery shows up in review as a plain SQL diff.
Regenerate after an intended change with:

    PGQUEUER_UPDATE_SNAPSHOTS=1 uv run pytest test/test_dequeue_shapes.py
"""

from __future__ import annotations

import functools
import os
import re
import uuid
from datetime import timedelta
from pathlib import Path

import pytest

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.composer import ComposedQuery
from pgqueuer.domain.settings import DBSettings

SNAPSHOT_DIR = Path(__file__).parent / "query_shapes"


def pinned_builder() -> qb.QueryQueueBuilder:
    # Snapshots hard-code unprefixed table names; pin them so a developer's
    # exported PGQUEUER_* env vars cannot skew the rendered SQL.
    return qb.QueryQueueBuilder(
        settings=DBSettings(db_schema=None, queue_table="pgqueuer", queue_table_log="pgqueuer_log")
    )


def compose(
    concurrency_limit: int,
    global_limit: int | None,
    builder: qb.QueryQueueBuilder | None = None,
) -> ComposedQuery:
    return (builder or pinned_builder()).build_dequeue_query(
        batch_size=10,
        entrypoints=["fetch"],
        concurrency_limits=[concurrency_limit],
        queue_manager_id=uuid.UUID(int=0),
        global_concurrency_limit=global_limit,
        heartbeat_timeout=timedelta(seconds=30),
    )


def update_snapshots() -> bool:
    return os.environ.get("PGQUEUER_UPDATE_SNAPSHOTS", "").lower() in ("1", "true")


@pytest.mark.parametrize(
    ("snapshot", "concurrency_limit", "global_limit"),
    [
        ("dequeue_no_gates.sql", 0, None),
        ("dequeue_entrypoint_gate.sql", 2, None),
        ("dequeue_global_gate.sql", 0, 3),
        ("dequeue_both_gates.sql", 2, 3),
    ],
)
def test_dequeue_sql_matches_snapshot(
    snapshot: str,
    concurrency_limit: int,
    global_limit: int | None,
) -> None:
    query = compose(concurrency_limit, global_limit)
    # Every $N placeholder maps onto exactly one bound arg: no gaps, no extras.
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", query.sql)}
    assert placeholders == set(range(1, len(query.args) + 1))

    path = SNAPSHOT_DIR / snapshot
    expected = query.sql + "\n"
    if update_snapshots():
        path.write_text(expected)
    assert expected == path.read_text(), (
        f"rendered dequeue SQL diverged from {path}; if intended, regenerate with "
        "PGQUEUER_UPDATE_SNAPSHOTS=1"
    )


def test_dequeue_sql_is_cached_per_shape() -> None:
    # Same gate shape reuses the rendered text: the busy loop must not
    # recompose the statement every iteration, and driver-side prepared
    # statement caches must see one entry per shape.
    builder = pinned_builder()
    first = compose(0, None, builder)
    second = compose(0, None, builder)
    assert first.sql is second.sql
    assert compose(2, 3, builder).sql is compose(2, 3, builder).sql


def test_dequeue_args_are_rebound_every_call() -> None:
    # Cached SQL must never share argument objects between calls: a driver
    # (a public Protocol) that mutates args in place must not poison later
    # dequeues.
    builder = pinned_builder()
    first = compose(2, 3, builder)
    entrypoints = first.args[1]
    assert isinstance(entrypoints, list)
    entrypoints.append("evil")

    second = compose(2, 3, builder)
    assert second.args[1] == ["fetch"]
    assert second.args[4] == [2]


def test_dequeue_rejects_mismatched_concurrency_limits() -> None:
    # The old monolithic query NULL-padded surplus entrypoints out of the
    # batch; per-gate composition would silently un-gate them instead.
    with pytest.raises(ValueError, match="same length"):
        pinned_builder().build_dequeue_query(
            batch_size=10,
            entrypoints=["a", "b"],
            concurrency_limits=[0],
            queue_manager_id=uuid.UUID(int=0),
            global_concurrency_limit=None,
            heartbeat_timeout=timedelta(seconds=30),
        )


@pytest.mark.parametrize(
    ("concurrency_limit", "global_limit", "expected"),
    [
        (0, None, 5),
        (2, None, 2),
        (0, 3, 3),
        (2, 3, 2),
    ],
)
async def test_dequeue_shapes_respect_gates(
    apgdriver: db.Driver,
    concurrency_limit: int,
    global_limit: int | None,
    expected: int,
) -> None:
    q = queries.Queries(apgdriver)
    await q.enqueue(["fetch"] * 5, [None] * 5, [0] * 5)

    dequeue = functools.partial(
        q.dequeue,
        batch_size=10,
        entrypoints={"fetch": queries.EntrypointExecutionParameter(concurrency_limit)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=global_limit,
        heartbeat_timeout=timedelta(seconds=30),
    )

    first = await dequeue()
    assert len(first) == expected

    second = await dequeue()
    assert second == []


async def test_minimal_shape_recovers_stale_jobs(apgdriver: db.Driver) -> None:
    q = queries.Queries(apgdriver)
    await q.enqueue(["fetch"] * 2, [None] * 2, [0] * 2)

    dequeue = functools.partial(
        q.dequeue,
        batch_size=10,
        entrypoints={"fetch": queries.EntrypointExecutionParameter(0)},
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )

    picked = await dequeue(queue_manager_id=uuid.uuid4())
    assert len(picked) == 2

    await apgdriver.execute(
        f"UPDATE {DBSettings().queue_table} SET heartbeat = NOW() - interval '1 hour'"
    )

    recovered = await dequeue(queue_manager_id=uuid.uuid4())
    assert sorted(j.id for j in recovered) == sorted(j.id for j in picked)
