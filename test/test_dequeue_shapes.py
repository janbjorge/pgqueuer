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
from typing import Any

import pytest

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.composer import ComposedQuery
from pgqueuer.adapters.persistence.dequeue import DequeueArgs, DequeueShape, dequeue_shape
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId

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
    batch_size: Any = 10,
) -> ComposedQuery:
    return (builder or pinned_builder()).build_dequeue_query(
        batch_size=batch_size,
        entrypoints=[QueueEntrypoint("fetch")],
        concurrency_limits=[concurrency_limit],
        queue_manager_id=QueueManagerId(uuid.UUID(int=0)),
        global_concurrency_limit=global_limit,
        heartbeat_timeout=timedelta(seconds=30),
    )


def update_snapshots() -> bool:
    return os.environ.get("PGQUEUER_UPDATE_SNAPSHOTS", "").lower() in ("1", "true")


@pytest.mark.parametrize(
    ("concurrency_limits", "global_limit", "expected"),
    [
        ([0], None, DequeueShape.no_gates),
        ([0, 2], None, DequeueShape.entrypoint_gate),
        ([0], 3, DequeueShape.global_gate),
        ([2], 3, DequeueShape.both_gates),
    ],
)
def test_dequeue_shape_follows_the_configured_gates(
    concurrency_limits: list[int],
    global_limit: int | None,
    expected: DequeueShape,
) -> None:
    args = DequeueArgs(
        batch_size=10,
        entrypoints=[QueueEntrypoint(f"ep{i}") for i in range(len(concurrency_limits))],
        concurrency_limits=concurrency_limits,
        queue_manager_id=QueueManagerId(uuid.UUID(int=0)),
        global_concurrency_limit=global_limit,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert dequeue_shape(args) is expected


@pytest.mark.parametrize(
    ("shape", "concurrency_limit", "global_limit"),
    [
        (DequeueShape.no_gates, 0, None),
        (DequeueShape.entrypoint_gate, 2, None),
        (DequeueShape.global_gate, 0, 3),
        (DequeueShape.both_gates, 2, 3),
    ],
)
def test_dequeue_sql_matches_snapshot(
    shape: DequeueShape,
    concurrency_limit: int,
    global_limit: int | None,
) -> None:
    query = compose(concurrency_limit, global_limit)
    # Every $N placeholder maps onto exactly one bound arg: no gaps, no extras.
    placeholders = {int(n) for n in re.findall(r"\$(\d+)", query.sql)}
    assert placeholders == set(range(1, len(query.args) + 1))

    path = SNAPSHOT_DIR / f"dequeue_{shape.value}.sql"
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


def test_dequeue_template_cache_is_not_part_of_the_builder_value() -> None:
    builder = pinned_builder()
    compose(2, 3, builder)
    assert builder == pinned_builder()
    assert "dequeue_templates" not in repr(builder)


def test_dequeue_args_are_rebound_every_call() -> None:
    # Cached SQL must never share argument objects between calls: a driver
    # (a public Protocol) that mutates args in place must not poison later
    # dequeues.
    builder = pinned_builder()
    first = compose(2, 3, builder)
    entrypoints = first.args[0]
    assert isinstance(entrypoints, list)
    entrypoints.append("evil")

    second = compose(2, 3, builder)
    assert second.args[0] == ["fetch"]
    assert second.args[3] == [2]


@pytest.mark.parametrize("batch_size", [0, -1, True])
def test_dequeue_rejects_batch_size_that_is_not_a_positive_int(batch_size: Any) -> None:
    # batch_size is rendered into the SQL text, so only a positive int may reach it.
    with pytest.raises(ValueError, match="positive integer"):
        compose(2, None, batch_size=batch_size)


def test_dequeue_sql_is_cached_per_batch_size() -> None:
    builder = pinned_builder()
    ten = compose(2, 3, builder, batch_size=10)
    eleven = compose(2, 3, builder, batch_size=11)
    assert ten.sql != eleven.sql
    assert compose(2, 3, builder, batch_size=10).sql is ten.sql
    assert compose(2, 3, builder, batch_size=11).sql is eleven.sql


def test_dequeue_rejects_mismatched_concurrency_limits() -> None:
    # The old monolithic query NULL-padded surplus entrypoints out of the
    # batch; per-gate composition would silently un-gate them instead.
    with pytest.raises(ValueError, match="same length"):
        pinned_builder().build_dequeue_query(
            batch_size=10,
            entrypoints=[QueueEntrypoint("a"), QueueEntrypoint("b")],
            concurrency_limits=[0],
            queue_manager_id=QueueManagerId(uuid.UUID(int=0)),
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
        entrypoints={
            QueueEntrypoint("fetch"): queries.EntrypointExecutionParameter(concurrency_limit)
        },
        queue_manager_id=QueueManagerId(uuid.uuid4()),
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
        entrypoints={QueueEntrypoint("fetch"): queries.EntrypointExecutionParameter(0)},
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )

    picked = await dequeue(queue_manager_id=QueueManagerId(uuid.uuid4()))
    assert len(picked) == 2

    await apgdriver.execute(
        f"UPDATE {DBSettings().queue_table} SET heartbeat = NOW() - interval '1 hour'"
    )

    recovered = await dequeue(queue_manager_id=QueueManagerId(uuid.uuid4()))
    assert sorted(j.id for j in recovered) == sorted(j.id for j in picked)
