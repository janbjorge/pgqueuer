"""Each Queries call sends the SQL in its snapshot under test/query_shapes/statements/.

Regenerate after an intended change with:

    PGQUEUER_UPDATE_SNAPSHOTS=1 uv run pytest test/test_statement_snapshots.py
"""

from __future__ import annotations

import inspect
import os
import re
import textwrap
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NamedTuple

import pytest

from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.domain import models
from pgqueuer.domain.settings import DBSettings, Durability
from pgqueuer.domain.types import (
    Channel,
    CronEntrypoint,
    CronExpression,
    JobId,
    QueueEntrypoint,
    QueueManagerId,
    ScheduleId,
)
from test.helpers import mocked_job

SNAPSHOT_DIR = Path(__file__).parent / "query_shapes" / "statements"

# Every name that reaches SQL, pinned so a developer's PGQUEUER_* env vars cannot move a snapshot.
SETTINGS = DBSettings(
    db_schema=None,
    queue_table="pgqueuer",
    queue_table_log="pgqueuer_log",
    statistics_table="pgqueuer_statistics",
    schedules_table="pgqueuer_schedules",
    queue_status_type="pgqueuer_status",
    channel=Channel("ch_pgqueuer"),
    function="fn_pgqueuer_changed",
    trigger="tg_pgqueuer_changed",
)


class Statement(NamedTuple):
    sql: str
    args: tuple[object, ...]


class RecordingDriver(InMemoryDriver):
    """Keeps each statement it is handed; every fetch answers with *rows*."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        super().__init__()
        self.rows = rows
        self.seen: list[Statement] = []

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.seen.append(Statement(query, args))
        return self.rows

    async def execute(self, query: str, *args: object) -> str:
        self.seen.append(Statement(query, args))
        return ""


EPOCH = datetime(2024, 1, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)
FETCH = [QueueEntrypoint("fetch")]
WORKER = QueueManagerId(uuid.UUID(int=1))
JOB = mocked_job(
    id=1,
    created=EPOCH,
    heartbeat=EPOCH,
    execute_after=EPOCH,
    updated=EPOCH,
    queue_manager_id=WORKER,
)
PAIR = models.CronExpressionEntrypoint(
    entrypoint=CronEntrypoint("report"),
    expression=CronExpression("* * * * *"),
)

# Keyed "<method>" or "<method>-<branch>": one per branch of a method that sends different SQL.
CALLS: dict[str, Callable[[Queries], Awaitable[object]]] = {
    "active_workers": lambda q: q.active_workers(),
    "aggregate_logs": lambda q: q.aggregate_logs(),
    "alter_durability": lambda q: q.alter_durability(Durability.balanced),
    "browse_queue": lambda q: q.browse_queue(),
    "clear_queue-all": lambda q: q.clear_queue(),
    "clear_queue-entrypoint": lambda q: q.clear_queue("fetch"),
    "clear_queue_log-all": lambda q: q.clear_queue_log(),
    "clear_queue_log-entrypoint": lambda q: q.clear_queue_log("fetch"),
    "clear_schedule": lambda q: q.clear_schedule(),
    "clear_statistics_log-all": lambda q: q.clear_statistics_log(),
    "clear_statistics_log-entrypoint": lambda q: q.clear_statistics_log("fetch"),
    "delete_schedule": lambda q: q.delete_schedule({ScheduleId(1)}, {PAIR.entrypoint}, keep={PAIR}),
    "eligible_queued_work": lambda q: q.eligible_queued_work(FETCH, concurrency_limits=[2]),
    "enqueue-raise": lambda q: q.enqueue("fetch", None),
    "enqueue-skip": lambda q: q.enqueue("fetch", None, dedupe_key="key", on_conflict="skip"),
    "enqueue-update_priority": lambda q: q.enqueue(
        "fetch", None, dedupe_key="key", on_conflict="update_priority"
    ),
    "exception_logs": lambda q: q.exception_logs(),
    "fetch_schedule": lambda q: q.fetch_schedule({PAIR: HOUR}),
    "has_function": lambda q: q.has_function("fn_pgqueuer_changed"),
    "has_table": lambda q: q.has_table("pgqueuer"),
    "has_trigger": lambda q: q.has_trigger("tg_pgqueuer_changed"),
    "has_user_defined_enum": lambda q: q.has_user_defined_enum("canceled", "pgqueuer_status"),
    "insert_schedule": lambda q: q.insert_schedule({PAIR: HOUR}),
    "job_duration_percentiles": lambda q: q.job_duration_percentiles(HOUR),
    "job_log_history": lambda q: q.job_log_history(JobId(1)),
    "job_status": lambda q: q.job_status([JobId(1)]),
    "list_failed_jobs-asc": lambda q: q.list_failed_jobs(order="ASC"),
    "list_failed_jobs-desc": lambda q: q.list_failed_jobs(order="DESC"),
    "log_jobs": lambda q: q.log_jobs([(JOB, "successful", None)]),
    "log_statistics-all": lambda q: q.log_statistics(None),
    "log_statistics-last": lambda q: q.log_statistics(None, HOUR),
    "log_statistics-limit": lambda q: q.log_statistics(10),
    "log_statistics-limit_last": lambda q: q.log_statistics(10, HOUR),
    "mark_job_as_cancelled": lambda q: q.mark_job_as_cancelled([JobId(1)]),
    "next_deferred_eta": lambda q: q.next_deferred_eta(FETCH),
    "next_stale_eta": lambda q: q.next_stale_eta(
        FETCH, queue_manager_id=WORKER, heartbeat_timeout=HOUR
    ),
    "optimize_autovacuum": lambda q: q.optimize_autovacuum(),
    "optimize_autovacuum_rollback": lambda q: q.optimize_autovacuum_rollback(),
    "peek_schedule": lambda q: q.peek_schedule(),
    "queue_age": lambda q: q.queue_age(),
    "queue_job_by_id": lambda q: q.queue_job_by_id(JobId(1)),
    "queue_log": lambda q: q.queue_log(),
    "queue_size": lambda q: q.queue_size(),
    "queued_work": lambda q: q.queued_work(FETCH),
    "requeue_jobs": lambda q: q.requeue_jobs([JobId(1)]),
    "retry_job": lambda q: q.retry_job(JOB, HOUR, None),
    "schema_info": lambda q: q.schema_info(),
    "set_schedule_queued": lambda q: q.set_schedule_queued({ScheduleId(1)}),
    "stale_jobs": lambda q: q.stale_jobs(HOUR),
    "table_has_column": lambda q: q.table_has_column("pgqueuer", "slot"),
    "table_has_index": lambda q: q.table_has_index("pgqueuer", "pgqueuer_picked_slot_idx"),
    "throughput_summary": lambda q: q.throughput_summary(HOUR),
    "throughput_timeseries": lambda q: q.throughput_timeseries(HOUR),
    "unaggregated_log_count": lambda q: q.unaggregated_log_count(),
    "update_heartbeat": lambda q: q.update_heartbeat([JobId(1)]),
    "update_schedule_heartbeat": lambda q: q.update_schedule_heartbeat({ScheduleId(1)}),
}

# The catalog probes unpack exactly one row; every other call reads none.
ROWS: dict[str, list[dict[str, object]]] = {
    name: [{"exists": True}]
    for name in ("has_function", "has_table", "has_trigger", "table_has_column", "table_has_index")
}

SKIP = {
    "dequeue": "test_dequeue_shapes.py snapshots its texts",
    "install": "schema management; the schema tests cover it",
    "uninstall": "schema management; the schema tests cover it",
    "upgrade": "schema management; the schema tests cover it",
    "apply_upgrade": "schema management; the schema tests cover it",
    "plan_upgrade": "schema management; the schema tests cover it",
    "schema_lock": "schema management; the schema tests cover it",
    "verify_schema": "schema management; the schema tests cover it",
    "missing_schema": "schema management; the schema tests cover it",
    "schema_is_installed": "schema management; the schema tests cover it",
    "from_asyncpg_connection": "constructor; sends nothing",
    "from_asyncpg_pool": "constructor; sends nothing",
    "from_psycopg_connection": "constructor; sends nothing",
    "add_listener": "listener API of the driver; sends no statement",
    "remove_listener": "listener API of the driver; sends no statement",
    "notify_health_check": "driver.notify only; sends no statement",
    "notify_job_cancellation": "driver.notify only; sends no statement",
    "notify_table_changed": "driver.notify only; sends no statement",
}


def shown(sql: str) -> str:
    lines = sql.strip("\n").splitlines()
    rest = textwrap.dedent("\n".join(lines[1:])).splitlines()
    return "\n".join(line.rstrip() for line in [lines[0].strip(), *rest]).strip() + "\n"


@pytest.mark.parametrize("name", CALLS)
async def test_statements_match_snapshot(name: str) -> None:
    driver = RecordingDriver(ROWS.get(name, []))
    await CALLS[name](Queries(driver, SETTINGS))
    assert driver.seen, f"{name} sends no SQL; move it to SKIP"

    texts = [shown(statement.sql) for statement in driver.seen]
    if len(texts) > 1:
        texts = [f"-- statement {n}\n{text}" for n, text in enumerate(texts, 1)]
    expected = "\n".join(texts)
    path = SNAPSHOT_DIR / f"{name}.sql"
    if os.environ.get("PGQUEUER_UPDATE_SNAPSHOTS", "").lower() in ("1", "true"):
        path.write_text(expected)
    assert expected == path.read_text(), (
        f"SQL sent by {name} diverged from {path}; if intended, regenerate with "
        "PGQUEUER_UPDATE_SNAPSHOTS=1"
    )


@pytest.mark.parametrize("name", CALLS)
async def test_statement_placeholders_match_their_args(name: str) -> None:
    driver = RecordingDriver(ROWS.get(name, []))
    await CALLS[name](Queries(driver, SETTINGS))
    for statement in driver.seen:
        placeholders = {int(n) for n in re.findall(r"\$(\d+)", statement.sql)}
        assert placeholders == set(range(1, len(statement.args) + 1)), statement.sql


def test_every_public_method_is_snapshotted_or_skipped() -> None:
    public = {name for name, _ in inspect.getmembers(Queries, callable) if not name.startswith("_")}
    called = {call.partition("-")[0] for call in CALLS}
    assert called.isdisjoint(SKIP)
    assert called | SKIP.keys() == public, "add the method to CALLS or SKIP"


def test_every_snapshot_belongs_to_a_call() -> None:
    assert {path.stem for path in SNAPSHOT_DIR.glob("*.sql")} == set(CALLS)
