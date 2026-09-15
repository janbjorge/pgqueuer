"""Upgrading a database installed by an old release must reach the target schema.

Each fixture under ``schema_releases/`` is the verbatim install DDL of a
release whose schema differed from the one before it, generated once from the
tag and never regenerated -- the point is what those databases actually look
like in the field, not what we would install for them today.

``test_schema_inspect.py`` proves a *fresh* install matches the model. These
prove the other entry point: a database that has been around since v0.18 and
has been upgraded ever since lands in exactly the same place.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import pytest

from pgqueuer.adapters.persistence.schema_ddl import rendered
from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.adapters.persistence.schema_plan import plan
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.schema.model import Schema, Table
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId
from pgqueuer.queries import EntrypointExecutionParameter, Queries
from test.helpers import collapse, declared_schema

RELEASES_DIR = Path(__file__).parent / "schema_releases"
RELEASES = sorted(path.stem for path in RELEASES_DIR.glob("*.sql"))


class Gap(NamedTuple):
    """One declared object the live schema does not match.

    ``target`` names the object, ``detail`` carries the spelling found, so a
    failure says which object drifted and what it looks like now.
    """

    target: str
    detail: str


def enum_gaps(live: Schema, declared: Schema) -> list[Gap]:
    found = {entry.name: entry for entry in live.enums}
    gaps: list[Gap] = []
    for enum in declared.enums:
        installed = found.get(enum.name)
        if installed is None:
            gaps.append(Gap(f"type {enum.name}", "absent"))
            continue
        gaps += [
            Gap(f"type {enum.name}", f"label {label} absent")
            for label in enum.labels
            if label not in installed.labels
        ]
    return gaps


def table_gaps(live: Schema, declared: Schema) -> list[Gap]:
    found = {entry.name: entry for entry in live.tables}
    gaps: list[Gap] = []
    for table in declared.tables:
        installed = found.get(table.name)
        if installed is None:
            gaps.append(Gap(f"table {table.name}", "absent"))
            continue
        if installed.unlogged != table.unlogged:
            gaps.append(Gap(f"table {table.name}", f"unlogged={installed.unlogged}"))
        if installed.id_sequence_type != table.id_sequence_type:
            gaps.append(Gap(f"table {table.name}", f"id sequence {installed.id_sequence_type}"))
        gaps += column_gaps(installed, table)
    return gaps


def column_gaps(installed: Table, declared: Table) -> list[Gap]:
    found = {entry.name: entry for entry in installed.columns}
    gaps: list[Gap] = []
    for column in declared.columns:
        target = f"column {declared.name}.{column.name}"
        live_column = found.get(column.name)
        if live_column is None:
            gaps.append(Gap(target, "absent"))
        elif live_column != column:
            gaps.append(Gap(target, f"{live_column}"))
    return gaps


def index_gaps(live: Schema, declared: Schema) -> list[Gap]:
    found = {entry.name: entry for entry in live.indexes}
    gaps: list[Gap] = []
    for index in declared.indexes:
        installed = found.get(index.name)
        if installed is None:
            gaps.append(Gap(f"index {index.name}", "absent"))
        elif installed != index:
            gaps.append(Gap(f"index {index.name}", f"unique={installed.unique} {installed.body}"))
    return gaps


def routine_gaps(live: Schema, declared: Schema) -> list[Gap]:
    bodies = {entry.name: entry.body for entry in live.functions}
    triggers = {entry.name: entry for entry in live.triggers}
    gaps = [
        Gap(f"function {entry.name}", "differs")
        for entry in declared.functions
        if bodies.get(entry.name) != entry.body
    ]
    gaps += [
        Gap(f"trigger {entry.name}", "differs")
        for entry in declared.triggers
        if triggers.get(entry.name) != entry
    ]
    return gaps


def shortfall(live: Schema, declared: Schema) -> list[Gap]:
    """What *declared* asks for that *live* lacks, or spells differently.

    Containment, not equality: an upgraded database legitimately keeps objects
    the current release no longer declares. ``time_in_queue`` holds data, and
    dropping a column to satisfy a diff would destroy it.
    """
    return (
        enum_gaps(live, declared)
        + table_gaps(live, declared)
        + index_gaps(live, declared)
        + routine_gaps(live, declared)
    )


async def install_release(driver: AsyncpgDriver, release: str) -> None:
    """Replace the template schema with the one *release* shipped."""
    await Queries(driver).uninstall()
    await driver.execute((RELEASES_DIR / f"{release}.sql").read_text())


async def upgraded(driver: AsyncpgDriver, release: str) -> Queries:
    await install_release(driver, release)
    queries = Queries(driver)
    await queries.upgrade()
    return queries


@pytest.mark.parametrize("release", RELEASES)
async def test_upgrade_reaches_the_declaration(apgdriver: AsyncpgDriver, release: str) -> None:
    """Upgrading any release reaches every object the model declares.

    Install and upgrade are rendered from the same declaration, so there is
    nothing left for them to disagree about. A failure here names the object
    and the spelling found.
    """
    settings = DBSettings()
    await upgraded(apgdriver, release)

    live = collapse(await inspect(apgdriver, settings))
    assert shortfall(live, declared_schema(settings)) == []


@pytest.mark.parametrize("release", RELEASES)
async def test_upgraded_release_runs_a_job(apgdriver: AsyncpgDriver, release: str) -> None:
    """The queue works end to end after upgrading, not merely on paper."""
    queries = await upgraded(apgdriver, release)

    assert len(await queries.enqueue(["ep"], [b"x"], [0])) == 1
    jobs = await queries.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("ep"): EntrypointExecutionParameter(0)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=300),
    )
    assert len(jobs) == 1
    await queries.log_jobs([(jobs[0], "successful", None)])


@pytest.mark.parametrize("release", RELEASES)
async def test_upgraded_release_aggregates_statistics(
    apgdriver: AsyncpgDriver,
    release: str,
) -> None:
    """Folding the log into statistics needs the unique index to match.

    Two things used to break this on a v0.18 database: the unique index still
    keyed on ``time_in_queue``, so no index backed the ``ON CONFLICT``
    specification, and the column itself was ``NOT NULL`` with nothing left to
    fill it. Converging rebuilds the index and relaxes the constraint.
    """
    queries = await upgraded(apgdriver, release)
    await queries.enqueue(["ep"], [b"x"], [0])
    await queries.aggregate_logs()

    assert await queries.log_statistics(limit=10) != []


@pytest.mark.parametrize("release", RELEASES)
async def test_upgrade_is_rerunnable(apgdriver: AsyncpgDriver, release: str) -> None:
    """A second upgrade on an already-upgraded database changes nothing."""
    settings = DBSettings()
    queries = await upgraded(apgdriver, release)
    once = collapse(await inspect(apgdriver, settings))

    await queries.upgrade()
    assert collapse(await inspect(apgdriver, settings)) == once


@pytest.mark.parametrize("release", RELEASES)
async def test_nothing_is_left_to_do_after_upgrade(apgdriver: AsyncpgDriver, release: str) -> None:
    """The planner, run against the real catalog, has no statement left to emit.

    ``shortfall`` above compares two models; this compares what the planner
    would actually do. Both must agree the database is converged.

    Notes are not statements and do not count. A v0.18 database keeps
    ``time_in_queue`` by design, so the note offering to drop it is still
    there, and will be until an operator acts on it.
    """
    settings = DBSettings()
    await upgraded(apgdriver, release)

    live = await inspect(apgdriver, settings)
    assert plan(live, rendered(settings), settings).statements == ()


async def test_a_retired_column_is_reported_not_dropped(apgdriver: AsyncpgDriver) -> None:
    """v0.18 is the only fixture that still carries time_in_queue."""
    settings = DBSettings()
    await upgraded(apgdriver, "v0.18.10")

    computed = plan(await inspect(apgdriver, settings), rendered(settings), settings)
    assert [one for one in computed.notes if "time_in_queue" in one]
    assert all("DROP COLUMN" not in one for one in computed.statements)


async def test_an_untouched_database_needs_the_whole_schema(apgdriver: AsyncpgDriver) -> None:
    """The other end of the range: uninstall, and every object is planned."""
    settings = DBSettings()
    await Queries(apgdriver).uninstall()

    live = await inspect(apgdriver, settings)
    statements = plan(live, rendered(settings), settings).statements
    assert sum("CREATE TABLE" in one for one in statements) == len(rendered(settings).tables)
