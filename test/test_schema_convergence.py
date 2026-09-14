"""Upgrading a database installed by an old release must reach the target schema.

Each fixture under ``schema_releases/`` is the verbatim install DDL of a
release whose schema differed from the one before it, generated once from the
tag and never regenerated -- the point is what those databases actually look
like in the field, not what we would install for them today.

``test_schema_inspect.py`` proves a *fresh* install matches the model. These
prove the other entry point: what a database that has been around since v0.18
looks like after ``pgq upgrade``, and what still does not work there.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import pytest

from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.schema.model import Schema, Table
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import QueueEntrypoint, QueueManagerId
from pgqueuer.queries import EntrypointExecutionParameter, Queries
from test.helpers import collapse, declared_schema

RELEASES_DIR = Path(__file__).parent / "schema_releases"
RELEASES = sorted(path.stem for path in RELEASES_DIR.glob("*.sql"))

# Objects the append-only upgrade stream leaves differing from the declaration.
# A subset check, not equality: PostgreSQL 13 normalises the two timezone
# spellings to one, so it reaches the declaration where 14+ does not.
#
#   pgqueuer_statistics.created / _unique_count
#       Installs before this release wrote DATE_TRUNC('sec', NOW() at time
#       zone 'UTC'). PostgreSQL 14+ reports back the syntax that was written,
#       so those databases read as AT TIME ZONE where the model says
#       timezone(). Same meaning, different text, and upgrade redefines
#       neither.
#
#   pgqueuer_log (v0.18 only)
#       The upgrade stream creates it UNLOGGED unconditionally, ignoring
#       settings.durability, which install respects.
#
# On v0.18 the statistics index additionally still keys on time_in_queue, and
# that one is not cosmetic: see test_upgraded_release_aggregates_statistics.
# Shrinking this set to empty is what ADR-0016's computed planner is for.
UNREPAIRED = {
    "column pgqueuer_statistics.created",
    "index pgqueuer_statistics_unique_count",
    "table pgqueuer_log",
}

# The v0.18 statistics index keys on date_trunc('sec', time_in_queue) as well,
# so it cannot back the aggregation's ON CONFLICT specification, and nothing in
# the upgrade stream rebuilds it.
AGGREGATION_BROKEN = "v0.18.10"


class Gap(NamedTuple):
    """One declared object the live schema does not match.

    ``target`` names the object and nothing else, so the expected-drift set
    stays stable across PostgreSQL versions; ``detail`` carries the spelling
    for the failure message.
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
async def test_upgrade_leaves_only_the_known_drift(
    apgdriver: AsyncpgDriver,
    release: str,
) -> None:
    """Every declared object is reached, bar the drift UNREPAIRED names.

    A new entry in the failure output means the upgrade stream fell further
    behind the declaration -- the failure mode this schema model exists to
    make impossible.
    """
    settings = DBSettings()
    await upgraded(apgdriver, release)

    gaps = shortfall(collapse(await inspect(apgdriver, settings)), declared_schema(settings))
    assert [gap for gap in gaps if gap.target not in UNREPAIRED] == []


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

    v0.18 raises ``there is no unique or exclusion constraint matching the ON
    CONFLICT specification``: its index still keys on ``time_in_queue``.
    """
    if release == AGGREGATION_BROKEN:
        pytest.xfail("v0.18 statistics index still keys on time_in_queue")

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
