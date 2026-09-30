"""Rebuilding a redefined index must never leave a database without it.

Upgrade used to drop the old index and then build the new one. When the build
failed the index was gone, and every rerun failed the same way. A v0.18
database with statistics history always failed: its buckets were split on
``time_in_queue``, so they collide under the current unique key.
"""

from __future__ import annotations

import asyncpg
import pytest

from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.adapters.persistence.schema_plan import plan, rebuild_name
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import install_release


async def seed_split_buckets(driver: AsyncpgDriver) -> None:
    """Three rows of one bucket, split on time_in_queue, and one bucket alone."""
    await driver.execute(
        """
        INSERT INTO pgqueuer_statistics
            (created, count, priority, time_in_queue, status, entrypoint)
        VALUES
            ('2024-01-01 00:00:00.100+00', 1, 0, '1 second', 'successful', 'split'),
            ('2024-01-01 00:00:00.200+00', 2, 0, '2 seconds', 'successful', 'split'),
            ('2024-01-01 00:00:00.300+00', 4, 0, '3 seconds', 'successful', 'split'),
            ('2024-01-01 00:00:00.000+00', 5, 0, '1 second', 'successful', 'alone')
        """
    )


async def buckets(driver: AsyncpgDriver) -> list[tuple[str, int]]:
    rows = await driver.fetch(
        "SELECT entrypoint, count FROM pgqueuer_statistics ORDER BY entrypoint, id"
    )
    return [(cell(row, "entrypoint", str), cell(row, "count", int)) for row in rows]


async def index_definition(driver: AsyncpgDriver, name: str) -> str | None:
    rows = await driver.fetch(
        "SELECT pg_get_indexdef(to_regclass($1)) AS definition",
        name,
    )
    return None if rows[0]["definition"] is None else cell(rows[0], "definition", str)


def replacement_for(name: str) -> str:
    """Where upgrade builds the new definition of index *name* before the swap."""
    return rebuild_name(next(entry for entry in target(DBSettings()).indexes if entry.name == name))


async def planned(driver: AsyncpgDriver) -> tuple[str, ...]:
    settings = DBSettings()
    return plan(await inspect(driver, settings), target(settings), settings).statements


async def test_upgrade_folds_split_statistics_buckets(apgdriver: AsyncpgDriver) -> None:
    await install_release(apgdriver, "v0.18.10")
    await seed_split_buckets(apgdriver)

    await Queries(apgdriver).upgrade()

    assert await buckets(apgdriver) == [("alone", 5), ("split", 7)]
    assert await planned(apgdriver) == ()


async def test_folding_keeps_the_oldest_row_of_a_bucket(apgdriver: AsyncpgDriver) -> None:
    await install_release(apgdriver, "v0.18.10")
    await seed_split_buckets(apgdriver)
    rows = await apgdriver.fetch(
        "SELECT min(id) AS id FROM pgqueuer_statistics WHERE entrypoint = 'split'"
    )
    oldest = cell(rows[0], "id", int)

    await Queries(apgdriver).upgrade()

    rows = await apgdriver.fetch("SELECT id FROM pgqueuer_statistics WHERE entrypoint = 'split'")
    assert [cell(row, "id", int) for row in rows] == [oldest]


async def test_upgrade_recovers_a_database_left_without_the_index(
    apgdriver: AsyncpgDriver,
) -> None:
    """What the drop-then-build upgrade left behind: rows split, index gone."""
    await install_release(apgdriver, "v0.18.10")
    await seed_split_buckets(apgdriver)
    await apgdriver.execute("DROP INDEX pgqueuer_statistics_unique_count")

    await Queries(apgdriver).upgrade()

    assert await buckets(apgdriver) == [("alone", 5), ("split", 7)]
    assert await index_definition(apgdriver, "pgqueuer_statistics_unique_count") is not None


async def test_aggregation_adds_to_a_folded_bucket(apgdriver: AsyncpgDriver) -> None:
    """The folded row is the one the aggregation upsert then conflicts on."""
    await install_release(apgdriver, "v0.18.10")
    await seed_split_buckets(apgdriver)
    queries = Queries(apgdriver)
    await queries.upgrade()

    await apgdriver.execute(
        """
        INSERT INTO pgqueuer_statistics (created, count, priority, status, entrypoint)
        VALUES ('2024-01-01 00:00:00.900+00', 10, 0, 'successful', 'split')
        ON CONFLICT (
            priority, date_trunc('sec', created AT TIME ZONE 'UTC'), status, entrypoint
        ) DO UPDATE SET count = pgqueuer_statistics.count + EXCLUDED.count
        """
    )

    assert await buckets(apgdriver) == [("alone", 5), ("split", 17)]


async def test_a_failed_rebuild_keeps_the_old_index(apgdriver: AsyncpgDriver) -> None:
    """Duplicates in a job index are not folded; the upgrade stops, index intact."""
    await apgdriver.execute("DROP INDEX pgqueuer_picked_slot_idx")
    await apgdriver.execute(
        "CREATE INDEX pgqueuer_picked_slot_idx ON pgqueuer (entrypoint, slot) "
        "WHERE status = 'picked' AND slot IS NOT NULL"
    )
    await apgdriver.execute(
        "INSERT INTO pgqueuer (priority, status, entrypoint, slot) "
        "VALUES (0, 'picked', 'ep', 1), (0, 'picked', 'ep', 1)"
    )
    before = await index_definition(apgdriver, "pgqueuer_picked_slot_idx")

    with pytest.raises(asyncpg.UniqueViolationError):
        await Queries(apgdriver).upgrade()

    assert await index_definition(apgdriver, "pgqueuer_picked_slot_idx") == before
    stranded = replacement_for("pgqueuer_picked_slot_idx")
    assert await index_definition(apgdriver, stranded) is None
    rows = await apgdriver.fetch("SELECT count(*) AS n FROM pgqueuer")
    assert cell(rows[0], "n", int) == 2


async def test_a_failed_rebuild_succeeds_once_the_data_is_fixed(
    apgdriver: AsyncpgDriver,
) -> None:
    await apgdriver.execute("DROP INDEX pgqueuer_picked_slot_idx")
    await apgdriver.execute(
        "CREATE INDEX pgqueuer_picked_slot_idx ON pgqueuer (entrypoint, slot) "
        "WHERE status = 'picked' AND slot IS NOT NULL"
    )
    await apgdriver.execute(
        "INSERT INTO pgqueuer (priority, status, entrypoint, slot) "
        "VALUES (0, 'picked', 'ep', 1), (0, 'picked', 'ep', 1)"
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await Queries(apgdriver).upgrade()

    await apgdriver.execute("DELETE FROM pgqueuer WHERE id = (SELECT max(id) FROM pgqueuer)")
    await Queries(apgdriver).upgrade()

    assert await planned(apgdriver) == ()
    definition = await index_definition(apgdriver, "pgqueuer_picked_slot_idx")
    assert definition is not None and definition.startswith("CREATE UNIQUE INDEX")


async def test_an_interrupted_rebuild_is_resumed(apgdriver: AsyncpgDriver) -> None:
    """A replacement left behind by a run cut off after its build is cleared first."""
    name = "pgqueuer_log_not_aggregated"
    replacement = replacement_for(name)
    await apgdriver.execute(f"DROP INDEX {name}")
    await apgdriver.execute(f"CREATE INDEX {name} ON pgqueuer_log (created)")
    await apgdriver.execute(f"CREATE INDEX {replacement} ON pgqueuer_log (job_id)")

    await Queries(apgdriver).upgrade()

    assert await planned(apgdriver) == ()
    assert await index_definition(apgdriver, replacement) is None
    definition = await index_definition(apgdriver, name)
    assert definition is not None and "WHERE (NOT aggregated)" in definition


async def test_a_clean_statistics_table_is_left_alone(apgdriver: AsyncpgDriver) -> None:
    """Distinct buckets pass through a rebuild untouched."""
    await install_release(apgdriver, "v1.4.0")
    await apgdriver.execute(
        """
        INSERT INTO pgqueuer_statistics (created, count, priority, status, entrypoint)
        VALUES ('2024-01-01 00:00:00+00', 3, 0, 'successful', 'a'),
               ('2024-01-01 00:00:01+00', 4, 0, 'successful', 'a'),
               ('2024-01-01 00:00:00+00', 5, 0, 'exception', 'a')
        """
    )
    before = await buckets(apgdriver)

    await Queries(apgdriver).upgrade()

    assert await buckets(apgdriver) == before
    assert await planned(apgdriver) == ()
