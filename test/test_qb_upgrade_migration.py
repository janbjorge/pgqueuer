"""Effect-based checks for ``pgq upgrade``.

These run the migration against a real database and assert on the resulting
schema/behavior, never on the generated SQL text. Asserting on SQL strings
only proves what we wrote, not what Postgres does with it.
"""

from __future__ import annotations

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import Channel

SETTINGS = DBSettings()
WIDENED_TABLES = [
    SETTINGS.queue_table,
    SETTINGS.statistics_table,
    SETTINGS.schedules_table,
]

# Object names sharing no substring with the defaults: if any migration
# statement hardcoded a default name, upgrading this schema would miss its
# tables (the default schema is dropped first) and the test would fail.
CUSTOM = DBSettings(
    channel=Channel("acme_ch"),
    function="acme_fn_changed",
    statistics_table="acme_stats",
    queue_status_type="acme_status",
    queue_table="acme_jobs",
    queue_table_log="acme_jobs_log",
    trigger="acme_tg_changed",
    schedules_table="acme_schedules",
)
CUSTOM_WIDENED_TABLES = [
    CUSTOM.queue_table,
    CUSTOM.statistics_table,
    CUSTOM.schedules_table,
]


def _custom_queries(driver: db.Driver) -> queries.Queries:
    return queries.Queries(
        driver,
        qbe=qb.QueryBuilderEnvironment(settings=CUSTOM),
        qbq=qb.QueryQueueBuilder(settings=CUSTOM),
        qbs=qb.QuerySchedulerBuilder(settings=CUSTOM),
    )


async def _id_data_type(driver: db.Driver, table: str) -> str:
    rows = await driver.fetch(
        """SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
          AND column_name = 'id';""",
        table,
    )
    return rows[0]["data_type"]


async def _simulate_legacy_serial(driver: db.Driver, table: str) -> None:
    """Recreate the pre-#671 state: int4 column backed by an int4 SERIAL sequence."""
    seq = f"{table}_id_seq"
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id DROP IDENTITY IF EXISTS;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER;")
    await driver.execute(f"CREATE SEQUENCE {seq} AS INTEGER OWNED BY {table}.id;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id SET DEFAULT nextval('{seq}');")


async def test_upgrade_reruns_cleanly_on_current_schema(apgdriver: db.Driver) -> None:
    """pgq upgrade is idempotent: re-running on an up-to-date schema is a no-op that still works."""
    q = queries.Queries(apgdriver)

    await q.upgrade()
    await q.upgrade()

    ids = await q.enqueue(["ep"], [b"x"], [0])
    assert len(ids) == 1


async def test_upgrade_targets_configured_names(apgdriver: db.Driver) -> None:
    """The migration operates on the configured (prefixed) names, not hardcoded defaults."""
    # Drop the default schema so a leaked default name would fail loudly rather
    # than silently hit the template's default-named tables.
    await queries.Queries(apgdriver).uninstall()

    q = _custom_queries(apgdriver)
    await q.install()
    for table in CUSTOM_WIDENED_TABLES:
        await _simulate_legacy_serial(apgdriver, table)

    await q.upgrade()

    for table in CUSTOM_WIDENED_TABLES:
        assert await _id_data_type(apgdriver, table) == "bigint"
    # The prefixed queue is fully usable after migrating.
    ids = await q.enqueue(["ep"], [b"x"], [0])
    assert len(ids) == 1


async def test_widen_id_flag_controls_the_widen(apgdriver: db.Driver) -> None:
    """widen_id=False leaves the int4 column untouched; widen_id=True migrates it to bigint."""
    q = queries.Queries(apgdriver)
    table = SETTINGS.queue_table
    await _simulate_legacy_serial(apgdriver, table)

    await q.upgrade(widen_id=False)
    assert await _id_data_type(apgdriver, table) == "integer"

    await q.upgrade(widen_id=True)
    assert await _id_data_type(apgdriver, table) == "bigint"
