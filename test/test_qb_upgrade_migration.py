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
from test.helpers import id_data_type, simulate_legacy_serial

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


async def schema_snapshot(driver: db.Driver, settings: DBSettings) -> dict[str, object]:
    tables = [
        settings.queue_table,
        settings.queue_table_log,
        settings.statistics_table,
        settings.schedules_table,
    ]
    columns = await driver.fetch(
        """SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ANY($1)
        ORDER BY table_name, column_name""",
        tables,
    )
    # The upgrade path creates {queue_table}_heartbeat_id_id1_idx for legacy
    # installs; fresh installs never had it. Pre-existing divergence, excluded.
    indexes = await driver.fetch(
        """SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = ANY($1)
          AND indexname != $2
        ORDER BY indexname""",
        tables,
        f"{settings.queue_table}_heartbeat_id_id1_idx",
    )
    function = await driver.fetch(
        """SELECT pg_get_functiondef(p.oid) AS def
        FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = current_schema() AND p.proname = $1""",
        settings.function,
    )
    persistence = await driver.fetch(
        """SELECT c.relname, c.relpersistence, c.reloptions
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema() AND c.relname = ANY($1) AND c.relkind = 'r'
        ORDER BY c.relname""",
        tables,
    )
    return {
        "columns": columns,
        "indexes": indexes,
        "function": function,
        "persistence": persistence,
    }


async def test_upgrade_on_fresh_install_is_schema_noop(apgdriver: db.Driver) -> None:
    """Upgrade DDL converges to exactly what fresh install produces."""
    settings = DBSettings()
    before = await schema_snapshot(apgdriver, settings)

    await queries.Queries(apgdriver).upgrade()

    after = await schema_snapshot(apgdriver, settings)
    assert before == after


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
        await simulate_legacy_serial(apgdriver, table)

    await q.upgrade()

    for table in CUSTOM_WIDENED_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"
    # The prefixed queue is fully usable after migrating.
    ids = await q.enqueue(["ep"], [b"x"], [0])
    assert len(ids) == 1


async def test_widen_id_setting_controls_the_widen(apgdriver: db.Driver) -> None:
    """settings.widen_id=False leaves the int4 column untouched; the default widens it."""
    table = DBSettings().queue_table
    await simulate_legacy_serial(apgdriver, table)

    no_widen = DBSettings(widen_id=False)
    q_no_widen = queries.Queries(apgdriver, qbe=qb.QueryBuilderEnvironment(settings=no_widen))
    await q_no_widen.upgrade()
    assert await id_data_type(apgdriver, table) == "integer"

    await queries.Queries(apgdriver).upgrade()
    assert await id_data_type(apgdriver, table) == "bigint"
