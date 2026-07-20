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
from test.helpers import id_data_type, queries_for, simulate_legacy_serial

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

    q = queries_for(apgdriver, CUSTOM)
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
