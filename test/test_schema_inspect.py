from __future__ import annotations

import logging
import re

import pytest

from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.adapters.persistence.schema_plan import missing
from pgqueuer.core.qm import QueueManager
from pgqueuer.core.sm import SchedulerManager
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.errors import PgqException
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.schema.model import MissingObject, MissingObjects, Schema
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import collapse, declared_schema, queries_for


async def test_fresh_install_inspects_equal_to_target(apgdriver: AsyncpgDriver) -> None:
    """The model is canonical: what install creates is what target() declares.

    A failure here prints the spelling PostgreSQL reports; paste it into the
    declaration. This is the only thing keeping the model from rotting.
    """
    settings = DBSettings()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


@pytest.mark.parametrize("durability", ["balanced", "volatile"])
async def test_durability_round_trips(apgdriver: AsyncpgDriver, durability: str) -> None:
    settings = DBSettings(durability=durability)  # type: ignore[arg-type]
    await Queries(apgdriver).uninstall()
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


@pytest.mark.parametrize(
    "settings",
    [
        pytest.param(DBSettings(prefix="acme_", db_schema="billing"), id="scoped"),
        pytest.param(
            DBSettings(prefix="acme_", db_schema="billing", queue_status_type="status"),
            id="enum-named-like-its-column",
        ),
    ],
)
async def test_prefixed_and_schema_scoped_install(
    apgdriver: AsyncpgDriver,
    settings: DBSettings,
) -> None:
    await queries_for(apgdriver, settings).install()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)


@pytest.mark.parametrize(
    "settings",
    [
        pytest.param(DBSettings(prefix="Acme_"), id="mixed-case-prefix"),
        pytest.param(DBSettings(prefix="acme_", queue_table="AcmeJobs"), id="mixed-case-name"),
        pytest.param(DBSettings(prefix="acme_"), id="lowercase-prefix"),
    ],
)
async def test_mixed_case_names_are_found_after_install(
    apgdriver: AsyncpgDriver,
    settings: DBSettings,
) -> None:
    """The DDL folds unquoted names to lowercase; the catalog lookups must still find them."""
    queries = queries_for(apgdriver, settings)
    assert not await queries.schema_is_installed()
    await queries.install()

    assert await queries.has_table(settings.queue_table)
    assert await queries.schema_is_installed()
    assert collapse(await inspect(apgdriver, settings)) == declared_schema(settings)
    assert (await queries.plan_upgrade()).statements == ()
    await QueueManager(queries).verify_structure()


async def test_verify_structure_refuses_a_missing_trigger(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings()
    await apgdriver.execute(f"DROP TRIGGER {settings.trigger} ON {settings.queue_table}")
    assert (await Queries(apgdriver).plan_upgrade()).statements != ()
    with pytest.raises(RuntimeError, match=settings.trigger):
        await QueueManager(Queries(apgdriver)).verify_structure()


async def test_verify_schema_passes_on_a_fresh_install(apgdriver: AsyncpgDriver) -> None:
    assert await Queries(apgdriver).missing_schema() == MissingObjects()
    await Queries(apgdriver).verify_schema()


@pytest.mark.parametrize(
    ("ddl", "named"),
    [
        pytest.param(
            "ALTER TABLE {queue} DROP COLUMN headers", "column '{queue}.headers'", id="column"
        ),
        pytest.param("DROP TRIGGER {trigger} ON {queue}", "trigger '{trigger}'", id="trigger"),
        pytest.param(
            "DROP INDEX {queue}_unique_dedupe_key",
            "index '{queue}_unique_dedupe_key'",
            id="unique-index",
        ),
    ],
)
async def test_verify_schema_names_a_missing_object(
    apgdriver: AsyncpgDriver,
    ddl: str,
    named: str,
) -> None:
    settings = DBSettings()
    names = {"queue": settings.queue_table, "trigger": settings.trigger}
    await apgdriver.execute(ddl.format(**names))
    with pytest.raises(RuntimeError, match=re.escape(named.format(**names))):
        await Queries(apgdriver).verify_schema()


async def test_a_missing_non_unique_index_only_warns(
    apgdriver: AsyncpgDriver,
    caplog: pytest.LogCaptureFixture,
) -> None:
    index = f"{DBSettings().queue_table}_ep_ea_idx"
    await apgdriver.execute(f"DROP INDEX {index}")
    with caplog.at_level(logging.WARNING, logger="pgqueuer"):
        await QueueManager(Queries(apgdriver)).verify_structure()
    assert index in caplog.text


def test_an_absent_table_stands_in_for_what_it_holds() -> None:
    declared = target(DBSettings())
    found = missing(Schema((), (), (), (), ()), declared)
    assert found.speed_only == ()
    assert found.blocking == (
        *(MissingObject("table", table.name) for table in declared.tables),
        *(MissingObject("enum", enum.name) for enum in declared.enums),
        *(MissingObject("function", function.name) for function in declared.functions),
    )


async def test_scheduler_refuses_a_missing_trigger(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings()
    await apgdriver.execute(f"DROP TRIGGER {settings.trigger} ON {settings.queue_table}")
    with pytest.raises(RuntimeError, match=settings.trigger):
        await SchedulerManager(Queries(apgdriver)).run()


async def test_a_narrow_id_column_still_starts(apgdriver: AsyncpgDriver) -> None:
    """The gate checks presence, so a schema upgrade would still change is accepted."""
    queries = Queries(apgdriver)
    await apgdriver.execute(
        f"ALTER TABLE {queries.qbe.settings.queue_table} ALTER COLUMN id TYPE integer"
    )
    await QueueManager(queries).verify_structure()
    assert (await queries.plan_upgrade()).statements != ()


async def test_missing_namespace_is_reported(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings(db_schema="absent_schema")
    schema = await inspect(apgdriver, settings)
    assert schema.namespace_exists is False
    assert schema.tables == ()


async def test_a_table_owning_no_sequence_is_inspected(apgdriver: AsyncpgDriver) -> None:
    """OWNED BY NONE leaves the default in place but the table without a sequence."""
    settings = DBSettings()
    await apgdriver.execute(f"ALTER SEQUENCE {settings.schedules_table}_id_seq OWNED BY NONE")
    queries = Queries(apgdriver)

    assert await queries.schema_is_installed()
    assert (await queries.plan_upgrade()).statements == ()


async def test_queue_table_outside_current_schema_stops_inspect(apgdriver: AsyncpgDriver) -> None:
    """The install check stops when search_path resolves the queue table to a later schema."""
    await apgdriver.execute("CREATE SCHEMA app; SET search_path TO app, public")
    with pytest.raises(PgqException, match="resolves to the schema public"):
        await Queries(apgdriver).schema_is_installed()
