"""Unit tests for the absent-object planner. No database.

The planner is a pure function over two :class:`Schema` values, so each case
here removes exactly one thing from a copy of the declaration and checks that
the plan puts exactly that thing back.
"""

from __future__ import annotations

import dataclasses

from pgqueuer.adapters.persistence.schema_plan import plan
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.schema.model import Plan, Schema, resolve
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import TypeName

EMPTY = Schema(enums=(), tables=(), indexes=(), functions=(), triggers=())


def declared(settings: DBSettings) -> Schema:
    return resolve(target(settings), TypeName(settings.qualified.queue_status_type))


def without_table(schema: Schema, name: str) -> Schema:
    return dataclasses.replace(
        schema,
        tables=tuple(entry for entry in schema.tables if entry.name != name),
    )


def without_column(schema: Schema, table: str, column: str) -> Schema:
    return dataclasses.replace(
        schema,
        tables=tuple(
            dataclasses.replace(
                entry,
                columns=tuple(item for item in entry.columns if item.name != column),
            )
            if entry.name == table
            else entry
            for entry in schema.tables
        ),
    )


def test_converged_schema_plans_nothing() -> None:
    settings = DBSettings()
    schema = declared(settings)
    assert plan(schema, schema, settings) == Plan()


def test_empty_database_plans_every_object() -> None:
    settings = DBSettings()
    schema = declared(settings)
    statements = plan(EMPTY, schema, settings).statements

    assert sum(one.startswith("CREATE TYPE") for one in statements) == len(schema.enums)
    assert sum("CREATE TABLE" in one for one in statements) == len(schema.tables)
    assert sum("INDEX" in one for one in statements) == len(schema.indexes)
    assert sum(one.startswith("CREATE FUNCTION") for one in statements) == 1
    assert sum(one.startswith("CREATE TRIGGER") for one in statements) == 1


def test_objects_are_created_in_dependency_order() -> None:
    settings = DBSettings()
    statements = plan(EMPTY, declared(settings), settings).statements
    kinds = [one.split(" (")[0].split("\n")[0] for one in statements]

    def first(prefix: str) -> int:
        return next(index for index, one in enumerate(kinds) if one.startswith(prefix))

    assert first("CREATE TYPE") < first("CREATE TABLE")
    assert first("CREATE TABLE") < first("CREATE INDEX")
    assert first("CREATE FUNCTION") < first("CREATE TRIGGER")


def test_absent_schema_is_created_only_when_configured() -> None:
    scoped = DBSettings(db_schema="billing")
    absent = dataclasses.replace(EMPTY, namespace_exists=False)
    assert plan(absent, declared(scoped), scoped).statements[0] == (
        "CREATE SCHEMA IF NOT EXISTS billing;"
    )

    bare = DBSettings()
    assert not plan(absent, declared(bare), bare).statements[0].startswith("CREATE SCHEMA")


def test_absent_table_is_the_only_statement() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = without_table(schema, settings.schedules_table)

    statements = plan(live, schema, settings).statements
    assert len(statements) == 1
    assert statements[0].startswith(f"CREATE TABLE {settings.schedules_table} (")


def test_absent_column_is_added_not_the_whole_table() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = without_column(schema, settings.queue_table, "slot")

    assert plan(live, schema, settings).statements == (
        f"ALTER TABLE {settings.queue_table} ADD COLUMN slot bigint;",
    )


def test_absent_enum_label_is_added_one_statement_at_a_time() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = dataclasses.replace(
        schema,
        enums=tuple(
            dataclasses.replace(entry, labels=("queued", "picked")) for entry in schema.enums
        ),
    )

    statements = plan(live, schema, settings).statements
    assert len(statements) == 5
    assert all(one.startswith("ALTER TYPE") for one in statements)
    assert statements[0].endswith("ADD VALUE IF NOT EXISTS 'successful';")


def test_absent_unique_constraint_is_added() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = dataclasses.replace(
        schema,
        tables=tuple(
            dataclasses.replace(entry, unique_constraints=()) if entry.unique_constraints else entry
            for entry in schema.tables
        ),
    )

    assert plan(live, schema, settings).statements == (
        f"ALTER TABLE {settings.schedules_table} ADD UNIQUE (expression, entrypoint);",
    )


def test_absent_index_is_created() -> None:
    settings = DBSettings()
    schema = declared(settings)
    gone = f"{settings.queue_table}_ep_ea_idx"
    live = dataclasses.replace(
        schema,
        indexes=tuple(entry for entry in schema.indexes if entry.name != gone),
    )

    statements = plan(live, schema, settings).statements
    assert len(statements) == 1
    assert statements[0].startswith(f"CREATE INDEX {gone} ON")


def test_the_planner_only_ever_adds() -> None:
    """An invariant, not a convention.

    Checked on the leading keyword rather than by substring: the notify
    trigger fires on TRUNCATE, so its DDL contains the word without being a
    destructive statement. Retirement is planned against an explicit list, and
    a retired column is reported rather than dropped.
    """
    settings = DBSettings()
    schema = declared(settings)
    databases = (
        EMPTY,
        schema,
        without_table(schema, settings.queue_table_log),
        without_column(schema, settings.queue_table, "slot"),
    )

    for live in databases:
        for statement in plan(live, schema, settings).statements:
            assert statement.split()[0] in {"CREATE", "ALTER"}
            assert " DROP " not in statement


def test_serial_columns_are_never_added_to_an_existing_table() -> None:
    """ADD COLUMN cannot introduce a serial primary key, and never needs to."""
    settings = DBSettings()
    schema = declared(settings)
    live = without_column(schema, settings.queue_table, "id")

    assert plan(live, schema, settings).statements == ()
