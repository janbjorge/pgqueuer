"""Planner unit tests, no database: break one thing, expect one statement back."""

from __future__ import annotations

import dataclasses
from typing import Callable

import pytest

from pgqueuer.adapters.persistence.schema_plan import plan
from pgqueuer.domain.errors import SchemaDriftError
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.schema.model import Column, Index, Plan, Schema
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import IndexName, SqlExpression, SqlType, TableName

EMPTY = Schema(enums=(), tables=(), indexes=(), functions=(), triggers=())


def declared(settings: DBSettings) -> Schema:
    return target(settings)


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

    Checked on the leading keyword: the notify trigger fires on TRUNCATE, so
    its DDL carries the word without being destructive.
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


def changed_column(
    schema: Schema,
    table: str,
    column: str,
    edit: Callable[[Column], Column],
) -> Schema:
    """Apply *edit* to one named column, leaving the rest of the schema alone."""
    return dataclasses.replace(
        schema,
        tables=tuple(
            dataclasses.replace(
                entry,
                columns=tuple(
                    edit(item) if item.name == column else item for item in entry.columns
                ),
            )
            if entry.name == table
            else entry
            for entry in schema.tables
        ),
    )


def test_a_narrow_id_is_widened_when_enabled() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.queue_table,
        "id",
        lambda entry: dataclasses.replace(entry, type=SqlType("integer")),
    )

    assert plan(live, schema, settings).statements == (
        f"ALTER TABLE {settings.queue_table} ALTER COLUMN id TYPE bigint;",
    )


def test_a_narrow_id_is_reported_when_widening_is_disabled() -> None:
    settings = DBSettings(widen_id=False)
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.queue_table,
        "id",
        lambda entry: dataclasses.replace(entry, type=SqlType("integer")),
    )

    computed = plan(live, schema, settings)
    assert computed.statements == ()
    assert len(computed.notes) == 1
    assert "still integer" in computed.notes[0]


def test_a_legacy_status_type_is_cast_through_text() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.statistics_table,
        "status",
        lambda entry: dataclasses.replace(
            entry, type=SqlType(settings.legacy_statistics_status_type)
        ),
    )

    assert plan(live, schema, settings).statements == (
        f"ALTER TABLE {settings.statistics_table} ALTER COLUMN status "
        f"TYPE {settings.queue_status_type} USING status::TEXT::{settings.queue_status_type};",
    )


def test_an_unrecognised_type_change_refuses() -> None:
    """The planner does not guess at a cast that could lose data."""
    settings = DBSettings()
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.queue_table,
        "payload",
        lambda entry: dataclasses.replace(entry, type=SqlType("text")),
    )

    with pytest.raises(SchemaDriftError) as raised:
        plan(live, schema, settings)
    assert "payload" in str(raised.value)
    assert "text" in str(raised.value)


def test_a_kind_change_refuses() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.queue_table_log,
        "id",
        lambda entry: dataclasses.replace(entry, kind="serial", type=SqlType("integer")),
    )

    with pytest.raises(SchemaDriftError):
        plan(live, schema, settings)


def test_not_null_and_default_are_brought_into_line() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = changed_column(
        schema,
        settings.queue_table,
        "attempts",
        lambda entry: dataclasses.replace(entry, not_null=False, default=None),
    )

    assert plan(live, schema, settings).statements == (
        f"ALTER TABLE {settings.queue_table} ALTER COLUMN attempts SET NOT NULL;",
        f"ALTER TABLE {settings.queue_table} ALTER COLUMN attempts SET DEFAULT 0;",
    )


def test_a_redefined_index_is_dropped_before_it_is_rebuilt() -> None:
    settings = DBSettings()
    schema = declared(settings)
    name = f"{settings.queue_table_log}_not_aggregated"
    live = dataclasses.replace(
        schema,
        indexes=tuple(
            dataclasses.replace(entry, body=SqlExpression("USING btree (created)"))
            if entry.name == name
            else entry
            for entry in schema.indexes
        ),
    )

    statements = plan(live, schema, settings).statements
    assert statements[0] == f"DROP INDEX IF EXISTS {name};"
    assert statements[1].startswith(f"CREATE INDEX {name} ON")


def test_a_changed_function_body_is_replaced_not_recreated() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = dataclasses.replace(
        schema,
        functions=tuple(
            dataclasses.replace(entry, body="BEGIN RETURN NULL; END;") for entry in schema.functions
        ),
    )

    statements = plan(live, schema, settings).statements
    assert len(statements) == 1
    assert statements[0].startswith("CREATE OR REPLACE FUNCTION")


def test_function_bodies_compare_on_tokens_not_indentation() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = dataclasses.replace(
        schema,
        functions=tuple(
            dataclasses.replace(entry, body=f"  {entry.body}\n\n") for entry in schema.functions
        ),
    )

    assert plan(live, schema, settings).statements == ()


def test_a_retired_index_is_dropped_only_when_present() -> None:
    settings = DBSettings()
    schema = declared(settings)
    stale = Index(
        name=IndexName(f"{settings.queue_table}_heartbeat_id_id1_idx"),
        table=TableName(settings.queue_table),
        unique=False,
        body=SqlExpression("USING btree (heartbeat, id DESC)"),
    )
    live = dataclasses.replace(
        schema, indexes=tuple(sorted(schema.indexes + (stale,), key=lambda entry: entry.name))
    )

    assert plan(live, schema, settings).statements == (f"DROP INDEX IF EXISTS {stale.name};",)
    assert plan(schema, schema, settings).statements == ()


def test_a_durability_mismatch_is_a_note_not_a_rewrite() -> None:
    settings = DBSettings()
    schema = declared(settings)
    live = dataclasses.replace(
        schema,
        tables=tuple(
            dataclasses.replace(entry, unlogged=True)
            if entry.name == settings.queue_table
            else entry
            for entry in schema.tables
        ),
    )

    computed = plan(live, schema, settings)
    assert computed.statements == ()
    assert "pgq durability" in computed.notes[0]
