"""Diff the installed schema against the declaration and emit only the delta.

Absent objects are created, changed ones converted, and what earlier releases
left behind is cleaned up -- except a retired column, which holds data and is
reported rather than dropped. An unrecognised conversion raises rather than
guessing at a cast. A converged database yields an empty plan, which is what
lets ``pgq upgrade`` say "already up to date" and mean it.
"""

from __future__ import annotations

import zlib
from enum import Enum

from typing_extensions import assert_never

from pgqueuer.adapters.persistence.schema_ddl import (
    render_column,
    render_enum,
    render_function,
    render_index,
    render_table,
    render_trigger,
    spelled,
    widen_id_sequence,
)
from pgqueuer.domain.errors import SchemaDriftError
from pgqueuer.domain.schema.declaration import retired
from pgqueuer.domain.schema.model import Column, Plan, Schema, Table
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import SqlType


def plan_namespace(live: Schema, settings: DBSettings) -> list[str]:
    if settings.db_schema and not live.namespace_exists:
        return [f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema};"]
    return []


def plan_enums(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    """Create absent enums; add absent labels one statement at a time.

    A value added by ``ALTER TYPE`` cannot be used in the transaction that
    added it, so the caller must not batch them.
    """
    found = {entry.name: entry for entry in live.enums}
    statements: list[str] = []
    for enum in declared.enums:
        installed = found.get(enum.name)
        if installed is None:
            statements.append(render_enum(enum, settings))
            continue
        statements += [
            f"ALTER TYPE {settings.qualify(enum.name)} ADD VALUE IF NOT EXISTS '{label}';"
            for label in enum.labels
            if label not in installed.labels
        ]
    return statements


def plan_columns(installed: Table, declared: Table, settings: DBSettings) -> list[str]:
    """Add absent columns, skipping serial and identity ones.

    A table that exists already has its id, and ``ADD COLUMN`` cannot add a
    serial primary key to one.
    """
    present = {entry.name for entry in installed.columns}
    qualified = settings.qualify(declared.name)
    return [
        f"ALTER TABLE {qualified} ADD COLUMN {render_column(column, settings)};"
        for column in declared.columns
        if column.kind == "plain" and column.name not in present
    ]


class ColumnChange(Enum):
    """How an installed column differs in type from the one declared."""

    widen_id = "widen_id"
    into_status_enum = "into_status_enum"
    unsupported = "unsupported"


def classify(installed: Column, declared: Column, settings: DBSettings) -> ColumnChange:
    """Only the two conversions PgQueuer has shipped are recognised."""
    if installed.type == SqlType("integer") and declared.type == SqlType("bigint"):
        return ColumnChange.widen_id
    if declared.type == SqlType(settings.queue_status_type):
        return ColumnChange.into_status_enum
    return ColumnChange.unsupported


def plan_column_type(
    installed: Column,
    declared: Column,
    table: Table,
    settings: DBSettings,
) -> list[str]:
    """Convert a column whose installed type is not the declared one.

    A kind change is a data question rather than a DDL one, so it is refused.
    """
    qualified = settings.qualify(table.name)
    drift = SchemaDriftError(
        table=table.name,
        column=declared.name,
        installed=installed.type,
        declared=declared.type,
    )
    if installed.kind != declared.kind:
        raise drift

    change = classify(installed, declared, settings)
    if change is ColumnChange.widen_id:
        # Rewrites the table under ACCESS EXCLUSIVE, so it is opt-out.
        if not settings.widen_id:
            return []
        return [f"ALTER TABLE {qualified} ALTER COLUMN {declared.name} TYPE bigint;"]
    if change is ColumnChange.into_status_enum:
        status = settings.qualified.queue_status_type
        return [
            f"ALTER TABLE {qualified} ALTER COLUMN {declared.name} "
            f"TYPE {status} USING {declared.name}::TEXT::{status};"
        ]
    if change is ColumnChange.unsupported:
        raise drift
    assert_never(change)


def skipped_widening(installed: Table, declared: Table, settings: DBSettings) -> list[str]:
    """Notes for id columns left narrow because ``widen_id`` is off."""
    if settings.widen_id:
        return []
    found = {entry.name: entry for entry in installed.columns}
    notes = []
    for column in declared.columns:
        live_column = found.get(column.name)
        if live_column is None or live_column.type == column.type:
            continue
        if classify(live_column, column, settings) is ColumnChange.widen_id:
            notes.append(
                f"{declared.name}.{column.name} is still {live_column.type} and widening is "
                f"disabled. Run ALTER TABLE {settings.qualify(declared.name)} "
                f"ALTER COLUMN {column.name} TYPE bigint out of band."
            )
    return notes


def plan_column_constraints(
    installed: Column,
    declared: Column,
    table: Table,
    settings: DBSettings,
) -> list[str]:
    """Bring NOT NULL and DEFAULT into line; both are catalog-only changes.

    ``SET NOT NULL`` fails on an existing null, which is the correct outcome:
    PgQueuer must not fabricate a value to satisfy its own declaration.
    """
    qualified = settings.qualify(table.name)
    statements: list[str] = []
    if installed.not_null != declared.not_null:
        verb = "SET NOT NULL" if declared.not_null else "DROP NOT NULL"
        statements.append(f"ALTER TABLE {qualified} ALTER COLUMN {declared.name} {verb};")
    if installed.default != declared.default:
        change = (
            "DROP DEFAULT"
            if declared.default is None
            else f"SET DEFAULT {spelled(declared.default, settings)}"
        )
        statements.append(f"ALTER TABLE {qualified} ALTER COLUMN {declared.name} {change};")
    return statements


def plan_changed_columns(installed: Table, declared: Table, settings: DBSettings) -> list[str]:
    found = {entry.name: entry for entry in installed.columns}
    statements: list[str] = []
    for column in declared.columns:
        live_column = found.get(column.name)
        if live_column is None or live_column == column:
            continue
        if live_column.type != column.type:
            statements += plan_column_type(live_column, column, declared, settings)
        statements += plan_column_constraints(live_column, column, declared, settings)
    return statements


def plan_sequence(installed: Table, declared: Table, settings: DBSettings) -> list[str]:
    """Widen the id sequence, which ``ALTER COLUMN TYPE`` leaves capped.

    Resolved through ``pg_get_serial_sequence`` rather than assuming
    ``<table>_id_seq``: the catalog gives the sequence's type, not its name.
    """
    if installed.id_sequence_type == declared.id_sequence_type or not settings.widen_id:
        return []
    return [widen_id_sequence(declared.name, settings)]


def plan_constraints(installed: Table, declared: Table, settings: DBSettings) -> list[str]:
    present = set(installed.unique_constraints)
    qualified = settings.qualify(declared.name)
    return [
        f"ALTER TABLE {qualified} ADD UNIQUE ({', '.join(unique.columns)});"
        for unique in declared.unique_constraints
        if unique not in present
    ]


def plan_tables(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    found = {entry.name: entry for entry in live.tables}
    statements: list[str] = []
    for table in declared.tables:
        installed = found.get(table.name)
        if installed is None:
            statements.append(render_table(table, settings))
            continue
        statements += plan_columns(installed, table, settings)
        statements += plan_changed_columns(installed, table, settings)
        statements += plan_sequence(installed, table, settings)
        statements += plan_constraints(installed, table, settings)
    return statements


def table_notes(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    found = {entry.name: entry for entry in live.tables}
    notes = durability_notes(live, declared)
    for table in declared.tables:
        installed = found.get(table.name)
        if installed is not None:
            notes += skipped_widening(installed, table, settings)
    return notes


def plan_indexes(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    """Create absent indexes; drop and rebuild the ones defined differently.

    A live index PgQueuer does not declare is somebody else's, and is left alone.
    """
    found = {entry.name: entry for entry in live.indexes}
    statements: list[str] = []
    for index in declared.indexes:
        installed = found.get(index.name)
        if installed == index:
            continue
        if installed is not None:
            statements.append(f"DROP INDEX IF EXISTS {settings.qualify(index.name)};")
        statements.append(render_index(index, settings))
    return statements


def collapsed(body: str) -> str:
    """Function bodies compare on their tokens, not their indentation."""
    return " ".join(body.split())


def plan_routines(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    functions = {entry.name: collapsed(entry.body) for entry in live.functions}
    triggers = {entry.name: entry for entry in live.triggers}
    statements: list[str] = []
    for function in declared.functions:
        installed = functions.get(function.name)
        if installed == collapsed(function.body):
            continue
        statements.append(render_function(function, settings, replace=installed is not None))
    for trigger in declared.triggers:
        if triggers.get(trigger.name) == trigger:
            continue
        if trigger.name in triggers:
            statements.append(
                f"DROP TRIGGER IF EXISTS {trigger.name} ON {settings.qualify(trigger.table)};"
            )
        statements.append(render_trigger(trigger, settings))
    return statements


def plan_retired(live: Schema, settings: DBSettings) -> Plan:
    """Clean up what earlier releases created and this one does not declare.

    Conditional on the object being there: a database already cleaned up must
    plan nothing, or no upgrade could report itself converged.
    """
    gone = retired(settings)
    tables = {entry.name: entry for entry in live.tables}
    present = {entry.name for entry in live.indexes}
    statements = [
        f"DROP INDEX IF EXISTS {settings.qualify(name)};"
        for name in gone.indexes
        if name in present
    ]
    notes: list[str] = []

    for reference in gone.columns:
        table = tables.get(reference.table)
        if table is None:
            continue
        installed = next((entry for entry in table.columns if entry.name == reference.column), None)
        if installed is None:
            continue
        if installed.not_null:
            statements.append(
                f"ALTER TABLE {settings.qualify(reference.table)} "
                f"ALTER COLUMN {reference.column} DROP NOT NULL;"
            )
        notes.append(
            f"{reference.table}.{reference.column} is no longer used by PgQueuer. Once you no "
            f"longer need the data: ALTER TABLE {settings.qualify(reference.table)} "
            f"DROP COLUMN {reference.column};"
        )

    return Plan(statements=tuple(statements), notes=tuple(notes))


def plan_retired_types(live: Schema, settings: DBSettings) -> list[str]:
    """Last of all: a column references one until its retype has run."""
    present = {entry.name for entry in live.enums}
    return [
        f"DROP TYPE IF EXISTS {settings.qualify(name)};"
        for name in retired(settings).types
        if name in present
    ]


def durability_notes(live: Schema, declared: Schema) -> list[str]:
    """Durability is never changed here; rewriting a table is ``pgq durability``."""
    found = {entry.name: entry for entry in live.tables}
    return [
        f"{table.name} is {'UNLOGGED' if found[table.name].unlogged else 'LOGGED'} but declared "
        f"{'UNLOGGED' if table.unlogged else 'LOGGED'}. Run pgq durability to change it."
        for table in declared.tables
        if table.name in found and found[table.name].unlogged != table.unlogged
    ]


def plan(live: Schema, declared: Schema, settings: DBSettings) -> Plan:
    """Statements bringing *live* up to *declared*, in dependency order.

    Column work precedes index work so ``ALTER COLUMN TYPE`` never rebuilds an
    index about to be dropped; retired types come last, since a column may
    still reference one. Nothing here drops a table or a column: an object the
    declaration does not name is somebody else's.
    """
    gone = plan_retired(live, settings)
    statements = (
        plan_namespace(live, settings)
        + plan_enums(live, declared, settings)
        + plan_tables(live, declared, settings)
        + list(gone.statements)
        + plan_indexes(live, declared, settings)
        + plan_routines(live, declared, settings)
        + plan_retired_types(live, settings)
    )
    notes = table_notes(live, declared, settings) + list(gone.notes)
    return Plan(statements=tuple(statements), notes=tuple(notes))


def advisory_key(settings: DBSettings) -> int:
    """Lock number for one installation, keyed on the qualified queue table.

    ``zlib.crc32`` rather than ``hash()``: string hashing is randomised per
    process, and two concurrent upgrades must arrive at the same number.
    """
    return zlib.crc32(settings.qualified.queue_table.encode())
