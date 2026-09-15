"""Compute what a database is missing relative to the declaration.

The offline converge script in :mod:`schema_ddl` re-states everything and lets
``IF NOT EXISTS`` sort out what was already there. With a connection there is a
better answer: read the catalog, compare, and emit only what is actually
absent. An already-converged database yields an empty plan, which is what lets
``pgq upgrade`` say "already up to date" and mean it.

This module handles absent objects. Objects that exist but differ are the
harder half and are planned separately.
"""

from __future__ import annotations

from pgqueuer.adapters.persistence.schema_ddl import (
    render_column,
    render_enum,
    render_function,
    render_index,
    render_table,
    render_trigger,
)
from pgqueuer.domain.schema.model import Plan, Schema, Table
from pgqueuer.domain.settings import DBSettings


def plan_namespace(live: Schema, settings: DBSettings) -> list[str]:
    if settings.db_schema and not live.namespace_exists:
        return [f"CREATE SCHEMA IF NOT EXISTS {settings.db_schema};"]
    return []


def plan_enums(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    """Create absent enums whole; add absent labels to the ones that exist.

    A label added by ``ALTER TYPE`` cannot be used in the transaction that
    added it, so each is its own statement and the caller must not batch them.
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
    """Add the columns *declared* has and *installed* lacks.

    Serial and identity columns are skipped: a table that exists already has
    its id, and ``ADD COLUMN`` cannot introduce a serial primary key to one.
    """
    present = {entry.name for entry in installed.columns}
    qualified = settings.qualify(declared.name)
    return [
        f"ALTER TABLE {qualified} ADD COLUMN {render_column(column)};"
        for column in declared.columns
        if column.kind == "plain" and column.name not in present
    ]


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
        statements += plan_constraints(installed, table, settings)
    return statements


def plan_indexes(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    present = {entry.name for entry in live.indexes}
    return [
        render_index(index, settings) for index in declared.indexes if index.name not in present
    ]


def plan_routines(live: Schema, declared: Schema, settings: DBSettings) -> list[str]:
    functions = {entry.name for entry in live.functions}
    triggers = {entry.name for entry in live.triggers}
    statements = [
        render_function(function, settings)
        for function in declared.functions
        if function.name not in functions
    ]
    statements += [
        render_trigger(trigger, settings)
        for trigger in declared.triggers
        if trigger.name not in triggers
    ]
    return statements


def plan(live: Schema, declared: Schema, settings: DBSettings) -> Plan:
    """Statements bringing *live* up to *declared*, in dependency order.

    The schema holds the enums, the enums are referenced by table columns, the
    columns are referenced by indexes, and the trigger needs its function. No
    statement here drops anything: what is absent from the declaration but
    present in the database is left alone, and retirement is planned
    separately against an explicit list.
    """
    statements = (
        plan_namespace(live, settings)
        + plan_enums(live, declared, settings)
        + plan_tables(live, declared, settings)
        + plan_indexes(live, declared, settings)
        + plan_routines(live, declared, settings)
    )
    return Plan(statements=tuple(statements))
