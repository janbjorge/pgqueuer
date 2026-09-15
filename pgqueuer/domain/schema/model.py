from __future__ import annotations

import dataclasses
from typing import Literal, NamedTuple

from pgqueuer.domain.types import (
    ColumnName,
    FunctionName,
    IndexName,
    SqlExpression,
    SqlType,
    TableName,
    TriggerName,
    TypeName,
)

ColumnKind = Literal["plain", "serial", "identity"]

TIMESTAMP = SqlType("timestamp with time zone")


class ColumnRef(NamedTuple):
    """One column, addressed by the table it belongs to."""

    table: TableName
    column: ColumnName


class UniqueConstraint(NamedTuple):
    """Columns a table declares unique.

    Carries no name: PostgreSQL generates one, and the declaration should not
    have to predict it.
    """

    columns: tuple[ColumnName, ...]


@dataclasses.dataclass(frozen=True)
class Column:
    """A table column, spelled as ``pg_catalog`` reports it.

    ``type`` follows ``format_type`` (``timestamp with time zone``) and
    ``default`` follows ``pg_get_expr`` (``now()``).
    """

    name: ColumnName
    type: SqlType
    not_null: bool = False
    default: SqlExpression | None = None
    kind: ColumnKind = "plain"
    primary_key: bool = False


@dataclasses.dataclass(frozen=True)
class Table:
    name: TableName
    columns: tuple[Column, ...]
    unlogged: bool
    id_sequence_type: SqlType
    unique_constraints: tuple[UniqueConstraint, ...] = ()


@dataclasses.dataclass(frozen=True)
class Index:
    """A standalone index. Constraint-backed indexes belong to their table."""

    name: IndexName
    table: TableName
    unique: bool
    body: SqlExpression


@dataclasses.dataclass(frozen=True)
class EnumType:
    name: TypeName
    labels: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Function:
    """A function, ``body`` holding ``pg_proc.prosrc`` rather than full DDL."""

    name: FunctionName
    body: str


@dataclasses.dataclass(frozen=True)
class Trigger:
    name: TriggerName
    table: TableName
    function: FunctionName


@dataclasses.dataclass(frozen=True)
class Schema:
    enums: tuple[EnumType, ...]
    tables: tuple[Table, ...]
    indexes: tuple[Index, ...]
    functions: tuple[Function, ...]
    triggers: tuple[Trigger, ...]
    namespace_exists: bool = True


@dataclasses.dataclass(frozen=True)
class Plan:
    """What an upgrade would do to reach the declaration.

    ``statements`` apply in order, one per round trip. ``notes`` are what the
    upgrade will not do on its own, such as dropping a column holding data.
    """

    statements: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class Retired:
    """Objects earlier releases created that the current schema does not.

    A name only lands here when it is removed from the declaration; nothing is
    dropped for being merely absent.
    """

    indexes: tuple[IndexName, ...] = ()
    types: tuple[TypeName, ...] = ()
    columns: tuple[ColumnRef, ...] = ()


def column(
    name: str,
    sql_type: str,
    *,
    not_null: bool = False,
    default: str | None = None,
    kind: ColumnKind = "plain",
    primary_key: bool = False,
) -> Column:
    """Build a :class:`Column` from plain strings, so declarations read as DDL."""
    return Column(
        name=ColumnName(name),
        type=SqlType(sql_type),
        not_null=not_null,
        default=None if default is None else SqlExpression(default),
        kind=kind,
        primary_key=primary_key,
    )


def index(name: str, table: str, body: str, *, unique: bool = False) -> Index:
    return Index(
        name=IndexName(name),
        table=TableName(table),
        unique=unique,
        body=SqlExpression(body),
    )
