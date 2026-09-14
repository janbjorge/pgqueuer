from __future__ import annotations

import dataclasses

from pydantic import BaseModel

from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.domain.schema.declaration import retired, target
from pgqueuer.domain.schema.model import (
    Column,
    ColumnKind,
    ColumnRef,
    EnumType,
    Function,
    Index,
    Schema,
    Table,
    Trigger,
    UniqueConstraint,
)
from pgqueuer.domain.settings import DBSettings
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
from pgqueuer.ports.driver import Driver


class ColumnRow(BaseModel):
    tbl: str
    name: str
    type: str
    not_null: bool
    identity: str
    unlogged: bool
    default: str | None


class IndexRow(BaseModel):
    name: str
    tbl: str
    unique: bool
    definition: str


class ConstraintRow(BaseModel):
    tbl: str
    name: str
    kind: str
    columns: list[str]


class SequenceRow(BaseModel):
    tbl: str
    seqtype: str


class EnumRow(BaseModel):
    name: str
    label: str


class TriggerRow(BaseModel):
    name: str
    tbl: str
    function: str


class FunctionRow(BaseModel):
    name: str
    body: str


@dataclasses.dataclass(frozen=True)
class CatalogQueries:
    """Reads scoped to one namespace and an explicit name list, never a scan."""

    settings: DBSettings

    def namespace(self) -> str:
        return "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1) AS present"

    def columns(self) -> str:
        return f"""
        SELECT tbl.relname AS tbl, att.attname AS name,
               format_type(att.atttypid, att.atttypmod) AS type,
               att.attnotnull AS not_null, att.attidentity::text AS identity,
               tbl.relpersistence = 'u' AS unlogged,
               pg_get_expr(def.adbin, def.adrelid) AS default
        FROM pg_class tbl
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN pg_attribute att ON att.attrelid = tbl.oid
             AND att.attnum > 0 AND NOT att.attisdropped
        LEFT JOIN pg_attrdef def ON def.adrelid = tbl.oid AND def.adnum = att.attnum
        WHERE ns.nspname = {self.settings.schema_expr} AND tbl.relname = ANY($1::text[])
        ORDER BY tbl.relname, att.attnum
        """

    def indexes(self) -> str:
        """Constraint-backed indexes are excluded; the table declares those."""
        return f"""
        SELECT idx.relname AS name, tbl.relname AS tbl,
               pgi.indisunique AS unique, pg_get_indexdef(pgi.indexrelid) AS definition
        FROM pg_index pgi
        JOIN pg_class idx ON idx.oid = pgi.indexrelid
        JOIN pg_class tbl ON tbl.oid = pgi.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = {self.settings.schema_expr}
              AND tbl.relname = ANY($1::text[])
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint con WHERE con.conindid = pgi.indexrelid
              )
        ORDER BY idx.relname
        """

    def constraints(self) -> str:
        return f"""
        SELECT tbl.relname AS tbl, con.conname AS name, con.contype::text AS kind,
               array_agg(att.attname ORDER BY keys.ordinality) AS columns
        FROM pg_constraint con
        JOIN pg_class tbl ON tbl.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinality) ON TRUE
        JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = keys.attnum
        WHERE ns.nspname = {self.settings.schema_expr}
              AND tbl.relname = ANY($1::text[])
              AND con.contype IN ('p', 'u')
        GROUP BY tbl.relname, con.conname, con.contype
        ORDER BY con.conname
        """

    def sequences(self) -> str:
        return f"""
        SELECT tbl.relname AS tbl, format_type(seq.seqtypid, NULL) AS seqtype
        FROM pg_class src
        JOIN pg_depend dep ON dep.objid = src.oid AND dep.classid = 'pg_class'::regclass
        JOIN pg_class tbl ON tbl.oid = dep.refobjid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN pg_sequence seq ON seq.seqrelid = src.oid
        WHERE src.relkind = 'S' AND ns.nspname = {self.settings.schema_expr}
              AND tbl.relname = ANY($1::text[])
        ORDER BY tbl.relname
        """

    def enums(self) -> str:
        return f"""
        SELECT typ.typname AS name, lbl.enumlabel AS label
        FROM pg_type typ
        JOIN pg_enum lbl ON lbl.enumtypid = typ.oid
        JOIN pg_namespace ns ON ns.oid = typ.typnamespace
        WHERE ns.nspname = {self.settings.schema_expr} AND typ.typname = ANY($1::text[])
        ORDER BY typ.typname, lbl.enumsortorder
        """

    def triggers(self) -> str:
        return f"""
        SELECT tg.tgname AS name, tbl.relname AS tbl, fn.proname AS function
        FROM pg_trigger tg
        JOIN pg_class tbl ON tbl.oid = tg.tgrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN pg_proc fn ON fn.oid = tg.tgfoid
        WHERE NOT tg.tgisinternal AND ns.nspname = {self.settings.schema_expr}
              AND tg.tgname = ANY($1::text[])
        ORDER BY tg.tgname
        """

    def functions(self) -> str:
        return f"""
        SELECT fn.proname AS name, fn.prosrc AS body
        FROM pg_proc fn
        JOIN pg_namespace ns ON ns.oid = fn.pronamespace
        WHERE ns.nspname = {self.settings.schema_expr} AND fn.proname = ANY($1::text[])
        ORDER BY fn.proname
        """


def index_body(definition: str, db_schema: str | None) -> SqlExpression:
    """Strip ``CREATE [UNIQUE] INDEX name ON table`` off ``pg_get_indexdef``.

    The remainder is the tail the model declares. ``pg_get_indexdef`` qualifies
    names even when the schema is on ``search_path``, so drop the qualifier.
    """
    tail = definition[definition.index(" USING ") + 1 :]
    return SqlExpression(fold_timezone_calls(unqualify(tail, db_schema)))


def unqualify(text: str, db_schema: str | None) -> str:
    return text if db_schema is None else text.replace(f"{db_schema}.", "")


def closing_paren(text: str, start: int) -> int:
    """Index of the paren matching the one at ``start``, quotes respected."""
    depth, quoted, position = 0, False, start
    while position < len(text):
        character = text[position]
        if character == "'":
            quoted = not quoted
        elif not quoted and character == "(":
            depth += 1
        elif not quoted and character == ")":
            depth -= 1
            if depth == 0:
                return position
        position += 1
    raise ValueError(f"unbalanced parentheses in {text!r}")


def fold_timezone_calls(expression: str) -> str:
    """Rewrite PostgreSQL 13's ``timezone(zone, value)`` as ``AT TIME ZONE``.

    PostgreSQL 14 changed how it deparses ``AT TIME ZONE``, so one DDL reads
    back two ways across the supported majors. The model declares the 14+
    spelling and 13 is folded onto it.
    """
    marker = "timezone("
    while (start := expression.find(marker)) != -1:
        opening = start + len(marker) - 1
        closing = closing_paren(expression, opening)
        zone, _, value = expression[opening + 1 : closing].partition(", ")
        folded = f"({value} AT TIME ZONE {zone})"
        expression = expression[:start] + folded + expression[closing + 1 :]
    return expression


def column_kind(row: ColumnRow) -> ColumnKind:
    if row.identity:
        return "identity"
    if row.default is not None and row.default.startswith("nextval("):
        return "serial"
    return "plain"


async def namespace_present(
    driver: Driver,
    queries: CatalogQueries,
    settings: DBSettings,
) -> bool:
    rows = await driver.fetch(queries.namespace(), settings.db_schema)
    return bool(rows) and cell(rows[0], "present", bool)


async def inspect(driver: Driver, settings: DBSettings) -> Schema:
    """Read the installed schema, spelled as ``pg_catalog`` reports it."""
    declared, gone = target(settings), retired(settings)
    table_names = [table.name for table in declared.tables]
    index_names = [index.name for index in declared.indexes] + list(gone.indexes)
    type_names = [enum.name for enum in declared.enums] + list(gone.types)
    queries = CatalogQueries(settings)

    if settings.db_schema is not None and not await namespace_present(driver, queries, settings):
        return Schema((), (), (), (), (), namespace_exists=False)

    columns = [
        ColumnRow.model_validate(row) for row in await driver.fetch(queries.columns(), table_names)
    ]  # noqa: E501
    indexes = [
        IndexRow.model_validate(row) for row in await driver.fetch(queries.indexes(), table_names)
    ]  # noqa: E501
    constraints = [
        ConstraintRow.model_validate(row)
        for row in await driver.fetch(queries.constraints(), table_names)
    ]  # noqa: E501
    sequences = [
        SequenceRow.model_validate(row)
        for row in await driver.fetch(queries.sequences(), table_names)
    ]  # noqa: E501
    enums = [EnumRow.model_validate(row) for row in await driver.fetch(queries.enums(), type_names)]
    triggers = [
        TriggerRow.model_validate(row)
        for row in await driver.fetch(queries.triggers(), [declared.triggers[0].name])
    ]  # noqa: E501
    functions = [
        FunctionRow.model_validate(row)
        for row in await driver.fetch(queries.functions(), [declared.functions[0].name])
    ]  # noqa: E501

    return Schema(
        enums=build_enums(enums),
        tables=build_tables(columns, constraints, sequences, settings.db_schema),
        indexes=build_indexes(indexes, index_names, settings.db_schema),
        functions=tuple(Function(name=FunctionName(row.name), body=row.body) for row in functions),
        triggers=tuple(
            Trigger(
                name=TriggerName(row.name),
                table=TableName(row.tbl),
                function=FunctionName(row.function),
            )
            for row in triggers
        ),
    )


def build_enums(rows: list[EnumRow]) -> tuple[EnumType, ...]:
    names = sorted({row.name for row in rows})
    return tuple(
        EnumType(
            name=TypeName(name),
            labels=tuple(row.label for row in rows if row.name == name),
        )
        for name in names
    )


def build_indexes(
    rows: list[IndexRow],
    declared: list[IndexName],
    db_schema: str | None,
) -> tuple[Index, ...]:
    known = set(declared)
    return tuple(
        sorted(
            (
                Index(
                    name=IndexName(row.name),
                    table=TableName(row.tbl),
                    unique=row.unique,
                    body=index_body(row.definition, db_schema),
                )
                for row in rows
                if row.name in known
            ),
            key=lambda index: index.name,
        )
    )


def build_tables(
    columns: list[ColumnRow],
    constraints: list[ConstraintRow],
    sequences: list[SequenceRow],
    db_schema: str | None,
) -> tuple[Table, ...]:
    primary_keys = {
        ColumnRef(TableName(row.tbl), ColumnName(name))
        for row in constraints
        if row.kind == "p"
        for name in row.columns
    }
    unique = {
        row.tbl: tuple(
            UniqueConstraint(columns=tuple(ColumnName(name) for name in entry.columns))
            for entry in constraints
            if entry.kind == "u" and entry.tbl == row.tbl
        )
        for row in constraints
    }
    sequence_types = {row.tbl: SqlType(row.seqtype) for row in sequences}

    tables = []
    for name in sorted({row.tbl for row in columns}):
        rows = [row for row in columns if row.tbl == name]
        tables.append(
            Table(
                name=TableName(name),
                unlogged=rows[0].unlogged,
                id_sequence_type=sequence_types[name],
                unique_constraints=unique.get(name, ()),
                columns=tuple(build_column(row, primary_keys, db_schema) for row in rows),
            )
        )
    return tuple(tables)


def build_column(
    row: ColumnRow,
    primary_keys: set[ColumnRef],
    db_schema: str | None,
) -> Column:
    kind = column_kind(row)
    # A serial column reports nextval() as its default; that is the serial-ness.
    default = None if kind == "serial" else row.default
    return Column(
        name=ColumnName(row.name),
        type=SqlType(unqualify(row.type, db_schema)),
        not_null=row.not_null,
        default=(
            None
            if default is None
            else SqlExpression(fold_timezone_calls(unqualify(default, db_schema)))
        ),
        kind=kind,
        primary_key=ColumnRef(TableName(row.tbl), ColumnName(row.name)) in primary_keys,
    )
