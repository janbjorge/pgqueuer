from __future__ import annotations

import pytest

from pgqueuer.domain.schema import model
from pgqueuer.domain.schema.declaration import retired, target
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import TypeName

CUSTOM = DBSettings(
    prefix="acme_",
    db_schema="billing",
)


def all_names(schema: model.Schema) -> list[str]:
    return [
        *(enum.name for enum in schema.enums),
        *(table.name for table in schema.tables),
        *(index.name for index in schema.indexes),
        *(function.name for function in schema.functions),
        *(trigger.name for trigger in schema.triggers),
    ]


def test_every_object_carries_the_prefix() -> None:
    for name in all_names(target(CUSTOM)):
        assert name.startswith("acme_"), name


def test_no_object_name_is_schema_qualified() -> None:
    """Names are bare; inspect() strips the qualifier so comparison lines up."""
    for name in all_names(target(CUSTOM)):
        assert "." not in name, name


def test_collections_are_sorted() -> None:
    schema = target(DBSettings())
    assert list(schema.tables) == sorted(schema.tables, key=lambda table: table.name)
    assert list(schema.indexes) == sorted(schema.indexes, key=lambda index: index.name)


def test_target_is_recomputed_per_call() -> None:
    """Equal settings give equal but distinct values, so nothing is cached."""
    first, second = target(DBSettings()), target(DBSettings())
    assert first == second
    assert first is not second


@pytest.mark.parametrize(
    "durability, unlogged",
    [("durable", False), ("volatile", True)],
)
def test_durability_selects_table_persistence(durability: str, unlogged: bool) -> None:
    schema = target(DBSettings(durability=durability))  # type: ignore[arg-type]
    assert all(table.unlogged is unlogged for table in schema.tables)


def test_indexes_reference_declared_tables() -> None:
    schema = target(CUSTOM)
    tables = {table.name for table in schema.tables}
    assert {index.table for index in schema.indexes} <= tables


def test_resolve_substitutes_the_status_placeholder() -> None:
    settings = DBSettings()
    resolved = model.resolve(target(settings), TypeName(settings.queue_status_type))
    rendered = [
        *(column.type for table in resolved.tables for column in table.columns),
        *(column.default or "" for table in resolved.tables for column in table.columns),
        *(index.body for index in resolved.indexes),
    ]
    assert not any(model.STATUS_TYPE in text for text in rendered)
    assert any(settings.queue_status_type in text for text in rendered)


def test_retired_names_are_not_in_target() -> None:
    """Retirement is explicit; an object cannot be both declared and retired."""
    settings = DBSettings()
    schema, gone = target(settings), retired(settings)
    assert not {index.name for index in schema.indexes} & set(gone.indexes)
    assert not {enum.name for enum in schema.enums} & set(gone.types)

    declared = {(table.name, column.name) for table in schema.tables for column in table.columns}
    assert not declared & set(gone.columns)
