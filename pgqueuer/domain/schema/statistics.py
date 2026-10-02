from __future__ import annotations

from pgqueuer.domain.schema.model import TIMESTAMP, Index, Table, column, index
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import IndexName, SqlType, TableName


def statistics_table(settings: DBSettings) -> Table:
    return Table(
        name=TableName(settings.statistics_table),
        unlogged=settings.durability.config.statistics_table == "UNLOGGED",
        id_sequence_type=SqlType("bigint"),
        columns=(
            column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            column(
                "created",
                TIMESTAMP,
                not_null=True,
                # timezone() yields timestamp; assigning it shifts a non-UTC session.
                default="date_trunc('sec'::text, now())",
            ),
            column("count", "bigint", not_null=True),
            column("priority", "integer", not_null=True),
            column("status", settings.queue_status_type, not_null=True),
            column("entrypoint", "text", not_null=True),
        ),
    )


def unique_count_name(settings: DBSettings) -> IndexName:
    return IndexName(f"{settings.statistics_table}_unique_count")


# Columns of ``{statistics_table}_unique_count``: one row per bucket. Rows that collide
# on it are one bucket split in two, so an upgrade folds them before it builds the index.
UNIQUE_COUNT_KEY = (
    "priority, date_trunc('sec'::text, timezone('UTC'::text, created)), status, entrypoint"
)


def statistics_indexes(settings: DBSettings) -> tuple[Index, ...]:
    stats = settings.statistics_table
    return (
        index(
            unique_count_name(settings),
            stats,
            f"USING btree ({UNIQUE_COUNT_KEY})",
            unique=True,
        ),
    )
