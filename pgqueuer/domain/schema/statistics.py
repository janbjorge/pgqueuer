from __future__ import annotations

from pgqueuer.domain.schema.model import STATUS_TYPE, TIMESTAMP, Index, Table, column, index
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import SqlType, TableName


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
                default="date_trunc('sec'::text, (now() AT TIME ZONE 'UTC'::text))",
            ),
            column("count", "bigint", not_null=True),
            column("priority", "integer", not_null=True),
            column("status", STATUS_TYPE, not_null=True),
            column("entrypoint", "text", not_null=True),
        ),
    )


def statistics_indexes(settings: DBSettings) -> tuple[Index, ...]:
    stats = settings.statistics_table
    return (
        index(
            f"{stats}_unique_count",
            stats,
            "USING btree (priority, date_trunc('sec'::text, (created AT TIME ZONE 'UTC'::text)), status, entrypoint)",  # noqa: E501
            unique=True,
        ),
    )
