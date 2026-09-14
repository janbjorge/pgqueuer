from __future__ import annotations

from pgqueuer.domain.schema.model import (
    STATUS_TYPE,
    TIMESTAMP,
    Table,
    UniqueConstraint,
    column,
)
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import ColumnName, SqlType, TableName


def schedules_table(settings: DBSettings) -> Table:
    return Table(
        name=TableName(settings.schedules_table),
        unlogged=settings.durability.config.schedules_table == "UNLOGGED",
        id_sequence_type=SqlType("bigint"),
        unique_constraints=(
            UniqueConstraint(columns=(ColumnName("expression"), ColumnName("entrypoint"))),
        ),
        columns=(
            column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            column("expression", "text", not_null=True),
            column("entrypoint", "text", not_null=True),
            column("heartbeat", TIMESTAMP, not_null=True, default="now()"),
            column("created", TIMESTAMP, not_null=True, default="now()"),
            column("updated", TIMESTAMP, not_null=True, default="now()"),
            column("next_run", TIMESTAMP, not_null=True, default="now()"),
            column("last_run", TIMESTAMP),
            column("status", STATUS_TYPE, default=f"'queued'::{STATUS_TYPE}"),
        ),
    )
