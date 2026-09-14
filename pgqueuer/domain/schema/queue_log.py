from __future__ import annotations

from pgqueuer.domain.schema.model import STATUS_TYPE, TIMESTAMP, Index, Table, column, index
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import SqlType, TableName


def queue_log_table(settings: DBSettings) -> Table:
    return Table(
        name=TableName(settings.queue_table_log),
        unlogged=settings.durability.config.queue_log_table == "UNLOGGED",
        id_sequence_type=SqlType("bigint"),
        columns=(
            column("id", "bigint", not_null=True, kind="identity", primary_key=True),
            column("created", TIMESTAMP, not_null=True, default="now()"),
            column("job_id", "bigint", not_null=True),
            column("status", STATUS_TYPE, not_null=True),
            column("priority", "integer", not_null=True),
            column("entrypoint", "text", not_null=True),
            column("traceback", "jsonb", default="NULL::jsonb"),
            column("aggregated", "boolean", default="false"),
        ),
    )


def queue_log_indexes(settings: DBSettings) -> tuple[Index, ...]:
    log = settings.queue_table_log
    return (
        index(f"{log}_not_aggregated", log, "USING btree ((1)) WHERE (NOT aggregated)"),
        index(f"{log}_created", log, "USING btree (created)"),
        index(f"{log}_status", log, "USING btree (status)"),
        index(f"{log}_job_id_status", log, "USING btree (job_id, created DESC)"),
    )
