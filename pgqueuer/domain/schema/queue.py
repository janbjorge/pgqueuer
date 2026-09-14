from __future__ import annotations

from pgqueuer.domain.schema.model import STATUS_TYPE, TIMESTAMP, Index, Table, column, index
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import SqlType, TableName


def queue_table(settings: DBSettings) -> Table:
    return Table(
        name=TableName(settings.queue_table),
        unlogged=settings.durability.config.queue_table == "UNLOGGED",
        id_sequence_type=SqlType("bigint"),
        columns=(
            column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            column("priority", "integer", not_null=True),
            column("queue_manager_id", "uuid"),
            column("created", TIMESTAMP, not_null=True, default="now()"),
            column("updated", TIMESTAMP, not_null=True, default="now()"),
            column("heartbeat", TIMESTAMP, not_null=True, default="now()"),
            column("execute_after", TIMESTAMP, not_null=True, default="now()"),
            column("status", STATUS_TYPE, not_null=True),
            column("entrypoint", "text", not_null=True),
            column("dedupe_key", "text"),
            column("payload", "bytea"),
            column("headers", "jsonb"),
            column("attempts", "integer", not_null=True, default="0"),
            column("slot", "bigint"),
        ),
    )


def queue_indexes(settings: DBSettings) -> tuple[Index, ...]:
    queue = settings.queue_table
    return (
        index(
            f"{queue}_priority_id_id1_idx",
            queue,
            f"USING btree (priority, id DESC) INCLUDE (id) WHERE (status = 'queued'::{STATUS_TYPE})",  # noqa: E501
        ),
        index(
            f"{queue}_updated_id_id1_idx",
            queue,
            f"USING btree (updated, id DESC) INCLUDE (id) WHERE (status = 'picked'::{STATUS_TYPE})",  # noqa: E501
        ),
        index(
            f"{queue}_queue_manager_id_idx",
            queue,
            "USING btree (queue_manager_id) WHERE (queue_manager_id IS NOT NULL)",
        ),
        index(
            f"{queue}_ep_prio_id_idx",
            queue,
            f"USING btree (entrypoint, priority DESC, id) WHERE (status = 'queued'::{STATUS_TYPE})",  # noqa: E501
        ),
        index(
            f"{queue}_ep_ea_idx",
            queue,
            f"USING btree (entrypoint, execute_after) WHERE (status = 'queued'::{STATUS_TYPE})",  # noqa: E501
        ),
        index(
            f"{queue}_unique_dedupe_key",
            queue,
            f"USING btree (dedupe_key) WHERE ((status = ANY (ARRAY['queued'::{STATUS_TYPE}, 'picked'::{STATUS_TYPE}])) AND (dedupe_key IS NOT NULL))",  # noqa: E501
            unique=True,
        ),
        index(
            f"{queue}_picked_slot_idx",
            queue,
            f"USING btree (entrypoint, slot) WHERE ((status = 'picked'::{STATUS_TYPE}) AND (slot IS NOT NULL))",  # noqa: E501
            unique=True,
        ),
    )
