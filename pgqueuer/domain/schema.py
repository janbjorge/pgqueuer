from __future__ import annotations

import dataclasses
import textwrap
from typing import Literal

from pgqueuer.domain.settings import DBSettings

ColumnKind = Literal["plain", "serial", "identity"]

STATUS_TYPE = "{status_type}"


@dataclasses.dataclass(frozen=True)
class Column:
    """A table column, spelled as ``pg_catalog`` reports it.

    ``type`` uses ``format_type`` spelling (``timestamp with time zone``, not
    ``TIMESTAMPTZ``) and ``default`` uses ``pg_get_expr`` spelling (``now()``,
    ``false``). Either may contain the ``{status_type}`` placeholder.
    """

    name: str
    type: str
    not_null: bool = False
    default: str | None = None
    kind: ColumnKind = "plain"
    primary_key: bool = False


@dataclasses.dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    unlogged: bool
    unique_constraints: tuple[tuple[str, ...], ...] = ()
    id_sequence_type: str | None = None


@dataclasses.dataclass(frozen=True)
class Index:
    """A standalone index. Constraint-backed indexes belong to their table."""

    name: str
    table: str
    unique: bool
    body: str


@dataclasses.dataclass(frozen=True)
class EnumType:
    name: str
    labels: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Function:
    """A function, ``body`` holding ``pg_proc.prosrc`` rather than full DDL."""

    name: str
    body: str


@dataclasses.dataclass(frozen=True)
class Trigger:
    name: str
    table: str
    function: str
    live_definition: str | None = None


@dataclasses.dataclass(frozen=True)
class Schema:
    enums: tuple[EnumType, ...]
    tables: tuple[Table, ...]
    indexes: tuple[Index, ...]
    functions: tuple[Function, ...]
    triggers: tuple[Trigger, ...]
    namespace_exists: bool = True


@dataclasses.dataclass(frozen=True)
class Retired:
    """Objects earlier releases created that the current schema does not.

    A name only lands here when it is removed from :func:`target`; nothing is
    dropped for being merely absent.
    """

    indexes: tuple[str, ...] = ()
    types: tuple[str, ...] = ()
    columns: tuple[tuple[str, str], ...] = ()


@dataclasses.dataclass(frozen=True)
class Plan:
    """DDL to converge a database, plus what the planner declined to do."""

    statements: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.statements)


def resolve(schema: Schema, status_type: str) -> Schema:
    """Substitute the status enum placeholder throughout ``schema``.

    Comparison passes the bare enum name, DDL rendering the qualified one.
    """

    def text(value: str) -> str:
        return value.replace(STATUS_TYPE, status_type)

    return Schema(
        enums=tuple(dataclasses.replace(enum, name=text(enum.name)) for enum in schema.enums),
        tables=tuple(
            dataclasses.replace(
                table,
                columns=tuple(
                    dataclasses.replace(
                        column,
                        type=text(column.type),
                        default=None if column.default is None else text(column.default),
                    )
                    for column in table.columns
                ),
            )
            for table in schema.tables
        ),
        indexes=tuple(
            dataclasses.replace(index, body=text(index.body)) for index in schema.indexes
        ),
        functions=schema.functions,
        triggers=schema.triggers,
        namespace_exists=schema.namespace_exists,
    )


def notify_function_body(channel: str) -> str:
    """The plpgsql body of the change-notification trigger function."""
    return textwrap.dedent(
        f"""
        DECLARE
            to_emit BOOLEAN := false;  -- Flag to decide whether to emit a notification
        BEGIN
            -- Check operation type and set the emit flag accordingly
            IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
                to_emit := true;
            ELSIF TG_OP = 'DELETE' THEN
                to_emit := true;
            ELSIF TG_OP = 'INSERT' THEN
                to_emit := true;
            ELSIF TG_OP = 'TRUNCATE' THEN
                to_emit := true;
            END IF;

            -- Perform notification if the emit flag is set
            IF to_emit THEN
                PERFORM pg_notify(
                    '{channel}',
                    json_build_object(
                        'channel', '{channel}',
                        'operation', lower(TG_OP),
                        'sent_at', NOW(),
                        'table', TG_TABLE_NAME,
                        'type', 'table_changed_event'
                    )::text
                );
            END IF;

            -- Return appropriate value based on the operation
            IF TG_OP IN ('INSERT', 'UPDATE') THEN
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                RETURN OLD;
            ELSE
                RETURN NULL; -- For TRUNCATE and other non-row-specific contexts
            END IF;

        END;
        """
    )


def queue_table(settings: DBSettings) -> Table:
    timestamp = "timestamp with time zone"
    return Table(
        name=settings.queue_table,
        unlogged=settings.durability.config.queue_table == "UNLOGGED",
        id_sequence_type="bigint",
        columns=(
            Column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            Column("priority", "integer", not_null=True),
            Column("queue_manager_id", "uuid"),
            Column("created", timestamp, not_null=True, default="now()"),
            Column("updated", timestamp, not_null=True, default="now()"),
            Column("heartbeat", timestamp, not_null=True, default="now()"),
            Column("execute_after", timestamp, not_null=True, default="now()"),
            Column("status", STATUS_TYPE, not_null=True),
            Column("entrypoint", "text", not_null=True),
            Column("dedupe_key", "text"),
            Column("payload", "bytea"),
            Column("headers", "jsonb"),
            Column("attempts", "integer", not_null=True, default="0"),
            Column("slot", "bigint"),
        ),
    )


def queue_log_table(settings: DBSettings) -> Table:
    timestamp = "timestamp with time zone"
    return Table(
        name=settings.queue_table_log,
        unlogged=settings.durability.config.queue_log_table == "UNLOGGED",
        id_sequence_type="bigint",
        columns=(
            Column("id", "bigint", not_null=True, kind="identity", primary_key=True),
            Column("created", timestamp, not_null=True, default="now()"),
            Column("job_id", "bigint", not_null=True),
            Column("status", STATUS_TYPE, not_null=True),
            Column("priority", "integer", not_null=True),
            Column("entrypoint", "text", not_null=True),
            Column("traceback", "jsonb", default="NULL::jsonb"),
            Column("aggregated", "boolean", default="false"),
        ),
    )


def statistics_table(settings: DBSettings) -> Table:
    return Table(
        name=settings.statistics_table,
        unlogged=settings.durability.config.statistics_table == "UNLOGGED",
        id_sequence_type="bigint",
        columns=(
            Column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            Column(
                "created",
                "timestamp with time zone",
                not_null=True,
                default="date_trunc('sec'::text, timezone('UTC'::text, now()))",
            ),
            Column("count", "bigint", not_null=True),
            Column("priority", "integer", not_null=True),
            Column("status", STATUS_TYPE, not_null=True),
            Column("entrypoint", "text", not_null=True),
        ),
    )


def schedules_table(settings: DBSettings) -> Table:
    timestamp = "timestamp with time zone"
    return Table(
        name=settings.schedules_table,
        unlogged=settings.durability.config.schedules_table == "UNLOGGED",
        id_sequence_type="bigint",
        unique_constraints=(("expression", "entrypoint"),),
        columns=(
            Column("id", "bigint", not_null=True, kind="serial", primary_key=True),
            Column("expression", "text", not_null=True),
            Column("entrypoint", "text", not_null=True),
            Column("heartbeat", timestamp, not_null=True, default="now()"),
            Column("created", timestamp, not_null=True, default="now()"),
            Column("updated", timestamp, not_null=True, default="now()"),
            Column("next_run", timestamp, not_null=True, default="now()"),
            Column("last_run", timestamp),
            Column("status", STATUS_TYPE, default=f"'queued'::{STATUS_TYPE}"),
        ),
    )


def indexes(settings: DBSettings) -> tuple[Index, ...]:
    queue = settings.queue_table
    log = settings.queue_table_log
    unindexed = (
        Index(
            name=f"{queue}_priority_id_id1_idx",
            table=queue,
            unique=False,
            body=(
                "USING btree (priority, id DESC) INCLUDE (id) "
                f"WHERE (status = 'queued'::{STATUS_TYPE})"
            ),
        ),
        Index(
            name=f"{queue}_updated_id_id1_idx",
            table=queue,
            unique=False,
            body=(
                "USING btree (updated, id DESC) INCLUDE (id) "
                f"WHERE (status = 'picked'::{STATUS_TYPE})"
            ),
        ),
        Index(
            name=f"{queue}_queue_manager_id_idx",
            table=queue,
            unique=False,
            body="USING btree (queue_manager_id) WHERE (queue_manager_id IS NOT NULL)",
        ),
        Index(
            name=f"{queue}_ep_prio_id_idx",
            table=queue,
            unique=False,
            body=(
                "USING btree (entrypoint, priority DESC, id) "
                f"WHERE (status = 'queued'::{STATUS_TYPE})"
            ),
        ),
        Index(
            name=f"{queue}_ep_ea_idx",
            table=queue,
            unique=False,
            body=(
                f"USING btree (entrypoint, execute_after) WHERE (status = 'queued'::{STATUS_TYPE})"
            ),
        ),
        Index(
            name=f"{queue}_unique_dedupe_key",
            table=queue,
            unique=True,
            body=(
                "USING btree (dedupe_key) WHERE ((status = ANY "
                f"(ARRAY['queued'::{STATUS_TYPE}, 'picked'::{STATUS_TYPE}])) "
                "AND (dedupe_key IS NOT NULL))"
            ),
        ),
        Index(
            name=f"{queue}_picked_slot_idx",
            table=queue,
            unique=True,
            body=(
                "USING btree (entrypoint, slot) "
                f"WHERE ((status = 'picked'::{STATUS_TYPE}) AND (slot IS NOT NULL))"
            ),
        ),
        Index(
            name=f"{log}_not_aggregated",
            table=log,
            unique=False,
            body="USING btree ((1)) WHERE (NOT aggregated)",
        ),
        Index(name=f"{log}_created", table=log, unique=False, body="USING btree (created)"),
        Index(name=f"{log}_status", table=log, unique=False, body="USING btree (status)"),
        Index(
            name=f"{log}_job_id_status",
            table=log,
            unique=False,
            body="USING btree (job_id, created DESC)",
        ),
        Index(
            name=f"{settings.statistics_table}_unique_count",
            table=settings.statistics_table,
            unique=True,
            body=(
                "USING btree (priority, date_trunc('sec'::text, "
                "timezone('UTC'::text, created)), status, entrypoint)"
            ),
        ),
    )
    return tuple(sorted(unindexed, key=lambda index: index.name))


def target(settings: DBSettings) -> Schema:
    """The schema the current release installs.

    The single place a maintainer edits when the schema changes. Definitions
    are spelled as ``pg_catalog`` reports them, so a declared object compares
    to an installed one by string equality.
    """
    tables = (
        queue_table(settings),
        queue_log_table(settings),
        statistics_table(settings),
        schedules_table(settings),
    )
    return Schema(
        enums=(
            EnumType(
                name=settings.queue_status_type,
                labels=(
                    "queued",
                    "picked",
                    "successful",
                    "exception",
                    "canceled",
                    "deleted",
                    "failed",
                ),
            ),
        ),
        tables=tuple(sorted(tables, key=lambda table: table.name)),
        indexes=indexes(settings),
        functions=(
            Function(
                name=settings.function,
                body=notify_function_body(settings.channel),
            ),
        ),
        triggers=(
            Trigger(
                name=settings.trigger,
                table=settings.queue_table,
                function=settings.function,
            ),
        ),
    )


def retired(settings: DBSettings) -> Retired:
    """Objects earlier releases left behind, named so upgrade can clean up.

    ``time_in_queue`` is reported rather than dropped, since a ``DROP COLUMN``
    would destroy data an operator may still read.
    """
    return Retired(
        indexes=(f"{settings.queue_table}_heartbeat_id_id1_idx",),
        types=(settings.legacy_statistics_status_type,),
        columns=((settings.statistics_table, "time_in_queue"),),
    )
