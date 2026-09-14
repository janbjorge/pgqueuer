from __future__ import annotations

from pgqueuer.domain.schema.model import ColumnRef, EnumType, Retired, Schema
from pgqueuer.domain.schema.notify import notify_function, notify_trigger
from pgqueuer.domain.schema.queue import queue_indexes, queue_table
from pgqueuer.domain.schema.queue_log import queue_log_indexes, queue_log_table
from pgqueuer.domain.schema.schedules import schedules_table
from pgqueuer.domain.schema.statistics import statistics_indexes, statistics_table
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import ColumnName, IndexName, TableName, TypeName


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
    indexes = (
        *queue_indexes(settings),
        *queue_log_indexes(settings),
        *statistics_indexes(settings),
    )
    return Schema(
        enums=(
            EnumType(
                name=TypeName(settings.queue_status_type),
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
        indexes=tuple(sorted(indexes, key=lambda entry: entry.name)),
        functions=(notify_function(settings),),
        triggers=(notify_trigger(settings),),
    )


def retired(settings: DBSettings) -> Retired:
    """Objects earlier releases left behind, named so upgrade can clean up.

    ``time_in_queue`` is reported rather than dropped, since a ``DROP COLUMN``
    would destroy data an operator may still read.
    """
    return Retired(
        indexes=(IndexName(f"{settings.queue_table}_heartbeat_id_id1_idx"),),
        types=(TypeName(settings.legacy_statistics_status_type),),
        columns=(
            ColumnRef(
                table=TableName(settings.statistics_table),
                column=ColumnName("time_in_queue"),
            ),
        ),
    )
