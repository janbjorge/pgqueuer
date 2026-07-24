"""The append-only schema migration registry.

Each migration is a free function that renders its SQL against a configured
:class:`~pgqueuer.adapters.persistence.qb.QueryBuilderEnvironment`, so object
names honor the install's settings (prefix and schema). ``MIGRATIONS`` is the
ordered sequence ``QueryBuilderEnvironment.build_upgrade_queries`` walks.

Migrations are append-only: a shipped migration's id, description, and rendered
SQL are frozen forever. To change the schema, append a new migration; never edit
or reorder an existing one -- an in-place edit silently no-ops on deployments
that already ran the old statement. ``test_schema_migration_registry`` pins each
shipped migration by content hash to enforce this.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .qb import QueryBuilderEnvironment

# A migration renders its SQL from the environment's public surface (settings,
# qualified names, and the widen-id helpers) and nothing else.
Render = Callable[["QueryBuilderEnvironment"], Iterable[str]]


@dataclasses.dataclass(frozen=True)
class Migration:
    """One append-only step in the schema migration sequence.

    ``render`` yields the SQL for the step against a configured
    :class:`QueryBuilderEnvironment`. Ids are the 1-based position in
    ``MIGRATIONS``; see the module docstring for the append-only contract.
    """

    id: int
    description: str
    render: Render


def updated_tracking(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"  # noqa: E501
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table}_updated_id_id1_idx ON {qn.queue_table} (updated ASC, id DESC) INCLUDE (id) WHERE status = 'picked';"  # noqa: E501


def notify_function(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"""CREATE OR REPLACE FUNCTION {qn.function}() RETURNS TRIGGER AS $$
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
                '{env.settings.channel}',
                json_build_object(
                    'channel', '{env.settings.channel}',
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
    $$ LANGUAGE plpgsql;"""


def heartbeat_tracking(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"  # noqa: E501
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table}_heartbeat_id_id1_idx ON {qn.queue_table} (heartbeat ASC, id DESC) INCLUDE (id) WHERE status = 'picked';"  # noqa: E501


def queue_manager_id(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS queue_manager_id UUID;"  # noqa: E501
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table}_queue_manager_id_idx ON {qn.queue_table} (queue_manager_id) WHERE queue_manager_id IS NOT NULL;"  # noqa: E501


def schedules_table(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"""CREATE TABLE IF NOT EXISTS {qn.schedules_table} (
        id BIGSERIAL PRIMARY KEY,
        expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
        entrypoint TEXT NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE,
        status {qn.queue_status_type} DEFAULT 'queued',
        UNIQUE (expression, entrypoint)
    );"""


def execute_after(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"""ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"""  # noqa: E501


def terminal_status_enums(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TYPE {qn.queue_status_type} ADD VALUE IF NOT EXISTS 'successful';"
    yield f"ALTER TYPE {qn.queue_status_type} ADD VALUE IF NOT EXISTS 'exception';"
    yield f"ALTER TYPE {qn.queue_status_type} ADD VALUE IF NOT EXISTS 'canceled';"
    yield f"ALTER TYPE {qn.queue_status_type} ADD VALUE IF NOT EXISTS 'deleted';"


def statistics_status_type(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TABLE {qn.statistics_table} ALTER COLUMN status TYPE {qn.queue_status_type} USING status::TEXT::{qn.queue_status_type};"  # noqa


def log_table(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"""CREATE UNLOGGED TABLE IF NOT EXISTS {qn.queue_table_log} (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        job_id BIGINT NOT NULL,
        status {qn.queue_status_type} NOT NULL,
        priority INT NOT NULL,
        entrypoint TEXT NOT NULL,
        aggregated BOOLEAN DEFAULT FALSE
    );"""
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table_log}_not_aggregated ON {qn.queue_table_log} (entrypoint, priority, status, created) WHERE not aggregated;"  # noqa
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table_log}_created ON {qn.queue_table_log} (created);"  # noqa
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table_log}_status ON {qn.queue_table_log} (status);"  # noqa


def log_traceback(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table_log} ADD COLUMN IF NOT EXISTS traceback JSONB DEFAULT NULL;"  # noqa: E501


def dedupe_key(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS dedupe_key TEXT DEFAULT NULL;"  # noqa: E501
    yield f"CREATE UNIQUE INDEX IF NOT EXISTS {s.queue_table}_unique_dedupe_key ON {qn.queue_table} (dedupe_key) WHERE ((status IN ('queued', 'picked') AND dedupe_key IS NOT NULL));"  # noqa


def log_job_id_status_index(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table_log}_job_id_status ON {qn.queue_table_log} (job_id, created DESC);"  # noqa: E501


def headers(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS headers JSONB;"


def attempts(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TABLE {qn.queue_table} ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;"  # noqa: E501


def failed_status_enum(env: QueryBuilderEnvironment) -> Iterable[str]:
    qn = env.qualified
    yield f"ALTER TYPE {qn.queue_status_type} ADD VALUE IF NOT EXISTS 'failed';"


def dequeue_indexes(env: QueryBuilderEnvironment) -> Iterable[str]:
    s = env.settings
    qn = env.qualified
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table}_ep_prio_id_idx ON {qn.queue_table} (entrypoint, priority DESC, id ASC) WHERE status = 'queued';"  # noqa
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table}_ep_ea_idx ON {qn.queue_table} (entrypoint, execute_after) WHERE status = 'queued';"  # noqa


def widen_id(env: QueryBuilderEnvironment) -> Iterable[str]:
    # Widen int4 id columns to BIGINT on pre-existing installs (issue #671).
    # int4 SERIAL caps lifetime ids at ~2.1B; once exceeded inserts fail.
    # ALTER TYPE takes an ACCESS EXCLUSIVE lock and rewrites the table, so it
    # blocks briefly. Gated by settings.widen_id so operators can skip it and
    # widen a large table out-of-band; the DO-guards keep it a no-op once id
    # is already BIGINT.
    if not env.settings.widen_id:
        return
    for table in (
        env.settings.queue_table,
        env.settings.statistics_table,
        env.settings.schedules_table,
    ):
        yield env.build_widen_id_column_query(table)
        yield env.build_widen_id_sequence_query(table)


def constant_key_aggregation_index(env: QueryBuilderEnvironment) -> Iterable[str]:
    # #668 fattened the not_aggregated partial index to a 4-column composite
    # (migration 9), but the aggregation GROUP BY is on date_trunc('sec', created),
    # an expression, so the index ordering is never used while every log insert
    # pays to maintain the wider key. Revert to the constant-key worklist index.
    # DROP first: CREATE INDEX IF NOT EXISTS cannot redefine an index that already
    # exists by name (migration 9 is a no-op on installs that already have it).
    s = env.settings
    qn = env.qualified
    yield f"DROP INDEX IF EXISTS {s.qualify(f'{s.queue_table_log}_not_aggregated')};"  # noqa
    yield f"CREATE INDEX IF NOT EXISTS {s.queue_table_log}_not_aggregated ON {qn.queue_table_log} ((1)) WHERE not aggregated;"  # noqa


# Ordered, append-only sequence. Ids are the 1-based position; append new steps
# at the end and never edit or reorder shipped ones (see module docstring).
_STEPS: tuple[tuple[str, Render], ...] = (
    ("add updated column and picked index", updated_tracking),
    ("refresh notify trigger function", notify_function),
    ("add heartbeat column and index", heartbeat_tracking),
    ("add queue_manager_id column and index", queue_manager_id),
    ("create schedules table", schedules_table),
    ("add execute_after column", execute_after),
    ("add terminal status enum values", terminal_status_enums),
    ("retype statistics status to enum", statistics_status_type),
    ("create queue log table and indexes", log_table),
    ("add log traceback column", log_traceback),
    ("add dedupe_key column and unique index", dedupe_key),
    ("add log job_id/status index", log_job_id_status_index),
    ("add headers column", headers),
    ("add attempts column", attempts),
    ("add failed status enum value", failed_status_enum),
    ("add dequeue entrypoint indexes", dequeue_indexes),
    ("widen int4 id columns to bigint", widen_id),
    ("revert aggregation index to constant key", constant_key_aggregation_index),
)

MIGRATIONS: tuple[Migration, ...] = tuple(
    Migration(i, description, render) for i, (description, render) in enumerate(_STEPS, start=1)
)
