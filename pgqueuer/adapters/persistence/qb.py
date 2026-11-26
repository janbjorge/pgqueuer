from __future__ import annotations

import dataclasses
from collections.abc import Generator, Mapping
from datetime import timedelta
from typing import ClassVar, assert_never

from pgqueuer.adapters.persistence import schema_ddl
from pgqueuer.adapters.persistence.composer import ComposedQuery, QueryTemplate, SqlComposer
from pgqueuer.adapters.persistence.dequeue import (
    DequeueArgs,
    DequeueCacheKey,
    dequeue_shape,
    due,
    render_dequeue,
)
from pgqueuer.domain.schema.queue import dedupe_predicate
from pgqueuer.domain.schema.statistics import UNIQUE_COUNT_KEY
from pgqueuer.domain.settings import (
    DBSettings,
    Durability,
    DurabilityPolicy,
    QualifiedNames,
)
from pgqueuer.domain.types import (
    OnConflict,
    QueueEntrypoint,
    QueueManagerId,
    SortOrder,
)

# Re-export for backward compatibility within the adapter layer
__all__ = [
    "DBSettings",
    "Durability",
    "DurabilityPolicy",
    "QualifiedNames",
    "QueryBuilderEnvironment",
    "QueryQueueBuilder",
    "QuerySchedulerBuilder",
]


@dataclasses.dataclass
class QueryBuilderEnvironment:
    """DDL/utility query builder bound to a :class:`DBSettings`."""

    settings: DBSettings

    QUEUE_AUTOVACUUM: ClassVar[Mapping[str, str]] = {
        "autovacuum_vacuum_scale_factor": "0.01",
        "autovacuum_vacuum_threshold": "1000",
        "autovacuum_analyze_scale_factor": "0.02",
        "autovacuum_analyze_threshold": "500",
        "autovacuum_vacuum_cost_limit": "10000",
        "autovacuum_vacuum_cost_delay": "0",
        "fillfactor": "70",
    }
    LOG_AUTOVACUUM: ClassVar[Mapping[str, str]] = {
        "autovacuum_vacuum_scale_factor": "0.95",
        "autovacuum_vacuum_threshold": "1000000",
        "autovacuum_analyze_scale_factor": "0.05",
        "autovacuum_analyze_threshold": "10000",
    }

    @property
    def qualified(self) -> QualifiedNames:
        return self.settings.qualified

    def build_install_query(
        self,
        create_schema: bool = True,
        durability: Durability | None = None,
    ) -> str:
        """DDL: schema (optional), types, tables, indexes, trigger, and notify function.

        *durability* overrides the configured level for this install only.
        """
        settings = (
            self.settings
            if durability is None
            else self.settings.model_copy(update={"durability": durability})
        )
        return schema_ddl.render_install(settings, create_schema=create_schema)

    def build_uninstall_query(self) -> str:
        return schema_ddl.render_uninstall(self.settings)

    def build_table_has_column_query(self) -> str:
        return f"""SELECT EXISTS (
        SELECT FROM information_schema.columns
        WHERE
                table_schema = {self.settings.schema_expr}
            AND table_name = $1
            AND column_name = $2
        );"""

    def build_table_has_index_query(self) -> str:
        return f"""SELECT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE
                    tablename  = $1
                AND indexname  = $2
                AND schemaname = {self.settings.schema_expr}
        );
        """

    def build_has_table_query(self) -> str:
        return f"""SELECT EXISTS (
        SELECT FROM information_schema.columns
        WHERE
                table_schema = {self.settings.schema_expr}
            AND table_name = $1
        );"""

    def build_has_function_query(self) -> str:
        return f"""SELECT EXISTS (
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = {self.settings.schema_expr}
              AND p.proname = $1
        );"""

    def build_has_trigger_query(self) -> str:
        return f"""SELECT EXISTS (
            SELECT 1
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = {self.settings.schema_expr}
              AND t.tgname = $1
        );"""

    def build_user_types_query(self) -> str:
        # Unfiltered when db_schema is unset: the pre-schema behavior scanned
        # every schema, and narrowing it here would change what upgrade sees
        # on search_path-based installs.
        if self.settings.db_schema:
            return f"""SELECT enumlabel, typname
    FROM pg_enum
    JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
    JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
    WHERE pg_namespace.nspname = {self.settings.schema_expr}"""
        return """SELECT enumlabel, typname
    FROM pg_enum
    JOIN pg_type ON pg_enum.enumtypid = pg_type.oid"""

    def build_alter_durability_query(
        self,
        durability: Durability | None = None,
    ) -> Generator[str, None, None]:
        """ALTER statements for *durability*, or the configured level when None."""
        config = (self.settings.durability if durability is None else durability).config
        policies = {
            self.qualified.queue_table: config.queue_table,
            self.qualified.queue_table_log: config.queue_log_table,
            self.qualified.statistics_table: config.statistics_table,
            self.qualified.schedules_table: config.schedules_table,
        }
        for table, policy in policies.items():
            yield f"ALTER TABLE {table} SET {'LOGGED' if policy == '' else 'UNLOGGED'};"

    def autovacuum_settings(self) -> dict[str, Mapping[str, str]]:
        return {
            self.qualified.queue_table: self.QUEUE_AUTOVACUUM,
            self.qualified.schedules_table: self.QUEUE_AUTOVACUUM,
            self.qualified.queue_table_log: self.LOG_AUTOVACUUM,
            self.qualified.statistics_table: self.LOG_AUTOVACUUM,
        }

    def build_optimize_autovacuum_query(self) -> str:
        statements = []
        for table, block in self.autovacuum_settings().items():
            params = ",\n".join(f"    {param:<32} = {value}" for param, value in block.items())
            statements.append(f"ALTER TABLE {table} SET (\n{params}\n);")
        return "\n\n".join(statements)

    def build_optimize_autovacuum_rollback_query(self) -> str:
        statements = []
        for table, block in self.autovacuum_settings().items():
            params = ",\n".join(f"    {param}" for param in block)
            statements.append(f"ALTER TABLE {table} RESET (\n{params}\n);")
        return "\n\n".join(statements)


@dataclasses.dataclass
class QueryQueueBuilder:
    settings: DBSettings
    dequeue_templates: dict[DequeueCacheKey, QueryTemplate[DequeueArgs]] = dataclasses.field(
        init=False,
        repr=False,
        compare=False,
        default_factory=dict,
    )

    @property
    def qualified(self) -> QualifiedNames:
        return self.settings.qualified

    def build_dequeue_query(
        self,
        *,
        batch_size: int,
        entrypoints: list[QueueEntrypoint],
        concurrency_limits: list[int],
        queue_manager_id: QueueManagerId,
        global_concurrency_limit: int | None,
        heartbeat_timeout: timedelta,
    ) -> ComposedQuery:
        """Bind the call to the dequeue statement of the gates it needs.

        The statement is rendered once per shape and batch size, because dequeue
        runs every busy-loop iteration; the arguments are read fresh on every call.
        """
        args = DequeueArgs(
            batch_size=batch_size,
            entrypoints=entrypoints,
            concurrency_limits=concurrency_limits,
            queue_manager_id=queue_manager_id,
            global_concurrency_limit=global_concurrency_limit,
            heartbeat_timeout=heartbeat_timeout,
        )
        key = DequeueCacheKey(dequeue_shape(args), batch_size)
        if key not in self.dequeue_templates:
            self.dequeue_templates[key] = render_dequeue(self.qualified, key)
        return self.dequeue_templates[key].bind(args)

    def build_has_queued_work(self) -> str:
        return f"""
        SELECT COUNT(*) AS queued_work
        FROM (
            SELECT 1
            FROM {self.qualified.queue_table}
            WHERE entrypoint = ANY($1)
              AND status = 'queued'
            LIMIT 1
        ) sub
        """

    def build_has_eligible_queued_work(self) -> str:
        """$2 is parallel to $1; an entrypoint with a positive limit counts only below it."""
        queue_table = self.qualified.queue_table
        return f"""
        SELECT COUNT(*) AS queued_work
        FROM (
            SELECT 1
            FROM UNNEST($1::text[], $2::bigint[]) AS params(entrypoint, concurrency_limit)
            WHERE (
                params.concurrency_limit <= 0
                OR (
                    SELECT COUNT(*)
                    FROM {queue_table} p
                    WHERE p.entrypoint = params.entrypoint
                      AND p.queue_manager_id IS NOT NULL
                ) < params.concurrency_limit
            )
              AND EXISTS (
                SELECT 1
                FROM {queue_table} q
                WHERE q.entrypoint = params.entrypoint
                  AND q.status = 'queued'
                  AND {due("q")}
              )
            LIMIT 1
        ) sub
        """

    def build_enqueue_query(self, on_conflict: OnConflict = "raise") -> str:
        # Restating the index predicate is what lets PostgreSQL infer the
        # arbiter; spelled() qualifies the enum for a schema'd install.
        predicate = schema_ddl.spelled(dedupe_predicate(self.settings), self.settings)
        same_key = ""
        if on_conflict == "skip":
            on_conflict_clause = f"""ON CONFLICT (dedupe_key)
                WHERE {predicate}
                DO NOTHING"""
        elif on_conflict == "update_priority":
            on_conflict_clause = f"""ON CONFLICT (dedupe_key)
                WHERE {predicate}
                DO UPDATE SET priority = GREATEST(job.priority, EXCLUDED.priority)"""
            # An updated job keeps its own id, so it maps back to its input by key.
            same_key = "OR i.dedupe_key = ins.dedupe_key"
        elif on_conflict == "raise":
            on_conflict_clause = ""
        else:
            assert_never(on_conflict)
        return f"""
        WITH input AS MATERIALIZED (
            -- MATERIALIZED: nextval is volatile and `input` is referenced twice,
            -- so pin single evaluation -- each input row gets exactly one id.
            -- The scalar sub-select resolves the sequence name once (InitPlan)
            -- rather than a catalog lookup per row.
            -- Skipped ON CONFLICT rows leave harmless id gaps; a pre-count CTE
            -- could avoid them but isn't worth the extra pass.
            SELECT
                nextval(
                    (SELECT pg_get_serial_sequence('{self.qualified.queue_table}', 'id'))
                ) AS id,
                priority, entrypoint, payload, execute_after, dedupe_key, headers, ord
            FROM UNNEST(
                $1::int[], $2::text[], $3::bytea[],
                $4::interval[], $5::text[], $6::jsonb[]
            ) WITH ORDINALITY AS
                t(priority, entrypoint, payload, execute_after, dedupe_key, headers, ord)
        ),
        inserted AS (
            INSERT INTO {self.qualified.queue_table} AS job
            (id, priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
            SELECT id, priority, entrypoint, payload, execute_after + NOW(),
                dedupe_key, headers, 'queued'
            FROM input
            {on_conflict_clause}
            RETURNING id, entrypoint, priority, dedupe_key
        ),
        logged AS (
            INSERT INTO {self.qualified.queue_table_log}
            (job_id, status, entrypoint, priority)
            SELECT id, 'queued', entrypoint, priority
            FROM inserted
            -- A job whose priority was updated was queued before; log new jobs only.
            WHERE id IN (SELECT id FROM input)
        )
        -- Join maps each surviving id back to its input ordinal. Rows skipped by
        -- ON CONFLICT are absent from `inserted`, so their positions never appear.
        SELECT i.ord AS ord, ins.id AS id
        FROM inserted ins
        JOIN input i ON i.id = ins.id {same_key}
        ORDER BY i.ord
        """

    def build_delete_from_queue_query(self) -> str:
        return f"""WITH deleted AS (
            DELETE FROM {self.qualified.queue_table}
            WHERE entrypoint = ANY($1)
            RETURNING id, entrypoint, priority
        )
        INSERT INTO {self.qualified.queue_table_log} (job_id, status, entrypoint, priority)
        SELECT id, 'deleted'::{self.qualified.queue_status_type} AS status, entrypoint, priority
        FROM deleted
        """

    def build_truncate_queue_query(self) -> str:
        return f"TRUNCATE {self.qualified.queue_table}"

    def build_queue_size_query(self) -> str:
        return f"""SELECT
        count(*) AS count,
        priority,
        entrypoint,
        status
    FROM
        {self.qualified.queue_table}
    GROUP BY entrypoint, priority, status
    ORDER BY count, entrypoint, priority, status
    """

    def build_log_job_query(self) -> str:
        # Owner check is re-read under READ COMMITTED. A NULL owner (admin cancel)
        # removes queued and failed rows only; a picked row stays for its worker.
        return f"""WITH job_status AS (
            SELECT
                UNNEST($1::bigint[])                            AS id,
                UNNEST($2::{self.qualified.queue_status_type}[]) AS status,
                UNNEST($3::JSONB[])                             AS traceback,
                UNNEST($4::uuid[])                              AS queue_manager_id
        ), deleted AS (
            DELETE FROM {self.qualified.queue_table} q
            USING job_status js
            WHERE q.id = js.id
                AND js.status != 'failed'
                AND (
                    q.queue_manager_id = js.queue_manager_id
                    OR (js.queue_manager_id IS NULL AND q.status <> 'picked')
                )
            RETURNING q.id, q.entrypoint, q.priority
        ), held AS (
            UPDATE {self.qualified.queue_table} q
            SET status = 'failed', updated = NOW(), queue_manager_id = NULL
            FROM job_status js
            WHERE q.id = js.id
                AND js.status = 'failed'
                AND (js.queue_manager_id IS NULL OR q.queue_manager_id = js.queue_manager_id)
            RETURNING q.id, q.entrypoint, q.priority
        ), all_resolved AS (
            SELECT id, entrypoint, priority FROM deleted
            UNION ALL
            SELECT id, entrypoint, priority FROM held
        ), merged AS (
            SELECT
                job_status.id           AS id,
                job_status.status       AS status,
                job_status.traceback    AS traceback,
                all_resolved.entrypoint AS entrypoint,
                all_resolved.priority   AS priority
            FROM job_status
            INNER JOIN all_resolved
                ON all_resolved.id = job_status.id
        )
        INSERT INTO {self.qualified.queue_table_log} (
            job_id,
            status,
            entrypoint,
            priority,
            traceback
        )
        SELECT id, status, entrypoint, priority, traceback FROM merged
        """

    def build_truncate_log_statistics_query(self) -> str:
        return f"""TRUNCATE {self.qualified.statistics_table}"""

    def build_delete_from_log_statistics_query(self) -> str:
        return f"""DELETE FROM {self.qualified.statistics_table} WHERE entrypoint = ANY($1)"""

    def build_log_statistics_query(
        self,
        *,
        limit: int | None,
        last: timedelta | None,
    ) -> ComposedQuery:
        """Read recent statistics rows; an unset limit/last elides its clause."""
        composer = SqlComposer()

        window = ""
        if last is not None:
            bound_last = composer.bind(last)
            window = f"WHERE created > NOW() - {bound_last}::interval"

        cap = ""
        if limit is not None:
            bound_limit = composer.bind(limit)
            cap = f"LIMIT {bound_limit}"

        return composer.render(
            [],
            f"""
                SELECT count, created, entrypoint, priority, status
                FROM {self.qualified.statistics_table}
                {window}
                ORDER BY id DESC
                {cap}
            """,
        )

    def build_update_heartbeat_query(self) -> str:
        return f"""UPDATE {self.qualified.queue_table} SET heartbeat = NOW() WHERE id = ANY($1::bigint[])"""  # noqa: E501

    def build_truncate_log_query(self) -> str:
        return f"TRUNCATE {self.qualified.queue_table_log}"

    def build_delete_log_query(self) -> str:
        return f"DELETE FROM {self.qualified.queue_table_log} WHERE entrypoint = ANY($1)"

    def build_fetch_log_query(self) -> str:
        return f"SELECT * FROM {self.qualified.queue_table_log}"

    def build_aggregate_log_data_to_statistics_query(self, advisory_lock: bool = False) -> str:
        """Fold unaggregated log rows into per-second statistics buckets.

        A single-row ``guard`` CTE gates the whole statement. With
        ``advisory_lock=True`` its ``proceed`` column is a
        ``pg_try_advisory_xact_lock``, so concurrent workers serialize on
        aggregation (losers see ``proceed = FALSE`` and no-op) -- used by the
        periodic background task. Without it ``proceed`` is an unconditional
        ``TRUE`` (the on-demand ``log_statistics`` read path).

        Counts come from the rows this statement's UPDATE flagged, so a row
        another aggregation flagged first is not counted twice.
        """
        log = self.qualified.queue_table_log
        stats = self.qualified.statistics_table
        proceed = f"pg_try_advisory_xact_lock(hashtext('{stats}'))" if advisory_lock else "TRUE"
        return f"""WITH guard AS (
            SELECT {proceed} AS proceed
        ),
        updated_logs AS (
            UPDATE {log}
            SET aggregated = TRUE
            FROM guard
            WHERE NOT aggregated AND guard.proceed
            RETURNING entrypoint, priority, status, created
        ),
        log_aggregation AS (
            SELECT
                entrypoint,
                priority,
                status,
                COUNT(*) AS count,
                date_trunc('sec', created) AS created
            FROM updated_logs
            GROUP BY entrypoint, priority, status, date_trunc('sec', created)
        )
        INSERT INTO {stats} (count, created, entrypoint, priority, status)
        SELECT count, created, entrypoint, priority, status
        FROM log_aggregation
        ON CONFLICT ({UNIQUE_COUNT_KEY}) DO UPDATE SET
            count = {stats}.count + EXCLUDED.count
        """

    def build_retry_job_query(self) -> str:
        return f"""WITH retry AS (
            UPDATE {self.qualified.queue_table}
            SET
                status = 'queued',
                execute_after = NOW() + $2::interval,
                attempts = attempts + 1,
                updated = NOW(),
                queue_manager_id = NULL
            WHERE id = $1::bigint
                AND ($4::uuid IS NULL OR queue_manager_id = $4::uuid)
            RETURNING id, entrypoint, priority
        )
        INSERT INTO {self.qualified.queue_table_log} (job_id, status, entrypoint, priority, traceback)
        SELECT id, 'queued', entrypoint, priority, $3::JSONB FROM retry
        RETURNING job_id
        """  # noqa: E501

    def build_requeue_jobs_query(self) -> str:
        return f"""WITH requeued AS (
            UPDATE {self.qualified.queue_table}
            SET status = 'queued', execute_after = NOW(), updated = NOW(),
                attempts = 0, queue_manager_id = NULL
            WHERE id = ANY($1::bigint[]) AND status = 'failed'
            RETURNING id, entrypoint, priority
        )
        INSERT INTO {self.qualified.queue_table_log} (job_id, status, entrypoint, priority)
        SELECT id, 'queued', entrypoint, priority FROM requeued
        RETURNING job_id
        """

    def build_list_failed_jobs_query(self, order: SortOrder = "DESC") -> str:
        return f"""SELECT * FROM {self.qualified.queue_table}
        WHERE status = 'failed'
        ORDER BY created {order}
        LIMIT $1
        """

    def build_failed_jobs_query(self) -> str:
        return f"""SELECT * FROM {self.qualified.queue_table_log}
        WHERE status = 'exception'::{self.qualified.queue_status_type}
        ORDER BY created DESC
        LIMIT $1
        """

    def build_queue_log_query(self) -> str:
        return f"""SELECT * FROM {self.qualified.queue_table_log}
        ORDER BY created DESC
        LIMIT $1
        """

    def build_stale_jobs_query(self) -> str:
        return f"""SELECT id, priority, queue_manager_id, created, updated, heartbeat,
            execute_after, status, entrypoint,
            EXTRACT(EPOCH FROM NOW() - heartbeat) AS seconds_since_heartbeat
        FROM {self.qualified.queue_table}
        WHERE status = 'picked'::{self.qualified.queue_status_type}
          AND heartbeat < NOW() - $1::interval
        ORDER BY heartbeat ASC
        LIMIT $2
        """

    def build_queue_age_query(self) -> str:
        return f"""SELECT
            entrypoint,
            COUNT(*) AS queued_count,
            MIN(created) AS oldest_created,
            EXTRACT(EPOCH FROM NOW() - MIN(created)) AS oldest_age_seconds,
            AVG(EXTRACT(EPOCH FROM NOW() - created)) AS avg_age_seconds
        FROM {self.qualified.queue_table}
        WHERE status = 'queued'::{self.qualified.queue_status_type}
        GROUP BY entrypoint
        ORDER BY entrypoint
        """

    def build_throughput_summary_query(self) -> str:
        return f"""SELECT
            entrypoint,
            status,
            SUM(count) AS total_count
        FROM {self.qualified.statistics_table}
        WHERE ($1::interval IS NULL OR created > NOW() - $1)
        GROUP BY entrypoint, status
        ORDER BY entrypoint, status
        """

    def build_active_workers_query(self) -> str:
        return f"""SELECT
            queue_manager_id,
            COUNT(*) AS active_jobs,
            MIN(heartbeat) AS oldest_heartbeat,
            MAX(heartbeat) AS newest_heartbeat,
            array_agg(DISTINCT entrypoint) AS entrypoints
        FROM {self.qualified.queue_table}
        WHERE status = 'picked'::{self.qualified.queue_status_type}
          AND queue_manager_id IS NOT NULL
        GROUP BY queue_manager_id
        ORDER BY queue_manager_id
        """

    def build_schema_info_query(self) -> str:
        s = self.settings
        return f"""SELECT
            c.relname AS table_name,
            CASE WHEN c.relpersistence = 'u' THEN 'UNLOGGED'
                 WHEN c.relpersistence = 'p' THEN 'LOGGED'
                 ELSE c.relpersistence::text
            END AS persistence,
            c.reltuples::bigint AS estimated_rows,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = {s.schema_expr}
          AND c.relname IN (
            '{s.queue_table}',
            '{s.queue_table_log}',
            '{s.statistics_table}',
            '{s.schedules_table}'
          )
          AND c.relkind = 'r'
        ORDER BY c.relname
        """

    def build_job_status_query(self) -> str:
        return f"""SELECT DISTINCT ON (job_id) job_id, status
        FROM   {self.qualified.queue_table_log}
        WHERE  job_id = ANY($1)
        ORDER  BY job_id, created DESC, id DESC;
        """

    def build_next_deferred_eta_query(self) -> str:
        return f"""SELECT execute_after - NOW() AS eta
        FROM {self.qualified.queue_table}
        WHERE status = 'queued'
          AND NOT ({due()})
          AND entrypoint = ANY($1)
        ORDER BY execute_after
        LIMIT 1
        """

    def build_next_stale_eta_query(self) -> str:
        return f"""SELECT GREATEST(heartbeat + $3::interval - NOW(), INTERVAL '0') AS eta
        FROM {self.qualified.queue_table}
        WHERE status = 'picked'
          AND entrypoint = ANY($1)
          AND queue_manager_id IS DISTINCT FROM $2
        ORDER BY heartbeat
        LIMIT 1
        """

    def build_job_duration_percentiles_query(self) -> str:
        """Execution-duration percentiles per entrypoint, derived from log transitions.

        Pairs each terminal log row with the preceding 'picked' row of the same
        job. Jobs picked before the window start lose their pair and are
        undercounted rather than miscounted.
        """
        return f"""WITH transitions AS (
            SELECT
                entrypoint,
                status,
                created,
                LAG(status) OVER w AS prev_status,
                LAG(created) OVER w AS prev_created
            FROM {self.qualified.queue_table_log}
            WHERE created > NOW() - $1::interval
            WINDOW w AS (PARTITION BY job_id ORDER BY created, id)
        ), durations AS (
            SELECT
                entrypoint,
                EXTRACT(EPOCH FROM created - prev_created) AS duration_seconds
            FROM transitions
            WHERE prev_status = 'picked'
              AND status IN ('successful', 'exception', 'canceled', 'failed')
        )
        SELECT
            entrypoint,
            COUNT(*) AS completed,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS p50_seconds,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_seconds,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_seconds) AS p99_seconds,
            MAX(duration_seconds) AS max_seconds
        FROM durations
        GROUP BY entrypoint
        ORDER BY p95_seconds DESC
        """

    def build_throughput_timeseries_query(self) -> str:
        # Buckets start at a fixed UTC origin, so a bucket of a minute matches the minute.
        return f"""SELECT
            date_bin($2::interval, created, TIMESTAMPTZ '2001-01-01 00:00:00+00') AS bucket,
            entrypoint,
            status,
            SUM(count) AS count
        FROM {self.qualified.statistics_table}
        WHERE created > NOW() - $1::interval
        GROUP BY bucket, entrypoint, status
        ORDER BY bucket
        """

    def build_queue_job_by_id_query(self) -> str:
        return f"SELECT * FROM {self.qualified.queue_table} WHERE id = $1::bigint"

    def build_job_log_history_query(self) -> str:
        return f"""SELECT * FROM {self.qualified.queue_table_log}
        WHERE job_id = $1::bigint
        ORDER BY created ASC, id ASC
        LIMIT $2
        """

    def build_unaggregated_log_count_query(self) -> str:
        return f"""SELECT COUNT(*) AS unaggregated
        FROM {self.qualified.queue_table_log}
        WHERE NOT aggregated
        """

    def build_browse_queue_query(self) -> str:
        return f"""SELECT * FROM {self.qualified.queue_table}
        WHERE ($3::text[] IS NULL OR status::text = ANY($3::text[]))
          AND ($4::text[] IS NULL OR entrypoint = ANY($4::text[]))
        ORDER BY priority DESC, id ASC
        LIMIT $1 OFFSET $2
        """


@dataclasses.dataclass
class QuerySchedulerBuilder:
    settings: DBSettings

    @property
    def qualified(self) -> QualifiedNames:
        return self.settings.qualified

    def build_insert_schedule_query(self) -> str:
        return f"""WITH params AS (
        SELECT UNNEST($1::text[])       AS expression,
               UNNEST($2::text[])       AS entrypoint,
               UNNEST($3::interval[])   AS delay
        )
        INSERT INTO {self.qualified.schedules_table} (expression, next_run, entrypoint)
        SELECT expression, date_trunc('seconds', NOW() + delay), entrypoint FROM params
        ON CONFLICT (entrypoint, expression) DO NOTHING"""

    def build_fetch_schedule_query(self) -> str:
        return f"""WITH params AS (
        SELECT UNNEST($1::text[])       AS expression,
               UNNEST($2::text[])       AS entrypoint,
               UNNEST($3::interval[])   AS delay
    ), picked_jobs AS (
        SELECT id, expression, entrypoint
        FROM {self.qualified.schedules_table}
        WHERE
                ((entrypoint, expression) IN (SELECT entrypoint, expression FROM params)
            AND next_run <= NOW()
            AND status = 'queued')
            OR
                ((entrypoint, expression) IN (SELECT entrypoint, expression FROM params)
            AND NOW() - heartbeat > interval '30 seconds'
            AND status = 'picked')
        FOR UPDATE SKIP LOCKED
    )
    UPDATE {self.qualified.schedules_table}
    SET
        status = 'picked',
        updated = NOW(),
        heartbeat = NOW(),
        next_run = date_trunc('seconds', NOW() + (
                SELECT delay FROM params
                WHERE
                    params.entrypoint = {self.qualified.schedules_table}.entrypoint
                AND params.expression = {self.qualified.schedules_table}.expression
            ))
    WHERE (entrypoint, expression) IN (SELECT entrypoint, expression FROM picked_jobs)
    RETURNING *;"""

    def build_set_schedule_queued_query(self) -> str:
        return f"""UPDATE {self.qualified.schedules_table} SET status = 'queued', last_run = NOW(), updated = NOW() WHERE id = ANY($1);"""  # noqa: E501

    def build_update_schedule_heartbeat(self) -> str:
        return f"""UPDATE {self.qualified.schedules_table} SET heartbeat = NOW(), updated = NOW() WHERE id = ANY($1);"""  # noqa: E501

    def build_peek_schedule_query(self) -> str:
        return f"""SELECT * FROM {self.qualified.schedules_table} ORDER BY last_run ASC"""

    def build_delete_schedule_query(self) -> str:
        # An empty keep deletes every row of the entrypoint. A listed pair is spared.
        return f"""WITH keep AS (
            SELECT UNNEST($3::text[]) AS entrypoint,
                   UNNEST($4::text[]) AS expression
        )
        DELETE FROM {self.qualified.schedules_table}
        WHERE id = ANY($1)
            OR (
                entrypoint = ANY($2)
                AND (entrypoint, expression) NOT IN (
                    SELECT entrypoint, expression FROM keep
                )
            )
        """

    def build_truncate_schedule_query(self) -> str:
        return f"""TRUNCATE {self.qualified.schedules_table}"""
