from __future__ import annotations

import dataclasses
import os
from datetime import timedelta
from typing import Final, Generator

from . import db, models


def add_prefix(string: str) -> str:
    """
    Appends a predefined prefix from environment variables to
    the given string, typically used for database object names.
    """
    return f"{os.environ.get('PGQUEUER_PREFIX', '')}{string}"


@dataclasses.dataclass
class DBSettings:
    """
    Stores database settings such as table names and SQL
    function names, dynamically appending prefixes from
    environment variables.
    """

    # Channel name for PostgreSQL LISTEN/NOTIFY used to
    # receive notifications about changes in the queue.
    channel: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("ch_pgqueuer"),
        kw_only=True,
    )

    # Name of the database function triggered by changes to the queue
    # table, used to notify subscribers.
    function: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("fn_pgqueuer_changed"),
        kw_only=True,
    )

    # Name of the table that logs statistics about job processing,
    # e.g., processing times and outcomes.
    statistics_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_statistics"),
    )

    # Type of ENUM defining possible statuses for entries in the
    # statistics table, such as 'exception' or 'successful'.
    statistics_table_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_statistics_status"),
        kw_only=True,
    )

    # Type of ENUM defining statuses for queue jobs, such as 'queued' or 'picked'.
    queue_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_status"),
        kw_only=True,
    )

    # Name of the main table where jobs are queued before being processed.
    queue_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer"),
        kw_only=True,
    )

    # Name of the trigger that invokes the function to notify changes, applied
    # after DML operations on the queue table.
    trigger: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("tg_pgqueuer_changed"),
        kw_only=True,
    )


@dataclasses.dataclass
class QueryBuilder:
    """
    Generates SQL queries for job queuing operations.
    """

    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    def create_install_query(self) -> str:
        """
        Generates the SQL query string to create necessary database objects for
        the job queue system.

        This includes:

        - Enums for job statuses (e.g., 'queued', 'picked') and log statuses
            (e.g., 'exception', 'successful').
        - Tables for storing job queue data and log statistics with appropriate fields
            such as job ID,
          priority, status, entrypoint, and payload.
        - Indexes to improve the performance of querying these tables.
        - A trigger function that emits notifications upon changes to the
            job queue table, facilitating real-time update mechanisms using
            PostgreSQL's LISTEN/NOTIFY system.
        """

        return f"""
    CREATE TYPE {self.settings.queue_status_type} AS ENUM ('queued', 'picked');
    CREATE TABLE {self.settings.queue_table} (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        status {self.settings.queue_status_type} NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE INDEX {self.settings.queue_table}_priority_id_id1_idx ON {self.settings.queue_table} (priority ASC, id DESC)
        INCLUDE (id) WHERE status = 'queued';
    CREATE INDEX {self.settings.queue_table}_updated_id_id1_idx ON {self.settings.queue_table} (updated ASC, id DESC)
        INCLUDE (id) WHERE status = 'picked';

    CREATE TYPE {self.settings.statistics_table_status_type} AS ENUM ('exception', 'successful');
    CREATE TABLE {self.settings.statistics_table} (
        id SERIAL PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT DATE_TRUNC('sec', NOW() at time zone 'UTC'),
        count BIGINT NOT NULL,
        priority INT NOT NULL,
        time_in_queue INTERVAL NOT NULL,
        status {self.settings.statistics_table_status_type} NOT NULL,
        entrypoint TEXT NOT NULL
    );
    CREATE UNIQUE INDEX {self.settings.statistics_table}_unique_count ON {self.settings.statistics_table} (
        priority,
        DATE_TRUNC('sec', created at time zone 'UTC'),
        DATE_TRUNC('sec', time_in_queue),
        status,
        entrypoint
    );

    CREATE FUNCTION {self.settings.function}() RETURNS TRIGGER AS $$
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
                '{self.settings.channel}',
                json_build_object(
                    'channel', '{self.settings.channel}',
                    'operation', lower(TG_OP),
                    'sent_at', NOW(),
                    'table', TG_TABLE_NAME
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
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER {self.settings.trigger}
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {self.settings.queue_table}
    EXECUTE FUNCTION {self.settings.function}();
        """  # noqa: E501

    def create_uninstall_query(self) -> str:
        """
        Generates the SQL query string to remove all database structures
        related to the job queue system.
        """

        return f"""
    DROP TRIGGER {self.settings.trigger};
    DROP FUNCTION {self.settings.function};
    DROP TABLE {self.settings.queue_table};
    DROP TABLE {self.settings.statistics_table};
    DROP TYPE {self.settings.queue_status_type};
    DROP TYPE {self.settings.statistics_table_status_type};
    """

    def create_dequeue_query(self) -> str:
        """
        Generates the SQL query string to insert a new job into the queue.
        This query sets the job status to 'queued' and includes parameters for priority,
        entrypoint, and payload.
        """
        return f"""
    WITH next_job_queued AS (
        SELECT id
        FROM {self.settings.queue_table}
        WHERE
                entrypoint = ANY($2)
            AND status = 'queued'
        ORDER BY priority DESC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ),
    next_job_retry AS (
        SELECT id
        FROM {self.settings.queue_table}
        WHERE
                entrypoint = ANY($2)
            AND status = 'picked'
            AND ($3::interval IS NOT NULL AND updated < NOW() - $3::interval)
        ORDER BY updated DESC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ),
    combined_jobs AS (
        SELECT DISTINCT id
        FROM (
            SELECT id FROM next_job_queued
            UNION ALL
            SELECT id FROM next_job_retry WHERE $3::interval IS NOT NULL
        ) AS combined
    ),
    updated AS (
        UPDATE {self.settings.queue_table}
        SET status = 'picked', updated = NOW()
        WHERE id = ANY(SELECT id FROM combined_jobs)
        RETURNING *
    )
    SELECT * FROM updated ORDER BY priority DESC, id ASC
    """

    def create_enqueue_query(self) -> str:
        """
        Generates the SQL query string to insert a new job into the queue.
        This query sets the job status to 'queued' and includes parameters for priority,
        entrypoint, and payload.
        """
        return f"""
        INSERT INTO {self.settings.queue_table}
        (priority, entrypoint, payload, status)
        VALUES (unnest($1::int[]), unnest($2::text[]), unnest($3::bytea[]), 'queued')
    """

    def create_delete_from_queue_query(self) -> str:
        """
        Generates the SQL query string to delete jobs from the queue based on a
        list of entrypoints. This query uses the ANY clause to match any of the
        specified entrypoints for deletion.
        """
        return f"DELETE FROM {self.settings.queue_table} WHERE entrypoint = ANY($1)"

    def create_truncate_queue_query(self) -> str:
        """
        Generates the SQL query string to truncate the job queue table,
        effectively removing all jobs from the queue.
        """
        return f"TRUNCATE {self.settings.queue_table}"

    def create_queue_size_query(self) -> str:
        """
        Generates the SQL query string to count the number of jobs in the queue,
        grouped by entrypoint and priority. This helps in understanding the distribution
        of jobs across different priorities and entrypoints.
        """
        return f"""
    SELECT
        count(*) AS count,
        priority,
        entrypoint,
        status
    FROM
        {self.settings.queue_table}
    GROUP BY entrypoint, priority, status
    ORDER BY count, entrypoint, priority, status
    """

    def create_log_job_query(self) -> str:
        """
        Generates the SQL query string to move a completed or failed job from the
        queue table to the log table, capturing details like job priority,
        entrypoint, time in queue, creation time, and final status.
        """
        return f"""
    WITH deleted AS (
        DELETE FROM {self.settings.queue_table}
        WHERE id = ANY($1::integer[])
        RETURNING   id,
                    priority,
                    entrypoint,
                    DATE_TRUNC('sec', created at time zone 'UTC') AS created,
                    DATE_TRUNC('sec', AGE(updated, created)) AS time_in_queue
    ), job_status AS (
        SELECT
            unnest($1::integer[]) AS id,
            unnest($2::{self.settings.statistics_table_status_type}[]) AS status
    ), grouped_data AS (
        SELECT
            priority,
            entrypoint,
            time_in_queue,
            created,
            status,
            count(*)
        FROM deleted JOIN job_status ON job_status.id = deleted.id
        GROUP BY priority, entrypoint, time_in_queue, created, status
    )
    INSERT INTO {self.settings.statistics_table} (
        priority,
        entrypoint,
        time_in_queue,
        created,
        status,
        count
    ) SELECT
        priority,
        entrypoint,
        time_in_queue,
        created,
        status,
        count
    FROM grouped_data
    ON CONFLICT (
        priority,
        entrypoint,
        DATE_TRUNC('sec', created at time zone 'UTC'),
        DATE_TRUNC('sec', time_in_queue),
        status
    )
    DO UPDATE
    SET count = {self.settings.statistics_table}.count + EXCLUDED.count
    """

    def create_truncate_log_query(self) -> str:
        """
        Generates the SQL query string to truncate the job log table,
        effectively removing all logged entries.
        """
        return f"""TRUNCATE {self.settings.statistics_table}"""

    def create_delete_from_log_query(self) -> str:
        """
        Generates the SQL query string to delete entries from the job log table,
        optionally filtered by a list of entrypoints. This allows selective clearing
        of log entries based on their source entrypoints.
        """
        return f"""
    DELETE FROM {self.settings.statistics_table}
    WHERE entrypoint = ANY($1)
    """

    def create_log_statistics_query(self) -> str:
        """
        Generates the SQL query string to retrieve statistical data from the job
        log table, including count of jobs, creation time, time in queue,
        entrypoint, priority, and status, limited to a specified number of most
        recent records defined by 'tail'.
        """
        return f"""
    SELECT
        count,
        created,
        time_in_queue,
        entrypoint,
        priority,
        status
    FROM {self.settings.statistics_table}
    ORDER BY id DESC
    LIMIT $1
    """

    def create_upgrade_queries(self) -> Generator[str, None, None]:
        yield f"ALTER TABLE {self.settings.queue_table} ADD COLUMN IF NOT EXISTS updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"  # noqa: E501
        yield f"CREATE INDEX IF NOT EXISTS {self.settings.queue_table}_updated_id_id1_idx ON {self.settings.queue_table} (updated ASC, id DESC) INCLUDE (id) WHERE status = 'picked';"  # noqa: E501

    def create_has_column_query(self) -> str:
        return """
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_schema = current_schema()
                AND table_name = $1
                AND column_name = $2
            );"""


@dataclasses.dataclass
class Queries:
    """
    Handles operations related to job queuing such as
    enqueueing, dequeueing, and querying the size of the queue.
    """

    driver: db.Driver
    qb: QueryBuilder = dataclasses.field(default_factory=QueryBuilder)

    async def install(self) -> None:
        """
        Creates necessary database structures such as enums,
        tables, and triggers for job queuing and logging.
        """

        await self.driver.execute(self.qb.create_install_query())

    async def uninstall(self) -> None:
        """
        Drops all database structures related to job queuing
        and logging that were created by the install method.
        """
        await self.driver.execute(self.qb.create_uninstall_query())

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: set[str],
        retry_timer: timedelta | None = None,
    ) -> list[models.Job]:
        """
        Retrieves and updates the next 'queued' job to 'picked' status,
        ensuring no two jobs with the same entrypoint are picked simultaneously.

        Parameters:
        - batch_size: The number of jobs to retrieve. Must be greater than one (1).
        - entrypoints: A set of entrypoints to filter the jobs.
        - retry_timer: If specified, selects jobs that have been in 'picked' status for
            longer than the specified retry-timer duration. If `None`, the retry-timer
            checking is skipped.
        """

        if batch_size < 1:
            raise ValueError("Batch size must be greater or equal to one (1)")

        if retry_timer and retry_timer < timedelta(seconds=0):
            raise ValueError("Retry timer must be a non-negative timedelta")

        rows = await self.driver.fetch(
            self.qb.create_dequeue_query(),
            batch_size,
            entrypoints,
            retry_timer,
        )
        return [models.Job.model_validate(dict(row)) for row in rows]

    async def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes] | list[None] | list[bytes | None],
        priority: int | list[int] = 0,
    ) -> None:
        """
        Inserts a new job into the queue with the specified
        entrypoint, payload, and priority, marking it as 'queued'.
        This method ensures that if any of entrypoint, payload, or priority is a list,
        all list arguments must be of the same length.

        If two or more arguments are lists, they must be the same length;
            otherwise, an ValueError is raised.
        """

        # If they are not lists, create single-item lists for uniform processing
        normed_entrypoint = entrypoint if isinstance(entrypoint, list) else [entrypoint]
        normed_payload = payload if isinstance(payload, list) else [payload]
        normed_priority = priority if isinstance(priority, list) else [priority]

        await self.driver.execute(
            self.qb.create_enqueue_query(),
            normed_priority,
            normed_entrypoint,
            normed_payload,
        )

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears jobs from the queue, optionally filtering by entrypoint if specified.
        """
        await (
            self.driver.execute(
                self.qb.create_delete_from_queue_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.driver.execute(self.qb.create_truncate_queue_query())
        )

    async def queue_size(self) -> list[models.QueueStatistics]:
        """
        Returns the number of jobs in the queue grouped by entrypoint and priority.
        """
        return [
            models.QueueStatistics.model_validate(dict(x))
            for x in await self.driver.fetch(self.qb.create_queue_size_query())
        ]

    async def log_jobs(
        self,
        job_status: list[tuple[models.Job, models.STATUS_LOG]],
    ) -> None:
        """
        Moves a completed or failed job from the queue table to the log
        table, recording its final status and duration.
        """
        await self.driver.execute(
            self.qb.create_log_job_query(),
            [j.id for j, _ in job_status],
            [s for _, s in job_status],
        )

    async def clear_log(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears entries from the job log table, optionally filtering
        by entrypoint if specified.
        """
        await (
            self.driver.execute(
                self.qb.create_delete_from_log_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.driver.execute(self.qb.create_truncate_log_query())
        )

    async def log_statistics(self, tail: int) -> list[models.LogStatistics]:
        return [
            models.LogStatistics.model_validate(dict(x))
            for x in await self.driver.fetch(
                self.qb.create_log_statistics_query(), tail
            )
        ]

    async def upgrade(self) -> None:
        await self.driver.execute("\n\n".join(self.qb.create_upgrade_queries()))

    async def has_updated_column(self) -> bool:
        return await self.driver.fetchval(
            self.qb.create_has_column_query(),
            self.qb.settings.queue_table,
            "updated",
        )
