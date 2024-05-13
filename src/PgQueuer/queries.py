from __future__ import annotations

import dataclasses
import os
from typing import Final

import asyncpg

from . import models


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
        created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        status {self.settings.queue_status_type} NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE INDEX ON {self.settings.queue_table} (priority ASC, id DESC)
        INCLUDE (id) WHERE status = 'queued';

    CREATE TYPE {self.settings.statistics_table_status_type} AS ENUM ('exception', 'successful');
    CREATE TABLE {self.settings.statistics_table} (
        id SERIAL PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT DATE_TRUNC('sec', CURRENT_TIMESTAMP at time zone 'UTC'),
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
    WITH next_job AS (
        SELECT id
        FROM {self.settings.queue_table}
        WHERE status = 'queued' AND entrypoint = ANY($2)
        ORDER BY priority DESC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ),
    updated AS (
        UPDATE {self.settings.queue_table}
        SET status = 'picked'
        WHERE id = ANY(SELECT id FROM next_job)
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
    INSERT INTO {self.settings.queue_table} (priority, status, entrypoint, payload)
    VALUES ($1, 'queued', $2, $3)
    """  # noqa: E501

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
        WHERE id = $1
    )

    INSERT INTO {self.settings.statistics_table} (
        priority,
        entrypoint,
        time_in_queue,
        created,
        status,
        count
    )
    VALUES (
        $2,                                         -- priority
        $3,                                         -- entrypoint
        DATE_TRUNC('sec', NOW() - $4),              -- time_in_queue
        DATE_TRUNC('sec', $4 at time zone 'UTC'),   -- created at time zone 'UTC'
        $5,                                         -- status
        1                                           -- start count
    )
    ON CONFLICT (
        priority,
        entrypoint,
        DATE_TRUNC('sec', created at time zone 'UTC'),
        DATE_TRUNC('sec', time_in_queue),
        status
    )
    DO UPDATE
        SET
            count = {self.settings.statistics_table}.count + 1
        WHERE
                {self.settings.statistics_table}.priority = $2
            AND {self.settings.statistics_table}.entrypoint = $3
            AND DATE_TRUNC('sec', {self.settings.statistics_table}.time_in_queue) = DATE_TRUNC('sec', NOW() - $4)
            AND DATE_TRUNC('sec', {self.settings.statistics_table}.created) = DATE_TRUNC('sec', $4 at time zone 'UTC')
            AND {self.settings.statistics_table}.status = $5
    """  # noqa: E501

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


@dataclasses.dataclass
class Queries:
    """
    Handles operations related to job queuing such as
    enqueueing, dequeueing, and querying the size of the queue.
    """

    pool: asyncpg.Pool
    qb: QueryBuilder = dataclasses.field(default_factory=QueryBuilder)

    async def install(self) -> None:
        """
        Creates necessary database structures such as enums,
        tables, and triggers for job queuing and logging.
        """

        await self.pool.execute(self.qb.create_install_query())

    async def uninstall(self) -> None:
        """
        Drops all database structures related to job queuing
        and logging that were created by the install method.
        """
        await self.pool.execute(self.qb.create_uninstall_query())

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: set[str],
    ) -> list[models.Job]:
        """
        Retrieves and updates the next 'queued' job to 'picked'
        status, ensuring no two jobs with the same entrypoint
        are picked simultaneously.
        """
        if batch_size < 1:
            raise ValueError("Batch size must be greter then one (1)")

        rows = await self.pool.fetch(
            self.qb.create_dequeue_query(),
            batch_size,
            entrypoints,
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

        await self.pool.executemany(
            self.qb.create_enqueue_query(),
            zip(
                normed_priority,
                normed_entrypoint,
                normed_payload,
                strict=True,
            ),
        )

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears jobs from the queue, optionally filtering by entrypoint if specified.
        """
        await (
            self.pool.execute(
                self.qb.create_delete_from_queue_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.pool.execute(self.qb.create_truncate_queue_query())
        )

    async def queue_size(self) -> list[models.QueueStatistics]:
        """
        Returns the number of jobs in the queue grouped by entrypoint and priority.
        """
        return [
            models.QueueStatistics.model_validate(dict(x))
            for x in await self.pool.fetch(self.qb.create_queue_size_query())
        ]

    async def log_job(self, job: models.Job, status: models.STATUS_LOG) -> None:
        """
        Moves a completed or failed job from the queue table to the log
        table, recording its final status and duration.
        """
        await self.pool.execute(
            self.qb.create_log_job_query(),
            job.id,
            job.priority,
            job.entrypoint,
            job.created,
            status,
        )

    async def clear_log(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears entries from the job log table, optionally filtering
        by entrypoint if specified.
        """
        await (
            self.pool.execute(
                self.qb.create_delete_from_log_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.pool.execute(self.qb.create_truncate_log_query())
        )

    async def log_statistics(self, tail: int) -> list[models.LogStatistics]:
        return [
            models.LogStatistics.model_validate(dict(x))
            for x in await self.pool.fetch(self.qb.create_log_statistics_query(), tail)
        ]
