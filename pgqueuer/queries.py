"""
Database query builder and executor for job queue operations.

This module provides classes and functions to construct and execute SQL queries
related to job queuing, such as installing the necessary database schema,
enqueueing and dequeueing jobs, logging job statuses, and managing job statistics.
It abstracts the SQL details and offers a high-level interface for interacting
with the database in the context of the pgqueuer application.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import re
from datetime import timedelta
from typing import Final, Generator, overload

from . import db, helpers, models


def add_prefix(string: str) -> str:
    """
    Append a prefix from environment variables to a given string.

    This function prepends the value of the 'PGQUEUER_PREFIX' environment variable
    to the provided string. It is typically used to add a consistent prefix to
    database object names (e.g., tables, triggers) to avoid naming conflicts
    or to namespace the objects.

    Args:
        string (str): The base string to which the prefix will be added.

    Returns:
        str: The string with the prefix appended.
    """

    env = os.environ.get("PGQUEUER_PREFIX", "")
    # - Starts with a letter or underscore
    # - Contains only letters, numbers, and underscores
    # - No dots or special characters
    if env and not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", env):
        raise ValueError(
            "Invalid prefix: The 'PGQUEUER_PREFIX' environment variable must "
            "start with a letter or underscore and contain only letters, "
            "numbers, and underscores. It cannot contain '.', spaces, or "
            "other special characters."
        )

    return f"{env}{string}"


@dataclasses.dataclass
class DBSettings:
    """
    Configuration settings for database object names with optional prefixes.

    This class holds the names of various database objects used by the job queue
    system, such as tables, functions, triggers, and channels. It allows for
    dynamic generation of these names, incorporating a prefix if specified via
    environment variables. This aids in avoiding naming collisions and in
    deploying multiple instances of the application using different namespaces.

    Attributes:
        channel (Final[str]): The name of the PostgreSQL NOTIFY channel used for notifications.
        function (Final[str]): The name of the trigger function invoked on table changes.
        statistics_table (Final[str]): The name of the table that stores job processing statistics.
        statistics_table_status_type (Final[str]): The name of the ENUM type for log statuses.
        queue_status_type (Final[str]): The name of the ENUM type for queue job statuses.
        queue_table (Final[str]): The name of the main queue table where jobs are stored.
        trigger (Final[str]): The name of the trigger that calls the trigger function.
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
    Generates SQL queries for job queue operations.

    This class provides methods to create SQL query strings required for various
    database operations related to the job queue, such as installing or uninstalling
    the schema, enqueueing and dequeueing jobs, updating job statuses, and retrieving
    statistics. It uses the settings from the `DBSettings` class to ensure that
    object names are correctly prefixed.

    Attributes:
        settings (DBSettings): An instance of `DBSettings` containing database object names.
    """

    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    def create_install_query(self) -> str:
        """
        Generate SQL statements to install the job queue schema.

        Constructs the SQL commands needed to set up the database schema required
        by the job queue system. This includes creating custom ENUM types for statuses,
        creating the queue and statistics tables, defining indexes to optimize queries,
        and setting up triggers and functions for notifications.

        Returns:
            str: A string containing the SQL commands to install the schema.
        """

        return f"""CREATE TYPE {self.settings.queue_status_type} AS ENUM ('queued', 'picked');
    CREATE TABLE {self.settings.queue_table} (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        status {self.settings.queue_status_type} NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE INDEX {self.settings.queue_table}_priority_id_id1_idx ON {self.settings.queue_table} (priority ASC, id DESC)
        INCLUDE (id) WHERE status = 'queued';
    CREATE INDEX {self.settings.queue_table}_updated_id_id1_idx ON {self.settings.queue_table} (updated ASC, id DESC)
        INCLUDE (id) WHERE status = 'picked';

    CREATE TYPE {self.settings.statistics_table_status_type} AS ENUM ('exception', 'successful', 'canceled');
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
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER {self.settings.trigger}
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {self.settings.queue_table}
    EXECUTE FUNCTION {self.settings.function}();
        """  # noqa: E501

    def create_uninstall_query(self) -> str:
        """
        Generate SQL statements to uninstall the job queue schema.

        Constructs the SQL commands needed to remove all database objects created
        by the `create_install_query` method. This includes dropping triggers,
        functions, tables, and custom ENUM types.

        Returns:
            str: A string containing the SQL commands to uninstall the schema.
        """
        return f"""DROP TRIGGER {self.settings.trigger} ON {self.settings.queue_table};
    DROP FUNCTION {self.settings.function};
    DROP TABLE {self.settings.queue_table};
    DROP TABLE {self.settings.statistics_table};
    DROP TYPE {self.settings.queue_status_type};
    DROP TYPE {self.settings.statistics_table_status_type};
    """

    def create_dequeue_query(self) -> str:
        """
        Generate SQL query to retrieve and update the next jobs from the queue.

        Constructs an SQL query that selects jobs from the queue based on their
        status ('queued' or 'picked'), priority, and entrypoints. It updates the
        selected jobs to 'picked' status and sets the 'updated' timestamp to now.
        This ensures that jobs are processed in a controlled manner and supports
        retrying jobs that have been picked but not processed within a certain time.

        Returns:
            str: The SQL query string for dequeuing jobs.
        """

        return f"""WITH
    entrypoint_execution_params AS (
        SELECT
            unnest($2::text[]) AS entrypoint,
            unnest($3::interval[]) AS retry_after,
            unnest($4::boolean[]) AS serialized
    ),
    next_job_queued AS (
        SELECT {self.settings.queue_table}.id
        FROM {self.settings.queue_table}
        INNER JOIN entrypoint_execution_params
        ON entrypoint_execution_params.entrypoint = {self.settings.queue_table}.entrypoint
        WHERE
            {self.settings.queue_table}.entrypoint = ANY($2)
            AND {self.settings.queue_table}.status = 'queued'
            AND (
                NOT entrypoint_execution_params.serialized
                OR NOT EXISTS (
                    SELECT 1
                    FROM {self.settings.queue_table} existing_job
                    WHERE existing_job.entrypoint = {self.settings.queue_table}.entrypoint
                    AND existing_job.status = 'picked'
                )
            )
        ORDER BY {self.settings.queue_table}.priority DESC, {self.settings.queue_table}.id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ),
    next_job_retry AS (
        SELECT {self.settings.queue_table}.id
        FROM {self.settings.queue_table}
        INNER JOIN entrypoint_execution_params
        ON entrypoint_execution_params.entrypoint = {self.settings.queue_table}.entrypoint
        WHERE
            {self.settings.queue_table}.entrypoint = ANY($2)
            AND {self.settings.queue_table}.status = 'picked'
            AND entrypoint_execution_params.retry_after > interval '0'
            AND {self.settings.queue_table}.heartbeat < NOW() - entrypoint_execution_params.retry_after
        ORDER BY {self.settings.queue_table}.heartbeat DESC, {self.settings.queue_table}.id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    ),
    combined_jobs AS (
        SELECT DISTINCT id
        FROM (
            SELECT id FROM next_job_queued
            UNION ALL
            SELECT id FROM next_job_retry
        ) AS combined
    ),
    updated AS (
        UPDATE {self.settings.queue_table}
        SET status = 'picked', updated = NOW(), heartbeat = NOW()
        WHERE id = ANY(SELECT id FROM combined_jobs)
        RETURNING *
    )
    SELECT * FROM updated ORDER BY priority DESC, id ASC;
    """  # noqa

    def create_enqueue_query(self) -> str:
        """
        Generate SQL query to insert new jobs into the queue.

        Constructs an SQL query that inserts new jobs into the queue table with
        specified priorities, entrypoints, and payloads. The jobs are initially
        set to 'queued' status.

        Returns:
            str: The SQL query string for enqueueing jobs.
        """

        return f"""INSERT INTO {self.settings.queue_table}
        (priority, entrypoint, payload, status)
        VALUES (unnest($1::int[]), unnest($2::text[]), unnest($3::bytea[]), 'queued')
        RETURNING id
    """

    def create_delete_from_queue_query(self) -> str:
        """
        Generate SQL query to delete jobs from the queue based on entrypoints.

        Constructs an SQL query that deletes jobs from the queue table where the
        entrypoint matches any in a provided list. This allows for selective
        removal of jobs associated with specific entrypoints.

        Returns:
            str: The SQL query string for deleting jobs from the queue.
        """
        return f"DELETE FROM {self.settings.queue_table} WHERE entrypoint = ANY($1)"

    def create_truncate_queue_query(self) -> str:
        """
        Generate SQL query to truncate the entire queue table.

        Constructs an SQL command that removes all records from the queue table.
        This is a fast operation that effectively resets the queue by deleting
        all jobs.

        Returns:
            str: The SQL command string to truncate the queue table.
        """

        return f"TRUNCATE {self.settings.queue_table}"

    def create_queue_size_query(self) -> str:
        """
        Generate SQL query to count the number of jobs in the queue.

        Constructs an SQL query that counts the jobs in the queue table,
        grouping them by entrypoint, priority, and status. This provides
        insight into the current load and distribution of jobs.

        Returns:
            str: The SQL query string to get queue size statistics.
        """

        return f"""SELECT
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
        Generate SQL query to move jobs from the queue to the log table.

        Constructs an SQL query that deletes specified jobs from the queue table
        and inserts corresponding entries into the statistics (log) table.
        It captures details such as priority, entrypoint, time in queue,
        creation time, and final status. The query uses upsert logic to handle
        conflicts and aggregate counts.

        Returns:
            str: The SQL query string to log jobs.
        """

        return f"""WITH deleted AS (
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
        Generate SQL query to truncate the statistics (log) table.

        Constructs an SQL command that removes all records from the statistics
        table. This effectively clears the job processing history.

        Returns:
            str: The SQL command string to truncate the log table.
        """
        return f"""TRUNCATE {self.settings.statistics_table}"""

    def create_delete_from_log_query(self) -> str:
        """
        Generate SQL query to delete entries from the log table based on entrypoints.

        Constructs an SQL query that deletes records from the statistics table
        where the entrypoint matches any in a provided list. This allows for
        selective cleanup of log entries.

        Returns:
            str: The SQL query string to delete entries from the log table.
        """
        return f"""DELETE FROM {self.settings.statistics_table} WHERE entrypoint = ANY($1)"""

    def create_log_statistics_query(self) -> str:
        """
        Generate SQL query to retrieve job processing statistics.

        Constructs an SQL query that selects statistical data from the
        statistics table, including counts, creation times, time in queue,
        entrypoints, priorities, and statuses. The results can be limited
        by a tail value and a time window.

        Returns:
            str: The SQL query string to fetch log statistics.
        """
        return f"""SELECT
        count,
        created,
        time_in_queue,
        entrypoint,
        priority,
        status
    FROM {self.settings.statistics_table}
    WHERE ($2::float IS NULL OR created > NOW() - make_interval(secs=>$2))
    ORDER BY id DESC
    LIMIT $1
    """

    def create_upgrade_queries(self) -> Generator[str, None, None]:
        """
        Generate SQL queries required to upgrade the existing schema.

        Yields SQL commands needed to modify the existing database schema to
        a newer version, such as adding columns, indexes, or modifying functions.
        This is useful when the application schema evolves over time.

        Yields:
            Generator[str, None, None]: A generator that yields SQL commands.
        """
        yield f"ALTER TABLE {self.settings.queue_table} ADD COLUMN IF NOT EXISTS updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"  # noqa: E501
        yield f"CREATE INDEX IF NOT EXISTS {self.settings.queue_table}_updated_id_id1_idx ON {self.settings.queue_table} (updated ASC, id DESC) INCLUDE (id) WHERE status = 'picked';"  # noqa: E501
        yield f"""CREATE OR REPLACE FUNCTION {self.settings.function}() RETURNS TRIGGER AS $$
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
        yield f"ALTER TYPE {self.settings.statistics_table_status_type} ADD VALUE IF NOT EXISTS 'canceled';"  # noqa: E501
        yield f"ALTER TABLE {self.settings.queue_table} ADD COLUMN IF NOT EXISTS heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"  # noqa: E501
        yield f"CREATE INDEX IF NOT EXISTS {self.settings.queue_table}_heartbeat_id_id1_idx ON {self.settings.queue_table} (heartbeat ASC, id DESC) INCLUDE (id) WHERE status = 'picked';"  # noqa: E501

    def create_has_column_query(self) -> str:
        """
        Generate SQL query to check if a specific column exists in a table.

        Constructs an SQL query that checks the information schema for the
        existence of a column within a table in the current schema.

        Returns:
            str: The SQL query string to check for a column's existence.
        """
        return """
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_schema = current_schema()
                AND table_name = $1
                AND column_name = $2
            );"""

    def create_user_types_query(self) -> str:
        """
        Generate SQL query to list user-defined ENUM types and their labels.

        Constructs an SQL query that retrieves all ENUM labels and their type
        names from the PostgreSQL system catalogs. This can be used to verify
        the presence of specific ENUM values.

        Returns:
            str: The SQL query string to fetch user-defined ENUM types.
        """
        return """SELECT enumlabel, typname
    FROM pg_enum
    JOIN pg_type ON pg_enum.enumtypid = pg_type.oid"""

    def create_notify_query(self) -> str:
        """
        Generate SQL query to send a notification via PostgreSQL NOTIFY.

        Constructs an SQL command that sends a notification on the configured
        channel with a specified payload. This is used to inform listeners
        about events such as job cancellations or rate adjustments.

        Returns:
            str: The SQL command string to send a notification.
        """
        return f"""SELECT pg_notify('{self.settings.channel}', $1)"""

    def create_notify_activity_query(self) -> str:
        return f"""UPDATE {self.settings.queue_table} SET heartbeat = NOW() WHERE id = ANY($1::integer[])"""  # noqa: E501


@dataclasses.dataclass
class Queries:
    """
    High-level interface for executing job queue operations.

    This class provides methods to perform actions on the job queue and statistics
    tables, such as installing or uninstalling the schema, enqueueing and dequeuing jobs,
    logging job statuses, clearing the queue or logs, and retrieving statistics.
    It utilizes the SQL queries generated by `QueryBuilder` and executes them using
    the provided database driver.

    Attributes:
        driver (db.Driver): The database driver used to execute SQL commands.
        qb (QueryBuilder): An instance of `QueryBuilder` to generate SQL queries.
    """

    driver: db.Driver
    qb: QueryBuilder = dataclasses.field(default_factory=QueryBuilder)

    async def install(self) -> None:
        """
        Install the job queue schema in the database.

        Executes the SQL commands generated by `create_install_query` to set up
        the necessary tables, types, indexes, triggers, and functions required
        for the job queue system to operate.

        This method should be called during the initial setup of the application.
        """
        await self.driver.execute(self.qb.create_install_query())

    async def uninstall(self) -> None:
        """
        Uninstall the job queue schema from the database.

        Executes the SQL commands generated by `create_uninstall_query` to remove
        all database objects created during installation. This includes dropping
        tables, types, triggers, and functions.

        Use this method with caution, as it will delete all data and schema
        related to the job queue system.
        """
        await self.driver.execute(self.qb.create_uninstall_query())

    async def dequeue(
        self,
        batch_size: int,
        entrypoints: dict[str, tuple[timedelta, bool]],
    ) -> list[models.Job]:
        """
        Retrieve and update jobs from the queue to be processed.

        Selects jobs from the queue that match the specified entrypoints and updates
        their status to 'picked'. The selection prioritizes 'queued' jobs but can
        also include 'picked' jobs that have exceeded the retry timer, allowing
        for retries of stalled jobs.

        Args:
            batch_size (int): The maximum number of jobs to retrieve.
            entrypoints (set[str]): A set of entrypoints to filter the jobs.
            retry_timer (timedelta | None): The duration after which 'picked' jobs
                are considered for retry. If None, retry logic is skipped.

        Returns:
            list[models.Job]: A list of Job instances representing the dequeued jobs.

        Raises:
            ValueError: If batch_size is less than 1 or retry_timer is negative.
        """

        if batch_size < 1:
            raise ValueError("Batch size must be greater than or equal to one (1)")

        rows = await self.driver.fetch(
            self.qb.create_dequeue_query(),
            batch_size,
            list(entrypoints.keys()),
            [t for t, _ in entrypoints.values()],
            [s for _, s in entrypoints.values()],
        )
        return [models.Job.model_validate(dict(row)) for row in rows]

    @overload
    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
    ) -> list[models.JobId]: ...

    @overload
    async def enqueue(
        self,
        entrypoint: list[str],
        payload: list[bytes | None],
        priority: list[int],
    ) -> list[models.JobId]: ...

    async def enqueue(
        self,
        entrypoint: str | list[str],
        payload: bytes | None | list[bytes | None],
        priority: int | list[int] = 0,
    ) -> list[models.JobId]:
        """
        Insert new jobs into the queue.

        Adds one or more jobs to the queue with specified entrypoints, payloads,
        and priorities. Supports inserting multiple jobs in a single operation
        by accepting lists for the parameters.

        Args:
            entrypoint (str | list[str]): The entrypoint(s) associated with the job(s).
            payload (bytes | None | list[bytes | None]): The payload(s) for the job(s).
            priority (int | list[int]): The priority level(s) for the job(s).

        Returns:
            list[models.JobId]: A list of JobId instances representing the IDs of the enqueued jobs.

        Raises:
            ValueError: If the lengths of the lists provided do not match when using multiple jobs.
        """

        # If they are not lists, create single-item lists for uniform processing
        normed_entrypoint = entrypoint if isinstance(entrypoint, list) else [entrypoint]
        normed_payload = payload if isinstance(payload, list) else [payload]
        normed_priority = priority if isinstance(priority, list) else [priority]

        return [
            models.JobId(row["id"])
            for row in await self.driver.fetch(
                self.qb.create_enqueue_query(),
                normed_priority,
                normed_entrypoint,
                normed_payload,
            )
        ]

    async def clear_queue(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Remove jobs from the queue, optionally filtered by entrypoints.

        Deletes jobs from the queue table. If entrypoints are provided, only jobs
        matching those entrypoints are removed; otherwise, the entire queue is cleared.

        Args:
            entrypoint (str | list[str] | None): The entrypoint(s) to filter jobs for deletion.
        """
        await (
            self.driver.execute(
                self.qb.create_delete_from_queue_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.driver.execute(self.qb.create_truncate_queue_query())
        )

    async def mark_job_as_cancelled(self, ids: list[models.JobId]) -> None:
        """
        Mark specific jobs as cancelled and notify the system.

        Moves the specified jobs from the queue table to the statistics table with
        a status of 'canceled' and sends a cancellation event notification.

        Args:
            ids (list[models.JobId]): The IDs of the jobs to cancel.
        """
        await asyncio.gather(
            self.driver.execute(self.qb.create_log_job_query(), ids, ["canceled"] * len(ids)),
            self.notify_job_cancellation(ids),
        )

    async def queue_size(self) -> list[models.QueueStatistics]:
        """
        Get statistics about the current size of the queue.

        Retrieves the number of jobs in the queue, grouped by entrypoint, priority,
        and status. This provides insight into the workload and helps with monitoring.

        Returns:
            list[models.QueueStatistics]: A list of statistics entries for the queue.
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
        Move completed or failed jobs from the queue to the log table.

        Processes a list of jobs along with their final statuses, removing them
        from the queue table and recording their details in the statistics table.

        Args:
            job_status (list[tuple[models.Job, models.STATUS_LOG]]): A list of tuples
                containing jobs and their corresponding statuses
                ('successful', 'exception', or 'canceled').
        """
        await self.driver.execute(
            self.qb.create_log_job_query(),
            [j.id for j, _ in job_status],
            [s for _, s in job_status],
        )

    async def clear_log(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Remove entries from the statistics (log) table.

        Deletes log entries from the statistics table. If entrypoints are provided,
        only entries matching those entrypoints are removed; otherwise, the entire
        log is cleared.

        Args:
            entrypoint (str | list[str] | None): The entrypoint(s) to filter log
                entries for deletion.
        """
        await (
            self.driver.execute(
                self.qb.create_delete_from_log_query(),
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
            if entrypoint
            else self.driver.execute(self.qb.create_truncate_log_query())
        )

    async def log_statistics(
        self,
        tail: int | None,
        last: timedelta | None = None,
    ) -> list[models.LogStatistics]:
        """
        Retrieve job processing statistics from the log.

        Fetches entries from the statistics table, optionally limited by the number
        of recent entries (`tail`) and a time window (`last`). This information
        can be used for monitoring and analysis.

        Args:
            tail (int | None): The maximum number of recent entries to retrieve.
            last (timedelta | None): The time window to consider (e.g., last hour).

        Returns:
            list[models.LogStatistics]: A list of log statistics entries.
        """
        return [
            models.LogStatistics.model_validate(dict(x))
            for x in await self.driver.fetch(
                self.qb.create_log_statistics_query(),
                tail,
                None if last is None else last.total_seconds(),
            )
        ]

    async def upgrade(self) -> None:
        """
        Upgrade the existing database schema to the latest version.

        Executes the SQL commands generated by `create_upgrade_queries` to modify
        the database schema as needed. This may involve adding columns, indexes,
        or updating functions to support new features.

        This method should be called when updating the application to a new version
        that requires schema changes.
        """
        await self.driver.execute("\n\n".join(self.qb.create_upgrade_queries()))

    async def has_updated_column(self) -> bool:
        """
        Check if the 'updated' column exists in the queue table.

        Determines whether the 'updated' timestamp column is present in the queue
        table, which is necessary for certain queue operations, such as retrying jobs.

        Returns:
            bool: True if the 'updated' column exists, False otherwise.
        """
        rows = await self.driver.fetch(
            self.qb.create_has_column_query(),
            self.qb.settings.queue_table,
            "updated",
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def has_heartbeat_column(self) -> bool:
        """
        Check if the 'heartbeat' column exists in the queue table.

        Determines whether the 'heartbeat' timestamp column is present in the queue
        table, which is necessary for certain queue operations, such as retrying jobs.

        Returns:
            bool: True if the 'heartbeat' column exists, False otherwise.
        """
        rows = await self.driver.fetch(
            self.qb.create_has_column_query(),
            self.qb.settings.queue_table,
            "heartbeat",
        )
        assert len(rows) == 1
        (row,) = rows
        return row["exists"]

    async def has_user_type(
        self,
        key: str,
        user_type: str,
    ) -> bool:
        """
        Check if a specific value exists within a user-defined ENUM type.

        Queries the database to determine whether the specified ENUM type contains
        the given label (value). This is useful for ensuring that required statuses
        or values are defined in the database schema.

        Args:
            key (str): The label to look for within the ENUM type.
            user_type (str): The name of the user-defined ENUM type.

        Returns:
            bool: True if the label exists within the ENUM type, False otherwise.
        """
        rows = await self.driver.fetch(self.qb.create_user_types_query())
        return (key, user_type) in ((row["enumlabel"], row["typname"]) for row in rows)

    async def notify_debounce_event(
        self,
        entrypoing: str,
        quantity: int,
    ) -> None:
        """
        Send a requests-per-second event notification for an entrypoint.

        Emits a 'requests_per_second_event' notification via the PostgreSQL NOTIFY
        system to inform other components about the current request rate for an
        entrypoint. This can be used to adjust processing rates or trigger scaling.

        Args:
            entrypoint (str): The entrypoint for which the event is being sent.
            quantity (int): The number of requests per second to report.
        """
        await self.driver.execute(
            self.qb.create_notify_query(),
            models.RequestsPerSecondEvent(
                channel=self.qb.settings.channel,
                entrypoint=entrypoing,
                count=quantity,
                sent_at=helpers.perf_counter_dt(),
                type="requests_per_second_event",
            ).model_dump_json(),
        )

    async def notify_job_cancellation(self, ids: list[models.JobId]) -> None:
        """
        Send a cancellation event notification for specific job IDs.

        Emits a 'cancellation_event' notification via the PostgreSQL NOTIFY system
        to inform other components that certain jobs have been cancelled. This
        allows running tasks to check for cancellation and terminate if necessary.

        Args:
            ids (list[models.JobId]): The IDs of the jobs that have been cancelled.
        """
        await self.driver.execute(
            self.qb.create_notify_query(),
            models.CancellationEvent(
                channel=self.qb.settings.channel,
                ids=ids,
                sent_at=helpers.perf_counter_dt(),
                type="cancellation_event",
            ).model_dump_json(),
        )

    async def notify_activity(self, job_ids: list[models.JobId]) -> None:
        await self.driver.execute(
            self.qb.create_notify_activity_query(),
            list(set(job_ids)),
        )
