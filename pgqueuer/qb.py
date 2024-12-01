"""
Database query builder and executor for job queue operations.

This module provides classes and functions to construct and execute SQL queries
related to job queuing, such as installing the necessary database schema,
enqueueing and dequeueing jobs, logging job statuses, and managing job statistics.
It abstracts the SQL details and offers a high-level interface for interacting
with the database in the context of the pgqueuer application.
"""

from __future__ import annotations

import dataclasses
import os
import re
from typing import Generator

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pgqueuer.models import PGChannel


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


class DBSettings(BaseSettings):
    """
    Configuration settings for database object names with optional prefixes.

    This class contains the names of various database objects used by the job queue
    system, such as tables, functions, triggers, channels, and scheduler tables. The
    settings allow for the generation of object names with configurable prefixes,
    which are set via environment variables. This is useful for avoiding naming
    conflicts and supporting multiple instances with different namespaces.

    Attributes:
        channel (models.PGChannel): The PostgreSQL NOTIFY channel used for queue notifications.
        function (str): The name of the function triggered by changes to the queue table.
        statistics_table (str): The table that logs job processing statistics.
        statistics_table_status_type (str): The ENUM type for job log statuses in the statistics table.
        queue_status_type (str): The ENUM type defining statuses for queue jobs.
        queue_table (str): The main table where jobs are stored before processing.
        trigger (str): The name of the trigger on the queue table, used to invoke the trigger function.
        schedules_table (str): The table that stores the job scheduling information.
    """  # noqa: E501

    model_config = SettingsConfigDict(
        env_prefix=add_prefix(""),
        extra="ignore",
    )

    # Channel name for PostgreSQL LISTEN/NOTIFY used to
    # receive notifications about changes in the queue.
    channel: PGChannel = Field(default=PGChannel(add_prefix("ch_pgqueuer")))

    # Name of the database function triggered by changes to the queue
    # table, used to notify subscribers.
    function: str = Field(default=add_prefix("fn_pgqueuer_changed"))

    # Name of the table that logs statistics about job processing,
    # e.g., processing times and outcomes.
    statistics_table: str = Field(default=add_prefix("pgqueuer_statistics"))

    # Type of ENUM defining possible statuses for entries in the
    # statistics table, such as 'exception' or 'successful'.
    statistics_table_status_type: str = Field(default=add_prefix("pgqueuer_statistics_status"))

    # Type of ENUM defining statuses for queue jobs, such as 'queued' or 'picked'.
    queue_status_type: str = Field(default=add_prefix("pgqueuer_status"))

    # Name of the main table where jobs are queued before being processed.
    queue_table: str = Field(default=add_prefix("pgqueuer"))

    # Name of the trigger that invokes the function to notify changes, applied
    # after DML operations on the queue table.
    trigger: str = Field(default=add_prefix("tg_pgqueuer_changed"))

    # Name of scheduler table
    schedules_table: str = Field(default=add_prefix("pgqueuer_schedules"))


@dataclasses.dataclass
class QueryBuilderEnvironment:
    """
    Setup/teardown environment for executing queries.

    Handles the configuration required for executing SQL queries, ensuring
    consistent settings are applied during setup and teardown operations.

    Attributes:
        settings (DBSettings): Database settings instance.
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
        queue_manager_id UUID,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        execute_after TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        status {self.settings.queue_status_type} NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE INDEX {self.settings.queue_table}_priority_id_id1_idx ON {self.settings.queue_table} (priority ASC, id DESC)
        INCLUDE (id) WHERE status = 'queued';
    CREATE INDEX {self.settings.queue_table}_updated_id_id1_idx ON {self.settings.queue_table} (updated ASC, id DESC)
        INCLUDE (id) WHERE status = 'picked';
    CREATE INDEX {self.settings.queue_table}_queue_manager_id_idx ON {self.settings.queue_table} (queue_manager_id)
        WHERE queue_manager_id IS NOT NULL;

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

    CREATE TABLE {self.settings.schedules_table} (
        id SERIAL PRIMARY KEY,
        expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
        entrypoint TEXT NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE,
        status {self.settings.queue_status_type} DEFAULT 'queued',
        UNIQUE (expression, entrypoint)
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
    DROP TABLE {self.settings.schedules_table};
    DROP TYPE {self.settings.queue_status_type};
    DROP TYPE {self.settings.statistics_table_status_type};
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
        yield f"ALTER TABLE {self.settings.queue_table} ADD COLUMN IF NOT EXISTS queue_manager_id UUID;"  # noqa: E501
        yield f"CREATE INDEX IF NOT EXISTS {self.settings.queue_table}_queue_manager_id_idx ON {self.settings.queue_table} (queue_manager_id) WHERE queue_manager_id IS NOT NULL;"  # noqa: E501
        yield f"""CREATE TABLE IF NOT EXISTS {self.settings.schedules_table} (
        id SERIAL PRIMARY KEY,
        expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
        entrypoint TEXT NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE,
        status {self.settings.queue_status_type} DEFAULT 'queued',
        UNIQUE (expression, entrypoint)
    );"""
        yield f"""ALTER TABLE {self.settings.queue_table} ADD COLUMN IF NOT EXISTS execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();"""  # noqa: E501

    def create_table_has_column_query(self) -> str:
        """
        A query to check if a specific column exists in a table.

        Returns:
            str: The SQL query string to check for a column's existence.
        """
        return """SELECT EXISTS (
        SELECT FROM information_schema.columns
        WHERE
                table_schema = current_schema()
            AND table_name = $1
            AND column_name = $2
        );"""

    def create_has_table_query(self) -> str:
        """
        A query to check if a specific table exists in a table.

        Returns:
            str: The SQL query string to check for a table's existence.
        """
        return """SELECT EXISTS (
        SELECT FROM information_schema.columns
        WHERE
                table_schema = current_schema()
            AND table_name = $1
        );"""

    def create_user_types_query(self) -> str:
        """
        A query to list user-defined ENUM types and their labels.

        Constructs an SQL query that retrieves all ENUM labels and their type
        names from the PostgreSQL system catalogs. This can be used to verify
        the presence of specific ENUM values.

        Returns:
            str: The SQL query string to fetch user-defined ENUM types.
        """
        return """SELECT enumlabel, typname
    FROM pg_enum
    JOIN pg_type ON pg_enum.enumtypid = pg_type.oid"""


@dataclasses.dataclass
class QueryQueueBuilder:
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
            UNNEST($2::text[])      AS entrypoint,
            UNNEST($3::interval[])  AS retry_after,
            UNNEST($4::boolean[])   AS serialized,
            UNNEST($5::bigint[])    AS concurrency_limit
    ),
    jobs_by_queue_manager_entrypoint AS (
        SELECT COUNT(*), entrypoint
        FROM {self.settings.queue_table}
        WHERE
                queue_manager_id IS NOT NULL
            AND queue_manager_id = $6
            AND entrypoint = ANY($2)
        GROUP BY entrypoint
    ),
    next_job_queued AS (
        SELECT {self.settings.queue_table}.id
        FROM {self.settings.queue_table}
        INNER JOIN entrypoint_execution_params
        ON entrypoint_execution_params.entrypoint = {self.settings.queue_table}.entrypoint
        LEFT JOIN jobs_by_queue_manager_entrypoint
        ON jobs_by_queue_manager_entrypoint.entrypoint = {self.settings.queue_table}.entrypoint
        WHERE
                {self.settings.queue_table}.entrypoint = ANY($2)
            AND {self.settings.queue_table}.status = 'queued'
            AND {self.settings.queue_table}.execute_after < NOW()
            AND (
                NOT entrypoint_execution_params.serialized
                OR NOT EXISTS (
                    SELECT 1
                    FROM {self.settings.queue_table} existing_job
                    WHERE existing_job.entrypoint = {self.settings.queue_table}.entrypoint
                    AND existing_job.status = 'picked'
                )
            )
            AND (
                entrypoint_execution_params.concurrency_limit <= 0
                OR jobs_by_queue_manager_entrypoint.count IS NULL
                OR jobs_by_queue_manager_entrypoint.count < entrypoint_execution_params.concurrency_limit
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
            AND {self.settings.queue_table}.execute_after < NOW()
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
        SET status = 'picked', updated = NOW(), heartbeat = NOW(), queue_manager_id = $6
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
        (priority, entrypoint, payload, execute_after, status)
        VALUES (
            UNNEST($1::int[]),                  -- priority
            UNNEST($2::text[]),                 -- entrypoint
            UNNEST($3::bytea[]),                -- payload
            UNNEST($4::interval[]) + NOW(),     -- execute_after
            'queued'                            -- status
        )
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
            UNNEST($1::integer[]) AS id,
            UNNEST($2::{self.settings.statistics_table_status_type}[]) AS status
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

    def create_update_heartbeat_query(self) -> str:
        return f"""UPDATE {self.settings.queue_table} SET heartbeat = NOW() WHERE id = ANY($1::integer[])"""  # noqa: E501


@dataclasses.dataclass
class QuerySchedulerBuilder:
    """
    Generates SQL for job scheduler operations.

    Provides SQL queries for managing scheduled jobs, such as inserting,
    fetching, and updating job schedules.

    Attributes:
        settings (DBSettings): Database settings instance.
    """

    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    def create_insert_schedule_query(self) -> str:
        return f"""WITH params AS (
        SELECT UNNEST($1::text[])       AS expression,
               UNNEST($2::text[])       AS entrypoint,
               UNNEST($3::interval[])   AS delay
        )
        INSERT INTO {self.settings.schedules_table} (expression, next_run, entrypoint)
        SELECT expression, date_trunc('seconds', NOW() + delay), entrypoint FROM params
        ON CONFLICT (entrypoint, expression) DO NOTHING"""

    def create_fetch_schedule_query(self) -> str:
        return f"""WITH params AS (
        SELECT UNNEST($1::text[])       AS expression,
               UNNEST($2::text[])       AS entrypoint,
               UNNEST($3::interval[])   AS delay
    ), picked_jobs AS (
        SELECT id, expression, entrypoint
        FROM {self.settings.schedules_table}
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
    UPDATE {self.settings.schedules_table}
    SET
        status = 'picked',
        updated = NOW(),
        next_run = date_trunc('seconds', NOW() + (
                SELECT delay FROM params
                WHERE
                    params.entrypoint = {self.settings.schedules_table}.entrypoint
                AND params.expression = {self.settings.schedules_table}.expression
            ))
    WHERE (entrypoint, expression) IN (SELECT entrypoint, expression FROM picked_jobs)
    RETURNING *;"""  # noqa: E501

    def create_set_schedule_queued_query(self) -> str:
        return f"""UPDATE {self.settings.schedules_table} SET status = 'queued', last_run = NOW(), updated = NOW() WHERE id = ANY($1);"""  # noqa: E501

    def create_update_schedule_heartbeat(self) -> str:
        return f"""UPDATE {self.settings.schedules_table} SET heartbeat = NOW(), updated = NOW() WHERE id = ANY($1);"""  # noqa: E501

    def create_peak_schedule_query(self) -> str:
        return f"""SELECT * FROM {self.settings.schedules_table} ORDER BY last_run ASC"""

    def create_delete_schedule_query(self) -> str:
        return f"""DELETE FROM {self.settings.schedules_table} WHERE id = ANY($1) OR entrypoint = ANY($2)"""  # noqa: E501

    def create_truncate_schedule_query(self) -> str:
        return f"TRUNCATE {self.settings.schedules_table}"
