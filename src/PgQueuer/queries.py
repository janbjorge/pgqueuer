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

    channel: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("ch_pgqueuer"),
        kw_only=True,
    )
    function: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("fn_pgqueuer_changed"),
        kw_only=True,
    )
    log_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_log"),
        kw_only=True,
    )
    log_table_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_status_log"),
        kw_only=True,
    )
    queue_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_status"),
        kw_only=True,
    )
    queue_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer"),
        kw_only=True,
    )
    trigger: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("tg_pgqueuer_changed"),
        kw_only=True,
    )


@dataclasses.dataclass
class InstallUninstallQueries:
    """
    Provides methods to install (create) and uninstall (drop)
    database schemas and objects like tables, types, and triggers
    related to the pgqueuer system.
    """

    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def install(self) -> None:
        """
        Creates necessary database structures such as enums,
        tables, and triggers for job queuing and logging.
        """
        await self.pool.execute(
            f"""
    CREATE TYPE {self.settings.queue_status_type} AS ENUM ('queued', 'picked');
    CREATE TABLE {self.settings.queue_table} (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        status {self.settings.queue_status_type} NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE UNIQUE INDEX ON {self.settings.queue_table} (priority, id) WHERE status != 'picked';

    CREATE TYPE {self.settings.log_table_status_type} AS ENUM ('exception', 'successful');
    CREATE TABLE {self.settings.log_table} (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        duration INTERVAL NOT NULL,
        status {self.settings.log_table_status_type} NOT NULL,
        entrypoint TEXT NOT NULL
    );


    CREATE FUNCTION {self.settings.function}() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify(
        '{self.settings.channel}',
        json_build_object(
            'channel', '{self.settings.channel}',
            'operation', lower(TG_OP),
            'sent_at', NOW(),
            'table', TG_TABLE_NAME
        )::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER {self.settings.trigger}
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {self.settings.queue_table}
    EXECUTE FUNCTION {self.settings.function}();
    """  # noqa: E501
        )

    async def uninstall(self) -> None:
        """
        Drops all database structures related to job queuing
        and logging that were created by the install method.
        """
        await self.pool.execute(
            f"""
    DROP TRIGGER {self.settings.trigger};
    DROP FUNCTION {self.settings.function};
    DROP TABLE {self.settings.queue_table};
    DROP TABLE {self.settings.log_table};
    DROP TYPE {self.settings.queue_status_type};
    DROP TYPE {self.settings.log_table_status_type};
    """
        )


@dataclasses.dataclass
class PgQueuerQueries:
    """
    Handles operations related to job queuing such as
    enqueueing, dequeueing, and querying the size of the queue.
    """

    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def dequeue(self) -> models.Jobs:
        """
        Retrieves and updates the next 'queued' job to 'picked'
        status, ensuring no two jobs with the same entrypoint
        are picked simultaneously.
        """
        query = f"""
            WITH next_job AS (
                SELECT p1.id
                FROM {self.settings.queue_table} p1
                WHERE p1.status = 'queued' AND NOT EXISTS (
                    SELECT FROM
                    {self.settings.queue_table} p2
                    WHERE p1.entrypoint = p2.entrypoint AND p2.status = 'picked')
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {self.settings.queue_table}
            SET status = 'picked'
            WHERE id = ANY(SELECT id FROM next_job)
            RETURNING *;
        """

        return models.Jobs.model_validate(map(dict, await self.pool.fetch(query)))

    async def enqueue(
        self,
        entrypoint: str,
        payload: bytes | None,
        priority: int = 0,
    ) -> None:
        """
        Inserts a new job into the queue with the specified
        entrypoint, payload, and priority, marking it as 'queued'.
        """
        await self.pool.execute(
            f"""
    INSERT INTO {self.settings.queue_table} (priority, status, entrypoint, payload)
    VALUES ($1, 'queued', $2, $3)""",
            priority,
            entrypoint,
            payload,
        )

    async def clear(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears jobs from the queue, optionally filtering by entrypoint if specified.
        """
        if entrypoint:
            await self.pool.execute(
                f"DELETE FROM {self.settings.queue_table} WHERE entrypoint = ANY($1)",
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.pool.execute(f"TRUNCATE {self.settings.queue_table}")

    async def qsize(self) -> dict[tuple[str, int], int]:
        """
        Returns the number of jobs in the queue grouped by entrypoint and priority.
        """
        return {
            (row["entrypoint"], row["priority"]): row["cnt"]
            for row in await self.pool.fetch(f"""
        SELECT
            count(*) AS cnt,
            priority,
            entrypoint
        FROM
            {self.settings.queue_table}
        GROUP BY priority, entrypoint
        """)
        }


@dataclasses.dataclass
class PgQueuerLogQueries:
    """
    Manages operations related to job logging,
    including moving jobs to a log table, clearing logs,
    and querying the size of log entries.
    """

    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def move_job_log(self, job: models.Job, status: models.STATUS_LOG) -> None:
        """
        Moves a completed or failed job from the queue table to the log
        table, recording its final status and duration.
        """
        await self.pool.execute(
            f"""
    WITH moved_row AS (
        DELETE FROM {self.settings.queue_table}
        WHERE id = $1
        RETURNING id, priority, created, entrypoint
    )

    INSERT INTO {self.settings.log_table} (
        id, priority, created, duration, status, entrypoint
    )
        SELECT id, priority, created, NOW() - created, $2, entrypoint
        FROM moved_row;
    """,
            job.id,
            status,
        )

    async def clear(self, entrypoint: str | list[str] | None = None) -> None:
        """
        Clears entries from the job log table, optionally filtering
        by entrypoint if specified.
        """
        if entrypoint:
            await self.pool.execute(
                f"DELETE FROM {self.settings.log_table} WHERE entrypoint = ANY($1)",
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.pool.execute(f"TRUNCATE {self.settings.log_table}")

    async def qsize(self) -> dict[tuple[str, str, int], int]:
        """
        Returns the number of log entries grouped by status, entrypoint, and priority.
        """
        return {
            (row["status"], row["entrypoint"], row["priority"]): row["cnt"]
            for row in await self.pool.fetch(f"""
        SELECT
            count(*) AS cnt,
            status,
            priority,
            entrypoint
        FROM
            {self.settings.log_table}
        GROUP BY status, priority, entrypoint
        """)
        }
