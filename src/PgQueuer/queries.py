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
    statistics_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_statistics"),
    )
    statistics_table_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_statistics_status"),
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
class Queries:
    """
    Handles operations related to job queuing such as
    enqueueing, dequeueing, and querying the size of the queue.
    """

    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def install(self) -> None:
        """
        Creates necessary database structures such as enums,
        tables, and triggers for job queuing and logging.
        """

        query = f"""
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
            CREATE UNIQUE INDEX unique_count ON {self.settings.statistics_table} (
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

        await self.pool.execute(query)

    async def uninstall(self) -> None:
        """
        Drops all database structures related to job queuing
        and logging that were created by the install method.
        """
        query = f"""
            DROP TRIGGER {self.settings.trigger};
            DROP FUNCTION {self.settings.function};
            DROP TABLE {self.settings.queue_table};
            DROP TABLE {self.settings.statistics_table};
            DROP TYPE {self.settings.queue_status_type};
            DROP TYPE {self.settings.statistics_table_status_type};
        """
        await self.pool.execute(query)

    async def dequeue(self) -> models.Job | None:
        """
        Retrieves and updates the next 'queued' job to 'picked'
        status, ensuring no two jobs with the same entrypoint
        are picked simultaneously.
        """
        query = f"""
            WITH next_job AS (
                SELECT id
                FROM {self.settings.queue_table}
                WHERE status = 'queued'
                ORDER BY priority DESC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {self.settings.queue_table}
            SET status = 'picked'
            WHERE id = ANY(SELECT id FROM next_job)
            RETURNING *;
        """

        if row := await self.pool.fetchrow(query):
            return models.Job.model_validate(dict(row))
        return None

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

        query = f"""
            INSERT INTO {self.settings.queue_table} (priority, status, entrypoint, payload)
            VALUES ($1, 'queued', $2, $3)
        """  # noqa: E501

        await self.pool.executemany(
            query,
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
        if entrypoint:
            query = (
                f"DELETE FROM {self.settings.queue_table} WHERE entrypoint = ANY($1)"
            )
            await self.pool.execute(
                query,
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            query = f"TRUNCATE {self.settings.queue_table}"
            await self.pool.execute(query)

    async def queue_size(self) -> list[models.QueueStatistics]:
        """
        Returns the number of jobs in the queue grouped by entrypoint and priority.
        """
        query = f"""
            SELECT
                count(*) AS count,
                priority,
                entrypoint
            FROM
                {self.settings.queue_table}
            GROUP BY entrypoint, priority
            ORDER BY count, entrypoint, priority
        """
        return [
            models.QueueStatistics.model_validate(dict(x))
            for x in await self.pool.fetch(query)
        ]

    async def log_job(self, job: models.Job, status: models.STATUS_LOG) -> None:
        """
        Moves a completed or failed job from the queue table to the log
        table, recording its final status and duration.
        """

        query = f"""
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
                    AND DATE_TRUNC('sec', {self.settings.statistics_table}.time_in_queue) = DATE_TRUNC('sec', NOW() - $4)
                    AND DATE_TRUNC('sec', {self.settings.statistics_table}.created) = DATE_TRUNC('sec', $4 at time zone 'UTC')
                    AND {self.settings.statistics_table}.status = $5
                    AND {self.settings.statistics_table}.entrypoint = $3

         """  # noqa: E501

        await self.pool.execute(
            query,
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
        if entrypoint:
            query = f"""
                DELETE FROM {self.settings.statistics_table}
                WHERE entrypoint = ANY($1)
            """
            await self.pool.execute(
                query,
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            query = f"TRUNCATE {self.settings.statistics_table}"
            await self.pool.execute(query)

    async def log_statistics(self, tail: int) -> list[models.LogStatistics]:
        query = f"""
            SELECT
                count,
                created,
                time_in_queue,
                entrypoint,
                priority,
                status
            FROM {self.settings.statistics_table}
            ORDER BY (
                id, created, count, time_in_queue,
                entrypoint, priority, status
            ) DESC
            LIMIT $1
        """
        return [
            models.LogStatistics.model_validate(dict(x))
            for x in await self.pool.fetch(query, tail)
        ]
