import dataclasses
import os
from typing import Final

import asyncpg

from . import models


def add_prefix(string: str) -> str:
    return f"{os.environ.get('PGQUEUER_PREFIX', '')}{string}"


@dataclasses.dataclass
class DBSettings:
    channel: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("ch_pgqueuer"),
    )
    function: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("fn_pgqueuer_changed"),
    )
    log_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_log"),
    )
    log_table_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_status_log"),
    )
    queue_status_type: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer_status"),
    )
    queue_table: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("pgqueuer"),
    )
    trigger: Final[str] = dataclasses.field(
        default_factory=lambda: add_prefix("tg_pgqueuer_changed"),
    )


@dataclasses.dataclass
class InstallUninstallQueries:
    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def install(self) -> None:
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
    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def dequeue(self) -> models.Jobs:
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
        await self.pool.execute(
            f"""
    INSERT INTO {self.settings.queue_table} (priority, status, entrypoint, payload)
    VALUES ($1, 'queued', $2, $3)""",
            priority,
            entrypoint,
            payload,
        )

    async def clear(self, entrypoint: str | list[str] | None = None) -> None:
        if entrypoint:
            await self.pool.execute(
                f"DELETE FROM {self.settings.queue_table} WHERE entrypoint = ANY($1)",
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.pool.execute(f"TRUNCATE {self.settings.queue_table}")

    async def qsize(self) -> dict[tuple[str, int], int]:
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
    pool: asyncpg.Pool
    settings: DBSettings = dataclasses.field(default_factory=DBSettings)

    async def move_job_log(self, job: models.Job, status: models.STATUS_LOG) -> None:
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
        if entrypoint:
            await self.pool.execute(
                f"DELETE FROM {self.settings.log_table} WHERE entrypoint = ANY($1)",
                [entrypoint] if isinstance(entrypoint, str) else entrypoint,
            )
        else:
            await self.pool.execute(f"TRUNCATE {self.settings.log_table}")

    async def qsize(self) -> dict[tuple[str, str, int], int]:
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
