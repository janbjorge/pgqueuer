import dataclasses

import asyncpg

from . import models


@dataclasses.dataclass
class Queries:
    pool: asyncpg.Pool
    prefix: str = dataclasses.field(default="")

    async def install(self) -> None:
        await self.pool.execute(
            f"""
    CREATE TYPE {self.prefix}pgqueuer_status AS ENUM ('queued', 'picked');
    CREATE TYPE {self.prefix}pgqueuer_status_log AS ENUM ('exception', 'successful');

    CREATE TABLE {self.prefix}pgqueuer_log(
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        duration INTERVAL NOT NULL,
        status pgqueuer_status_log NOT NULL,
        entrypoint TEXT NOT NULL
    );

    CREATE TABLE {self.prefix}pgqueuer (
        id SERIAL PRIMARY KEY,
        priority INT NOT NULL,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        status pgqueuer_status NOT NULL,
        entrypoint TEXT NOT NULL,
        payload BYTEA
    );
    CREATE INDEX ON pgqueuer (priority, id) WHERE status != 'picked';

    CREATE FUNCTION {self.prefix}fn_pgqueuer_changed() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify(
        '{self.prefix}ch_pgqueuer',
        json_build_object(
            'channel', '{self.prefix}ch_pgqueuer',
            'operation', lower(TG_OP),
            'sent_at', NOW(),
            'table', TG_TABLE_NAME
        )::text);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER {self.prefix}tg_fn_pgqueuer_changed
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {self.prefix}pgqueuer_log
    EXECUTE FUNCTION fn_pgqueuer_changed();
    """
        )

    async def uninsall(self) -> None:
        await self.pool.execute(
            f"""
    DROP TRIGGER {self.prefix}tg_fn_pgqueuer_changed;
    DROP FUNCTION {self.prefix}fn_pgqueuer_changed;
    DROP TABLE {self.prefix}pgqueuer;
    DROP TABLE {self.prefix}pgqueuer_log;
    DROP TYPE {self.prefix}pgqueuer_status;
    DROP TYPE {self.prefix}pgqueuer_status_log;
    """
        )

    async def next_jobs(self, batch: int = 100) -> models.Jobs:
        assert batch > 0
        return models.Jobs.model_validate(
            map(
                dict,
                await self.pool.fetch(
                    f"""
    WITH next_jobs as (
        SELECT id
        FROM {self.prefix}pgqueuer
        WHERE status != 'picked'
        ORDER BY priority, id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    )
    UPDATE {self.prefix}pgqueuer
    SET status = 'picked'
    WHERE id = ANY(SELECT id FROM next_jobs)
    RETURNING *;
    """,
                    batch,
                ),
            )
        )

    async def set_job_status(self, job: models.Job, status: models.STATUS) -> None:
        await self.pool.execute(
            f"""
    UPDATE {self.prefix}pgqueuer
    SET status = $2
    WHERE id = $1 AND status != $2
    """,
            job.id,
            status,
        )

    async def move_job_log(self, job: models.Job, status: models.STATUS_LOG) -> None:
        await self.pool.execute(
            f"""
    WITH moved_row AS (
        DELETE FROM {self.prefix}pgqueuer
        WHERE id = $1
        RETURNING id, priority, created, entrypoint
    )

    INSERT INTO {self.prefix}pgqueuer_log (
        id, priority, created, duration, status, entrypoint
    )
        SELECT id, priority, created, NOW() - created, $2, entrypoint
        FROM moved_row;
    """,
            job.id,
            status,
        )

    async def put(
        self,
        entrypoint: str,
        payload: bytes,
        priority: int = 0,
    ) -> None:
        await self.pool.execute(
            f"""
    INSERT INTO {self.prefix}pgqueuer (priority, status, entrypoint, payload)
    VALUES ($1, 'queued', $2, $3)""",
            priority,
            entrypoint,
            payload,
        )
