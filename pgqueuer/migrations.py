"""
Lightweight migration framework for PGQueuer.

Tracks applied migrations in a dedicated table, ensuring each migration
runs exactly once. No external dependencies.
"""

from __future__ import annotations

import dataclasses
import hashlib
from typing import TYPE_CHECKING, Callable

from . import qb

if TYPE_CHECKING:
    from . import db


@dataclasses.dataclass
class Migration:
    """Represents a single database migration."""

    version: str
    description: str
    sql_generator: Callable[[], str]

    def checksum(self) -> str:
        """Calculate MD5 checksum for integrity verification."""
        sql = self.sql_generator()
        return hashlib.md5(sql.encode()).hexdigest()


@dataclasses.dataclass
class MigrationQueries:
    """Builds SQL queries for migration management."""

    migrations_table: str

    def build_create_table(self) -> str:
        """Build query to create migrations tracking table."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self.migrations_table} (
            version TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
        """

    def build_select_all(self) -> str:
        """Build query to select all migrations."""
        return (
            f"SELECT version, description, checksum, applied_at "
            f"FROM {self.migrations_table} ORDER BY version"
        )

    def build_select_versions(self) -> str:
        """Build query to select all migration versions."""
        return f"SELECT version FROM {self.migrations_table} ORDER BY version"

    def build_insert_migration(self) -> str:
        """Build query to insert a migration record."""
        return (
            f"INSERT INTO {self.migrations_table} "
            f"(version, description, checksum) VALUES ($1, $2, $3)"
        )


@dataclasses.dataclass
class MigrationManager:
    """Manages database migrations, tracking applied versions."""

    driver: db.Driver
    migrations_table: str | None = None

    def __post_init__(self) -> None:
        """Initialize query builder with proper table name (supports prefix)."""
        if self.migrations_table is None:
            self.migrations_table = qb.add_prefix("pgqueuer_migrations")
        self.queries = MigrationQueries(self.migrations_table)

    async def ensure_migrations_table(self) -> None:
        """Create the migrations tracking table if it doesn't exist."""
        await self.driver.execute(self.queries.build_create_table())

    async def get_all_migration_records(self) -> list[dict]:
        """Get all migration records from database."""
        try:
            return await self.driver.fetch(self.queries.build_select_all())
        except Exception:
            return []

    async def get_applied_migrations(self) -> set[str]:
        """Get set of applied migration versions."""
        records = await self.get_all_migration_records()
        return {record["version"] for record in records}

    async def record_migration(self, migration: Migration) -> None:
        """Record that a migration has been applied."""
        await self.driver.execute(
            self.queries.build_insert_migration(),
            migration.version,
            migration.description,
            migration.checksum(),
        )

    async def apply_migration(self, migration: Migration) -> None:
        """Apply a single migration and record it."""
        sql = migration.sql_generator()
        await self.driver.execute(sql)
        await self.record_migration(migration)

    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Run all pending migrations in order."""
        await self.ensure_migrations_table()
        applied = await self.get_applied_migrations()

        # Validate migrations are properly ordered
        for i, migration in enumerate(migrations):
            if i > 0 and migration.version <= migrations[i - 1].version:
                raise ValueError(
                    f"Migrations not properly ordered: {migration.version} "
                    f"should come after {migrations[i - 1].version}"
                )

        # Apply pending migrations
        applied_versions = []
        for migration in migrations:
            if migration.version not in applied:
                print(f"Applying migration {migration.version}: {migration.description}")
                await self.apply_migration(migration)
                applied_versions.append(migration.version)
                print(f"✓ Migration {migration.version} applied successfully")

        if not applied_versions:
            print("No pending migrations to apply")

        return applied_versions


def create_migrations_list() -> list[Migration]:
    """Create list of all PGQueuer migrations in order."""
    settings = qb.DBSettings()
    durability = settings.durability.config

    migrations = [
        Migration(
            version="001",
            description="Create pgqueuer_status enum type",
            sql_generator=lambda: f"""
                CREATE TYPE {settings.queue_status_type} AS ENUM (
                    'queued', 'picked', 'successful', 'exception',
                    'canceled', 'deleted'
                );
            """,
        ),
        Migration(
            version="002",
            description="Create pgqueuer queue table",
            sql_generator=lambda: f"""
                CREATE {durability.queue_table} TABLE {settings.queue_table} (
                    id SERIAL PRIMARY KEY,
                    priority INT NOT NULL,
                    queue_manager_id UUID,
                    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    execute_after TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    status {settings.queue_status_type} NOT NULL,
                    entrypoint TEXT NOT NULL,
                    dedupe_key TEXT,
                    payload BYTEA,
                    headers JSONB
                );
            """,
        ),
        Migration(
            version="003",
            description="Create index on priority for queued jobs",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table}_priority_id_id1_idx
                ON {settings.queue_table} (priority ASC, id DESC)
                INCLUDE (id)
                WHERE status = 'queued';
            """,
        ),
        Migration(
            version="004",
            description="Create index on updated for picked jobs",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table}_updated_id_id1_idx
                ON {settings.queue_table} (updated ASC, id DESC)
                INCLUDE (id)
                WHERE status = 'picked';
            """,
        ),
        Migration(
            version="005",
            description="Create index on queue_manager_id",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table}_queue_manager_id_idx
                ON {settings.queue_table} (queue_manager_id)
                WHERE queue_manager_id IS NOT NULL;
            """,
        ),
        Migration(
            version="006",
            description="Create unique index on dedupe_key",
            sql_generator=lambda: f"""
                CREATE UNIQUE INDEX {settings.queue_table}_unique_dedupe_key
                ON {settings.queue_table} (dedupe_key)
                WHERE (
                    (status IN ('queued', 'picked')
                    AND dedupe_key IS NOT NULL)
                );
            """,
        ),
        Migration(
            version="007",
            description="Create pgqueuer_log table",
            sql_generator=lambda: f"""
                CREATE {durability.queue_log_table} TABLE {settings.queue_table_log} (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    job_id BIGINT NOT NULL,
                    status {settings.queue_status_type} NOT NULL,
                    priority INT NOT NULL,
                    entrypoint TEXT NOT NULL,
                    traceback JSONB DEFAULT NULL,
                    aggregated BOOLEAN DEFAULT FALSE
                );
            """,
        ),
        Migration(
            version="008",
            description="Create index on pgqueuer_log for not aggregated",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table_log}_not_aggregated
                ON {settings.queue_table_log} ((1))
                WHERE not aggregated;
            """,
        ),
        Migration(
            version="009",
            description="Create index on pgqueuer_log created column",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table_log}_created
                ON {settings.queue_table_log} (created);
            """,
        ),
        Migration(
            version="010",
            description="Create index on pgqueuer_log status column",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table_log}_status
                ON {settings.queue_table_log} (status);
            """,
        ),
        Migration(
            version="011",
            description="Create index on pgqueuer_log job_id and created",
            sql_generator=lambda: f"""
                CREATE INDEX {settings.queue_table_log}_job_id_status
                ON {settings.queue_table_log} (job_id, created DESC);
            """,
        ),
        Migration(
            version="012",
            description="Create pgqueuer_statistics table",
            sql_generator=lambda: f"""
                CREATE {durability.statistics_table} TABLE {settings.statistics_table} (
                    id SERIAL PRIMARY KEY,
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT DATE_TRUNC('sec', NOW() at time zone 'UTC'),
                    count BIGINT NOT NULL,
                    priority INT NOT NULL,
                    status {settings.queue_status_type} NOT NULL,
                    entrypoint TEXT NOT NULL
                );
            """,
        ),
        Migration(
            version="013",
            description="Create unique index on pgqueuer_statistics",
            sql_generator=lambda: f"""
                CREATE UNIQUE INDEX {settings.statistics_table}_unique_count
                ON {settings.statistics_table} (
                    priority,
                    DATE_TRUNC('sec', created at time zone 'UTC'),
                    status,
                    entrypoint
                );
            """,
        ),
        Migration(
            version="014",
            description="Create pgqueuer_schedules table",
            sql_generator=lambda: f"""
                CREATE {durability.schedules_table} TABLE {settings.schedules_table} (
                    id SERIAL PRIMARY KEY,
                    expression TEXT NOT NULL,
                    entrypoint TEXT NOT NULL,
                    heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    last_run TIMESTAMP WITH TIME ZONE,
                    status {settings.queue_status_type} DEFAULT 'queued',
                    UNIQUE (expression, entrypoint)
                );
            """,
        ),
        Migration(
            version="015",
            description="Create notification trigger function",
            sql_generator=lambda: f"""CREATE FUNCTION {settings.function}() RETURNS TRIGGER AS $$
    DECLARE
        to_emit BOOLEAN := false;
    BEGIN
        IF TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW THEN
            to_emit := true;
        ELSIF TG_OP = 'DELETE' THEN
            to_emit := true;
        ELSIF TG_OP = 'INSERT' THEN
            to_emit := true;
        ELSIF TG_OP = 'TRUNCATE' THEN
            to_emit := true;
        END IF;

        IF to_emit THEN
            PERFORM pg_notify(
                '{settings.channel}',
                json_build_object(
                    'channel', '{settings.channel}',
                    'operation', lower(TG_OP),
                    'sent_at', NOW(),
                    'table', TG_TABLE_NAME,
                    'type', 'table_changed_event'
                )::text
            );
        END IF;

        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NULL;
        END IF;

    END;
    $$ LANGUAGE plpgsql;""",
        ),
        Migration(
            version="016",
            description="Create notification trigger on queue table",
            sql_generator=lambda: f"""
                CREATE TRIGGER {settings.trigger}
                AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
                ON {settings.queue_table}
                EXECUTE FUNCTION {settings.function}();
            """,
        ),
    ]

    return migrations
