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

    migrations_table: str = "pgqueuer_migrations"

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
        return f"SELECT version, description, checksum, applied_at FROM {self.migrations_table} ORDER BY version"

    def build_select_versions(self) -> str:
        """Build query to select all migration versions."""
        return f"SELECT version FROM {self.migrations_table} ORDER BY version"

    def build_insert_migration(self) -> str:
        """Build query to insert a migration record."""
        return f"INSERT INTO {self.migrations_table} (version, description, checksum) VALUES ($1, $2, $3)"


@dataclasses.dataclass
class MigrationManager:
    """Manages database migrations, tracking applied versions."""

    driver: db.Driver
    migrations_table: str = "pgqueuer_migrations"

    def __post_init__(self) -> None:
        """Initialize query builder."""
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
        """Apply a single migration after validating it contains one SQL statement."""
        sql = migration.sql_generator()
        self._validate_single_statement(sql)
        await self.driver.execute(sql)
        await self.record_migration(migration)

    def _validate_single_statement(self, sql: str) -> None:
        """Validate SQL contains exactly one statement (enforces atomic migrations)."""
        statement_count = 0
        
        for line in sql.split("\n"):
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            statement_count += stripped.count(";")

        if statement_count == 0:
            raise ValueError(
                "Migration must contain exactly one SQL statement ending with semicolon. "
                f"Found 0 statements."
            )
        elif statement_count > 1:
            raise ValueError(
                f"Migration must contain exactly one SQL statement. "
                f"Found {statement_count} statements. "
                f"Split complex migrations into multiple separate migrations."
            )

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

    migrations = [
        Migration(
            version="001",
            description="Add updated column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        Migration(
            version="002",
            description="Add index on updated column for picked jobs",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_updated_id_id1_idx ON {settings.queue_table} (updated ASC, id DESC) INCLUDE (id) WHERE status = 'picked';",  # noqa: E501
        ),
        Migration(
            version="003",
            description="Update notification trigger function",
            sql_generator=lambda: f"""CREATE OR REPLACE FUNCTION {settings.function}() RETURNS TRIGGER AS $$
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

        -- Return appropriate value based on the operation
        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NULL; -- For TRUNCATE and other non-row-specific contexts
        END IF;

    END;
    $$ LANGUAGE plpgsql;""",
        ),
        Migration(
            version="004",
            description="Add heartbeat column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        Migration(
            version="005",
            description="Add index on heartbeat column (deprecated - not used)",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_heartbeat_id_id1_idx ON {settings.queue_table} (heartbeat ASC, id DESC) INCLUDE (id) WHERE status = 'picked';",  # noqa: E501
        ),
        Migration(
            version="006",
            description="Add queue_manager_id column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS queue_manager_id UUID;",  # noqa: E501
        ),
        Migration(
            version="007",
            description="Add index on queue_manager_id column",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_queue_manager_id_idx ON {settings.queue_table} (queue_manager_id) WHERE queue_manager_id IS NOT NULL;",  # noqa: E501
        ),
        Migration(
            version="008",
            description="Create schedules table",
            sql_generator=lambda: f"""CREATE TABLE IF NOT EXISTS {settings.schedules_table} (
        id SERIAL PRIMARY KEY,
        expression TEXT NOT NULL, -- Crontab-like schedule definition (e.g., '* * * * *')
        entrypoint TEXT NOT NULL,
        heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        next_run TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE,
        status {settings.queue_status_type} DEFAULT 'queued',
        UNIQUE (expression, entrypoint)
    );""",
        ),
        Migration(
            version="009",
            description="Add execute_after column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        Migration(
            version="010",
            description="Add 'successful' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'successful';",  # noqa: E501
        ),
        Migration(
            version="011",
            description="Add 'exception' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'exception';",  # noqa: E501
        ),
        Migration(
            version="012",
            description="Add 'canceled' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'canceled';",  # noqa: E501
        ),
        Migration(
            version="013",
            description="Add 'deleted' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'deleted';",  # noqa: E501
        ),
        Migration(
            version="014",
            description="Update statistics table status column to use queue_status_type",
            sql_generator=lambda: f"ALTER TABLE {settings.statistics_table} ALTER COLUMN status TYPE {settings.queue_status_type} USING status::TEXT::pgqueuer_status;",  # noqa: E501
        ),
        Migration(
            version="015",
            description="Create queue log table",
            sql_generator=lambda: f"""CREATE UNLOGGED TABLE IF NOT EXISTS {settings.queue_table_log} (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
        job_id BIGINT NOT NULL,
        status {settings.queue_status_type} NOT NULL,
        priority INT NOT NULL,
        entrypoint TEXT NOT NULL,
        aggregated BOOLEAN DEFAULT FALSE
    );""",
        ),
        Migration(
            version="016",
            description="Add index for non-aggregated queue log entries",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_not_aggregated ON {settings.queue_table_log} ((1)) WHERE not aggregated;",  # noqa: E501
        ),
        Migration(
            version="017",
            description="Add index on created column in queue log",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_created ON {settings.queue_table_log} (created);",  # noqa: E501
        ),
        Migration(
            version="018",
            description="Add index on status column in queue log",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_status ON {settings.queue_table_log} (status);",  # noqa: E501
        ),
        # Migration 019: Add traceback column to queue log
        Migration(
            version="019",
            description="Add traceback column to queue log table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table_log} ADD COLUMN IF NOT EXISTS traceback JSONB DEFAULT NULL;",  # noqa: E501
        ),
        Migration(
            version="020",
            description="Add dedupe_key column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS dedupe_key TEXT DEFAULT NULL;",  # noqa: E501
        ),
        Migration(
            version="021",
            description="Add unique index on dedupe_key for active jobs",
            sql_generator=lambda: f"CREATE UNIQUE INDEX IF NOT EXISTS {settings.queue_table}_unique_dedupe_key ON {settings.queue_table} (dedupe_key) WHERE ((status IN ('queued', 'picked') AND dedupe_key IS NOT NULL));",  # noqa: E501
        ),
        Migration(
            version="022",
            description="Add composite index on job_id and created in queue log",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_job_id_status ON {settings.queue_table_log} (job_id, created DESC);",  # noqa: E501
        ),
        Migration(
            version="023",
            description="Add headers column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS headers JSONB;",  # noqa: E501
        ),
    ]

    return migrations
