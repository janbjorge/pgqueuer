"""
Lightweight migration framework for PGQueuer.

This module provides a simple, self-contained migration system that tracks
which schema changes have been applied to the database. It eliminates the need
to re-run all migrations on every upgrade by maintaining a migration history table.

Key features:
- Tracks applied migrations in a dedicated table
- Each migration has a unique version and checksum
- Migrations are executed in order
- Idempotent - safe to run multiple times
- No external dependencies
"""

from __future__ import annotations

import dataclasses
import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from . import db


@dataclasses.dataclass
class Migration:
    """
    Represents a single database migration.

    Attributes:
        version: Unique version number (e.g., "001", "002")
        description: Human-readable description of the migration
        sql_generator: Function that returns the SQL to execute
    """

    version: str
    description: str
    sql_generator: Callable[[], str]

    def checksum(self) -> str:
        """Calculate checksum of the migration SQL for integrity verification."""
        sql = self.sql_generator()
        return hashlib.sha256(sql.encode()).hexdigest()


@dataclasses.dataclass
class MigrationManager:
    """
    Manages database migrations for PGQueuer.

    Tracks which migrations have been applied and executes pending migrations
    in the correct order.

    Attributes:
        driver: Database driver for executing queries
        migrations_table: Name of the table tracking migration history
    """

    driver: db.Driver
    migrations_table: str = "pgqueuer_migrations"

    async def ensure_migrations_table(self) -> None:
        """Create the migrations tracking table if it doesn't exist."""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.migrations_table} (
            version VARCHAR(50) PRIMARY KEY,
            description TEXT NOT NULL,
            checksum VARCHAR(64) NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
        """
        await self.driver.execute(query)

    async def get_applied_migrations(self) -> set[str]:
        """Get the set of migration versions that have already been applied."""
        try:
            rows = await self.driver.fetch(
                f"SELECT version FROM {self.migrations_table} ORDER BY version"
            )
            return {row["version"] for row in rows}
        except Exception:
            # Table doesn't exist yet
            return set()

    async def record_migration(self, migration: Migration) -> None:
        """Record that a migration has been applied."""
        query = f"""
        INSERT INTO {self.migrations_table} (version, description, checksum)
        VALUES ($1, $2, $3)
        """
        await self.driver.execute(
            query,
            migration.version,
            migration.description,
            migration.checksum(),
        )

    async def apply_migration(self, migration: Migration) -> None:
        """
        Apply a single migration.

        Args:
            migration: The migration to apply

        Raises:
            Exception: If the migration fails to apply
        """
        sql = migration.sql_generator()
        # Execute the migration SQL
        # Note: Some migrations may contain multiple statements
        # For complex migrations, we split and execute them one by one
        for statement in self._split_sql_statements(sql):
            if statement.strip():
                await self.driver.execute(statement)

        # Record the migration
        await self.record_migration(migration)

    def _split_sql_statements(self, sql: str) -> list[str]:
        """
        Split a SQL script into individual statements.

        This is a simple implementation that splits on semicolons.
        For more complex SQL, this might need enhancement.
        """
        # Simple split - may need improvement for complex cases
        statements = []
        current = []

        for line in sql.split("\n"):
            stripped = line.strip()
            # Skip empty lines and comments
            if not stripped or stripped.startswith("--"):
                continue

            current.append(line)

            # Check if line ends with semicolon (simple heuristic)
            if stripped.endswith(";"):
                statements.append("\n".join(current))
                current = []

        # Add any remaining content
        if current:
            statements.append("\n".join(current))

        return statements

    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """
        Run all pending migrations.

        Args:
            migrations: List of all available migrations

        Returns:
            List of migration versions that were applied

        Raises:
            ValueError: If migrations are not properly ordered
        """
        # Ensure migrations table exists
        await self.ensure_migrations_table()

        # Get applied migrations
        applied = await self.get_applied_migrations()

        # Validate ordering
        for i, migration in enumerate(migrations):
            if i > 0 and migration.version <= migrations[i - 1].version:
                raise ValueError(
                    f"Migrations are not properly ordered: {migration.version} "
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
    """
    Create the list of all migrations for PGQueuer.

    This function contains all historical migrations extracted from the
    original upgrade logic, now as individual tracked migrations.

    Returns:
        List of Migration objects in order
    """
    from . import qb

    settings = qb.DBSettings()

    migrations = [
        # Migration 001: Add updated column
        Migration(
            version="001",
            description="Add updated column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        # Migration 002: Add updated index
        Migration(
            version="002",
            description="Add index on updated column for picked jobs",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_updated_id_id1_idx ON {settings.queue_table} (updated ASC, id DESC) INCLUDE (id) WHERE status = 'picked';",  # noqa: E501
        ),
        # Migration 003: Update trigger function
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
        # Migration 004: Add heartbeat column
        Migration(
            version="004",
            description="Add heartbeat column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        # Migration 005: Add heartbeat index
        Migration(
            version="005",
            description="Add index on heartbeat column (deprecated - not used)",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_heartbeat_id_id1_idx ON {settings.queue_table} (heartbeat ASC, id DESC) INCLUDE (id) WHERE status = 'picked';",  # noqa: E501
        ),
        # Migration 006: Add queue_manager_id column
        Migration(
            version="006",
            description="Add queue_manager_id column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS queue_manager_id UUID;",  # noqa: E501
        ),
        # Migration 007: Add queue_manager_id index
        Migration(
            version="007",
            description="Add index on queue_manager_id column",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table}_queue_manager_id_idx ON {settings.queue_table} (queue_manager_id) WHERE queue_manager_id IS NOT NULL;",  # noqa: E501
        ),
        # Migration 008: Create schedules table
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
        # Migration 009: Add execute_after column
        Migration(
            version="009",
            description="Add execute_after column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS execute_after TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();",  # noqa: E501
        ),
        # Migration 010: Add successful status to enum
        Migration(
            version="010",
            description="Add 'successful' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'successful';",  # noqa: E501
        ),
        # Migration 011: Add exception status to enum
        Migration(
            version="011",
            description="Add 'exception' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'exception';",  # noqa: E501
        ),
        # Migration 012: Add canceled status to enum
        Migration(
            version="012",
            description="Add 'canceled' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'canceled';",  # noqa: E501
        ),
        # Migration 013: Add deleted status to enum
        Migration(
            version="013",
            description="Add 'deleted' status to enum type",
            sql_generator=lambda: f"ALTER TYPE {settings.queue_status_type} ADD VALUE IF NOT EXISTS 'deleted';",  # noqa: E501
        ),
        # Migration 014: Update statistics table status column type
        Migration(
            version="014",
            description="Update statistics table status column to use queue_status_type",
            sql_generator=lambda: f"ALTER TABLE {settings.statistics_table} ALTER COLUMN status TYPE {settings.queue_status_type} USING status::TEXT::pgqueuer_status;",  # noqa: E501
        ),
        # Migration 015: Create queue log table
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
        # Migration 016: Add queue log not aggregated index
        Migration(
            version="016",
            description="Add index for non-aggregated queue log entries",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_not_aggregated ON {settings.queue_table_log} ((1)) WHERE not aggregated;",  # noqa: E501
        ),
        # Migration 017: Add queue log created index
        Migration(
            version="017",
            description="Add index on created column in queue log",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_created ON {settings.queue_table_log} (created);",  # noqa: E501
        ),
        # Migration 018: Add queue log status index
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
        # Migration 020: Add dedupe_key column
        Migration(
            version="020",
            description="Add dedupe_key column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS dedupe_key TEXT DEFAULT NULL;",  # noqa: E501
        ),
        # Migration 021: Add unique dedupe_key index
        Migration(
            version="021",
            description="Add unique index on dedupe_key for active jobs",
            sql_generator=lambda: f"CREATE UNIQUE INDEX IF NOT EXISTS {settings.queue_table}_unique_dedupe_key ON {settings.queue_table} (dedupe_key) WHERE ((status IN ('queued', 'picked') AND dedupe_key IS NOT NULL));",  # noqa: E501
        ),
        # Migration 022: Add queue log job_id status index
        Migration(
            version="022",
            description="Add composite index on job_id and created in queue log",
            sql_generator=lambda: f"CREATE INDEX IF NOT EXISTS {settings.queue_table_log}_job_id_status ON {settings.queue_table_log} (job_id, created DESC);",  # noqa: E501
        ),
        # Migration 023: Add headers column
        Migration(
            version="023",
            description="Add headers column to queue table",
            sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS headers JSONB;",  # noqa: E501
        ),
    ]

    return migrations
