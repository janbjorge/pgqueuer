"""Tests for the migration framework."""

from __future__ import annotations

import pytest

from pgqueuer import db, migrations, queries


async def test_migration_manager_creates_table(apgdriver: db.Driver) -> None:
    """Test that the migration manager creates the tracking table."""
    manager = migrations.MigrationManager(apgdriver)

    # Ensure table doesn't exist yet
    await apgdriver.execute("DROP TABLE IF EXISTS pgqueuer_migrations")

    # Create the table
    await manager.ensure_migrations_table()

    # Verify table exists by querying it
    result = await apgdriver.fetch("SELECT COUNT(*) as count FROM pgqueuer_migrations")
    assert result[0]["count"] == 0


async def test_migration_tracking(apgdriver: db.Driver) -> None:
    """Test that migrations are tracked correctly."""
    manager = migrations.MigrationManager(apgdriver)
    await manager.ensure_migrations_table()

    # Clean slate
    await apgdriver.execute("DELETE FROM pgqueuer_migrations")

    # Create a test migration
    test_migration = migrations.Migration(
        version="test_001",
        description="Test migration",
        sql_generator=lambda: "SELECT 1;",
    )

    # Apply the migration
    await manager.apply_migration(test_migration)

    # Verify it was recorded
    applied = await manager.get_applied_migrations()
    assert "test_001" in applied

    # Verify the record has correct data
    result = await apgdriver.fetch(
        "SELECT * FROM pgqueuer_migrations WHERE version = $1", "test_001"
    )
    assert len(result) == 1
    assert result[0]["description"] == "Test migration"
    assert result[0]["checksum"] == test_migration.checksum()


async def test_migration_ordering_validation(apgdriver: db.Driver) -> None:
    """Test that migrations must be in order."""
    manager = migrations.MigrationManager(apgdriver)
    await manager.ensure_migrations_table()

    # Clean slate
    await apgdriver.execute("DELETE FROM pgqueuer_migrations")

    # Create migrations with incorrect ordering
    migrations_list = [
        migrations.Migration(
            version="002",
            description="Second",
            sql_generator=lambda: "SELECT 1;",
        ),
        migrations.Migration(
            version="001",
            description="First",
            sql_generator=lambda: "SELECT 1;",
        ),
    ]

    # Should raise an error
    with pytest.raises(ValueError, match="not properly ordered"):
        await manager.run_migrations(migrations_list)


async def test_migration_idempotency(apgdriver: db.Driver) -> None:
    """Test that running migrations multiple times doesn't reapply them."""
    manager = migrations.MigrationManager(apgdriver)
    await manager.ensure_migrations_table()

    # Clean slate
    await apgdriver.execute("DELETE FROM pgqueuer_migrations")

    # Create a test migration
    test_migration = migrations.Migration(
        version="test_002",
        description="Test idempotency",
        sql_generator=lambda: "CREATE TEMPORARY TABLE IF NOT EXISTS test_temp (id INT);",
    )

    # Apply once
    applied = await manager.run_migrations([test_migration])
    assert len(applied) == 1
    assert "test_002" in applied

    # Apply again - should not reapply
    applied = await manager.run_migrations([test_migration])
    assert len(applied) == 0


async def test_migration_checksum_calculation() -> None:
    """Test that migration checksums are calculated correctly."""
    migration1 = migrations.Migration(
        version="001",
        description="Test",
        sql_generator=lambda: "SELECT 1;",
    )

    migration2 = migrations.Migration(
        version="001",
        description="Test",
        sql_generator=lambda: "SELECT 2;",
    )

    # Different SQL should produce different checksums
    assert migration1.checksum() != migration2.checksum()

    # Same SQL should produce same checksum
    migration3 = migrations.Migration(
        version="002",
        description="Different version",
        sql_generator=lambda: "SELECT 1;",
    )
    assert migration1.checksum() == migration3.checksum()


async def test_upgrade_with_fresh_database(apgdriver: db.Driver) -> None:
    """Test upgrade on a fresh database (all migrations should run)."""
    q = queries.Queries(apgdriver)

    # Drop the migrations table if it exists
    await apgdriver.execute("DROP TABLE IF EXISTS pgqueuer_migrations")

    # Run upgrade
    await q.upgrade()

    # Check that migrations were recorded
    result = await apgdriver.fetch("SELECT COUNT(*) as count FROM pgqueuer_migrations")
    assert result[0]["count"] > 0


async def test_upgrade_with_legacy_database(apgdriver: db.Driver) -> None:
    """Test upgrade on a database that was upgraded with the old system."""
    q = queries.Queries(apgdriver)

    # Simulate a legacy database by dropping the migrations table
    # but keeping the actual schema
    await apgdriver.execute("DROP TABLE IF EXISTS pgqueuer_migrations")

    # The database should already have the queue_table_log from conftest
    # So the seed function should detect it as legacy

    # Run upgrade
    await q.upgrade()

    # Check that all migrations were seeded
    result = await apgdriver.fetch("SELECT COUNT(*) as count FROM pgqueuer_migrations")
    migration_count = result[0]["count"]
    assert migration_count > 0

    # Verify we can run upgrade again without errors
    await q.upgrade()

    # Count should remain the same (no new migrations applied)
    result = await apgdriver.fetch("SELECT COUNT(*) as count FROM pgqueuer_migrations")
    assert result[0]["count"] == migration_count


async def test_create_migrations_list() -> None:
    """Test that the migration list is properly structured."""
    migration_list = migrations.create_migrations_list()

    # Should have migrations
    assert len(migration_list) > 0

    # All migrations should have unique versions
    versions = [m.version for m in migration_list]
    assert len(versions) == len(set(versions))

    # Migrations should be in order
    for i in range(len(migration_list) - 1):
        assert migration_list[i].version < migration_list[i + 1].version

    # Each migration should have required fields
    for migration in migration_list:
        assert migration.version
        assert migration.description
        assert callable(migration.sql_generator)
        # Should be able to generate SQL without errors
        sql = migration.sql_generator()
        assert isinstance(sql, str)
        assert len(sql) > 0


async def test_migration_versions_sequential() -> None:
    """Test that migration versions are sequential with no gaps."""
    migration_list = migrations.create_migrations_list()

    versions = [int(m.version) for m in migration_list]
    
    # Check that versions start at 1 and are sequential
    assert versions[0] == 1, "First migration should be version 001"
    
    for i, version in enumerate(versions, start=1):
        assert version == i, f"Migration versions must be sequential. Expected {i}, got {version}"
