# Migration Framework

PGQueuer includes a lightweight migration framework to manage database schema changes over time. This framework ensures that schema updates are applied consistently and only once, making it easier to maintain and upgrade PGQueuer installations.

## Overview

The migration framework tracks which schema changes have been applied to your database using a dedicated tracking table (`pgqueuer_migrations`). Each migration has:

- **Version**: A unique identifier (e.g., "001", "002")
- **Description**: Human-readable explanation of what the migration does
- **Checksum**: SHA-256 hash of the migration SQL for integrity verification
- **Applied timestamp**: When the migration was executed

## How It Works

### Fresh Installation

When you run `pgq install` on a fresh database:

1. All core tables and objects are created immediately
2. The migration tracking table is created
3. No migrations are recorded (since everything was installed at once)

### Upgrading an Existing Database

When you run `pgq upgrade`:

1. The system checks for the migration tracking table
2. If it doesn't exist but schema objects do (legacy database), it seeds the migration history
3. Pending migrations are identified and executed in order
4. Each successful migration is recorded in the tracking table
5. Future upgrades only run new migrations

### Legacy Database Detection

The framework automatically detects databases that were upgraded using the old system (pre-migration framework). When detected:

- All historical migrations are marked as applied (seeded) without execution
- This ensures backward compatibility
- Future migrations will apply normally

## Migration Structure

Each migration is defined with:

```python
Migration(
    version="023",
    description="Add headers column to queue table",
    sql_generator=lambda: "ALTER TABLE pgqueuer ADD COLUMN IF NOT EXISTS headers JSONB;"
)
```

The `sql_generator` is a function that returns the SQL to execute, allowing for dynamic table names and settings.

## Adding New Migrations

To add a new migration:

1. Open `pgqueuer/migrations.py`
2. Add a new `Migration` object to the list in `create_migrations_list()`
3. Use the next sequential version number
4. Provide a clear description
5. Include the SQL in the `sql_generator` function

**Example:**

```python
Migration(
    version="024",
    description="Add new_column to queue table",
    sql_generator=lambda: f"ALTER TABLE {settings.queue_table} ADD COLUMN IF NOT EXISTS new_column TEXT;"
)
```

## Best Practices

### Migration SQL Guidelines

1. **Use IF NOT EXISTS**: Makes migrations safer if partially applied
   ```sql
   ALTER TABLE queue ADD COLUMN IF NOT EXISTS col TEXT;
   CREATE INDEX IF NOT EXISTS idx_name ON table (col);
   ```

2. **Keep migrations small**: Each migration should do one logical thing

3. **Add columns with defaults**: Ensures existing rows get values
   ```sql
   ALTER TABLE queue ADD COLUMN status TEXT DEFAULT 'pending';
   ```

4. **Test on a copy**: Always test migrations on a database copy first

5. **Document side effects**: If a migration affects data, document it clearly

### Version Numbering

- Use zero-padded sequential numbers: "001", "002", ..., "099", "100"
- Never reuse or skip version numbers
- Never modify existing migrations once released
- Keep migrations in order within the list

## Troubleshooting

### Migration Fails Midway

If a migration fails:

1. Check the error message for details
2. The failed migration is **not** marked as applied
3. Fix the issue (SQL syntax, permissions, etc.)
4. Run `pgq upgrade` again to retry

### Migration Already Applied Error

If you see "migration already applied":

- This is expected behavior (idempotency)
- The migration was successfully applied previously
- No action needed

### Checking Migration Status

To see which migrations have been applied:

```sql
SELECT version, description, applied_at 
FROM pgqueuer_migrations 
ORDER BY version;
```

### Force Reseed (Advanced)

If you need to reseed the migration history (e.g., after manual schema changes):

```sql
-- WARNING: Only do this if you know what you're doing
DELETE FROM pgqueuer_migrations;
```

Then run `pgq upgrade` to reseed based on current schema state.

## Migration History

The migration framework was introduced to replace the previous upgrade system which:

- Re-ran all migrations on every upgrade
- Relied entirely on idempotent SQL (IF NOT EXISTS)
- Had no tracking of what was applied
- Became increasingly difficult to maintain

The new system provides:

- ✅ Clear migration history
- ✅ Each migration runs exactly once
- ✅ Better error handling and debugging
- ✅ Easier to add new migrations
- ✅ Backward compatible with old installations
- ✅ No external dependencies

## API Reference

### Migration Class

```python
@dataclasses.dataclass
class Migration:
    version: str                          # Unique version identifier
    description: str                      # Human-readable description
    sql_generator: Callable[[], str]      # Function returning SQL to execute
    
    def checksum(self) -> str:
        """Calculate SHA-256 checksum of migration SQL"""
```

### MigrationManager Class

```python
@dataclasses.dataclass
class MigrationManager:
    driver: db.Driver                     # Database driver
    migrations_table: str = "pgqueuer_migrations"
    
    async def ensure_migrations_table(self) -> None:
        """Create tracking table if it doesn't exist"""
    
    async def get_applied_migrations(self) -> set[str]:
        """Get set of applied migration versions"""
    
    async def apply_migration(self, migration: Migration) -> None:
        """Execute and record a single migration"""
    
    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Run all pending migrations, returns list of applied versions"""
```

## Examples

### Checking Migration Status Programmatically

```python
from pgqueuer import migrations, queries, db
import asyncpg

# Connect to database
conn = await asyncpg.connect(dsn="postgresql://...")
driver = db.AsyncpgDriver(conn)

# Check applied migrations
manager = migrations.MigrationManager(driver)
applied = await manager.get_applied_migrations()
print(f"Applied migrations: {applied}")
```

### Running Migrations Programmatically

```python
from pgqueuer import queries
import asyncpg

# Connect and create queries instance
conn = await asyncpg.connect(dsn="postgresql://...")
q = queries.Queries.from_asyncpg_connection(conn)

# Run upgrade
await q.upgrade()
```

## See Also

- [Database Initialization](database_initialization.md) - Initial setup guide
- [CLI Reference](cli.rst) - Command-line interface documentation
- [Development Guide](development.md) - Contributing to PGQueuer
