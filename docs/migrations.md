# Migration Framework

PGQueuer uses a lightweight migration framework to manage database schema changes. Migrations are tracked in a dedicated table, ensuring each runs exactly once.

## Quick Start

### Fresh Installation

```bash
pgq install        # Creates all tables including migration tracking
pgq migrations     # View migration status (optional)
```

### Upgrading Existing Installation

```bash
# Backup first (recommended)
pg_dump your_database > backup.sql

# Upgrade PGQueuer
pip install --upgrade pgqueuer

# Apply migrations
pgq upgrade

# Verify (optional)
pgq migrations
```

### CLI Commands

```bash
pgq upgrade                # Apply pending migrations
pgq upgrade --dry-run      # Preview without applying
pgq migrations             # Show applied migrations
pgq migrations --all       # Show applied + pending
```

## How It Works

### Migration Tracking

Each migration is stored in `pgqueuer_migrations` table with:
- **version**: Unique identifier (e.g., "001", "002")
- **description**: What the migration does
- **checksum**: SHA-256 hash for integrity
- **applied_at**: Timestamp

### Upgrade Process

When you run `pgq upgrade`:

1. Checks for migration tracking table
2. For legacy databases (has tables, no tracking), seeds history automatically
3. Identifies pending migrations
4. Executes them in order
5. Records each successful migration

**Legacy Detection**: If upgrading from pre-migration-framework version, the system detects this and seeds all historical migrations as applied without re-running them.

## Adding Migrations

Create a new `Migration` in `pgqueuer/migrations.py`:

```python
Migration(
    version="024",
    description="Add new_column to queue table",
    sql_generator=lambda: f"ALTER TABLE {settings.queue_table} "
                          f"ADD COLUMN IF NOT EXISTS new_column TEXT;"
)
```

### Rules

1. **One SQL statement per migration** (enforced by validation)
2. **Sequential versions** - no gaps (001, 002, 003...)
3. **Never modify existing migrations** once released
4. **Use IF NOT EXISTS** for safety
5. **Test on a copy first**

### Examples

```python
# ✓ Valid - single statement
Migration(
    version="024",
    description="Add column",
    sql_generator=lambda: "ALTER TABLE queue ADD COLUMN col TEXT;"
)

# ✗ Invalid - multiple statements (raises ValueError)
Migration(
    version="024",
    description="Two changes",
    sql_generator=lambda: "ALTER TABLE queue ADD COLUMN a TEXT; ALTER TABLE queue ADD COLUMN b TEXT;"
)
```

## Troubleshooting

### Migration Fails

If a migration fails:
1. Check the error message
2. Failed migration is NOT marked as applied
3. Fix the issue
4. Run `pgq upgrade` again

### Check Migration Status

```bash
# View applied migrations
pgq migrations

# Or query directly
psql -c "SELECT * FROM pgqueuer_migrations ORDER BY version;"
```

### Force Reseed (Advanced)

Only if you manually modified schema:

```sql
-- WARNING: Only do this if you know what you're doing
DELETE FROM pgqueuer_migrations;
```

Then run `pgq upgrade` to reseed based on current schema.

## API Reference

### Migration Class

```python
@dataclasses.dataclass
class Migration:
    version: str
    description: str
    sql_generator: Callable[[], str]
    
    def checksum(self) -> str:
        """Calculate SHA-256 checksum."""
```

### MigrationManager Class

```python
@dataclasses.dataclass
class MigrationManager:
    driver: db.Driver
    migrations_table: str = "pgqueuer_migrations"
    
    async def ensure_migrations_table(self) -> None:
        """Create tracking table if needed."""
    
    async def get_applied_migrations(self) -> set[str]:
        """Get set of applied migration versions."""
    
    async def apply_migration(self, migration: Migration) -> None:
        """Execute and record a migration."""
    
    async def run_migrations(self, migrations: list[Migration]) -> list[str]:
        """Run pending migrations, returns applied versions."""
```

## See Also

- [Database Initialization](database_initialization.md)
- [CLI Reference](cli.rst)
- [Development Guide](development.md)
