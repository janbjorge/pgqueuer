# Migration Guide: Upgrading to the New Migration Framework

This guide helps existing PGQueuer users understand and transition to the new migration framework introduced in version 0.38.0.

## What Changed?

PGQueuer has moved from a brittle "re-run all migrations" approach to a robust migration tracking system. This change:

- **Improves reliability**: Migrations are only run once, tracked in a dedicated table
- **Easier maintenance**: Adding new migrations is simpler and less error-prone
- **Better visibility**: CLI commands show migration status
- **Backward compatible**: Automatically detects and handles legacy databases

## Do I Need to Do Anything?

**Short answer: No!** The transition is automatic.

When you run `pgq upgrade` on an existing database:

1. The system detects it's a legacy database (has tables but no migration history)
2. Automatically seeds the migration history with all previous migrations marked as applied
3. Only new migrations (if any) will be applied
4. Future upgrades work normally with the new system

## Recommended Upgrade Steps

For existing PGQueuer installations:

```bash
# 1. Backup your database (always a good practice!)
pg_dump your_database > backup.sql

# 2. Upgrade PGQueuer to the latest version
pip install --upgrade pgqueuer

# 3. Run the upgrade command
pgq upgrade

# 4. (Optional) Verify migration status
pgq migrations
```

### What You'll See

On first upgrade after updating PGQueuer:

```
Detected legacy database - seeding migration history...
✓ Seeded 23 migrations in history table
No pending migrations to apply
```

This is expected and means your database has been successfully transitioned to the new system!

## New CLI Commands

### Check Migration Status

View which migrations have been applied:

```bash
pgq migrations
```

View all migrations (applied and pending):

```bash
pgq migrations --all
```

### Preview Upgrades

See what migrations will be applied without running them:

```bash
pgq upgrade --dry-run
```

## For Fresh Installations

If you're setting up PGQueuer for the first time:

```bash
# Install the schema
pgq install

# The migration table is created automatically
# No migrations are recorded since everything was installed at once

# Future upgrades will use the migration tracking system
pgq upgrade
```

## Troubleshooting

### "Migration already applied" message

This is normal! It means the migration was successfully applied previously. The new system prevents re-running migrations.

### Want to check what's in the migration table?

```sql
SELECT version, description, applied_at 
FROM pgqueuer_migrations 
ORDER BY version;
```

### Something went wrong?

1. Check the error message - it will indicate which migration failed
2. The failed migration is NOT marked as applied
3. Fix the underlying issue (permissions, SQL syntax, etc.)
4. Run `pgq upgrade` again to retry

## Technical Details

### Migration Tracking Table

The new `pgqueuer_migrations` table stores:

```sql
CREATE TABLE pgqueuer_migrations (
    version VARCHAR(50) PRIMARY KEY,
    description TEXT NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

### Backward Compatibility

The system detects legacy databases by checking for:
- Absence of `pgqueuer_migrations` table
- Presence of `pgqueuer_log` table (indicates previous upgrades)

When both conditions are true, all historical migrations are automatically seeded as applied.

### Migration Checksums

Each migration has a SHA-256 checksum of its SQL. This ensures integrity and helps detect if migration files have been modified.

## Benefits of the New System

| Old System | New System |
|------------|------------|
| Re-ran all migrations every time | Each migration runs once |
| No tracking | Full migration history |
| Hard to debug failures | Clear error messages |
| Difficult to add migrations | Simple extension |
| No visibility | CLI status commands |
| Assumed idempotency | Enforced idempotency |

## Need Help?

- 📚 [Full Migration Documentation](migrations.md)
- 💬 [Discord Community](https://discord.gg/C7YMBzcRMQ)
- 🐛 [Report Issues](https://github.com/janbjorge/pgqueuer/issues)

## For Developers

If you're developing PGQueuer or adding custom migrations:

1. Read the [full migration documentation](migrations.md)
2. Follow the migration best practices
3. Test migrations on a database copy first
4. Use version numbers in sequence
5. Never modify existing migrations

See `pgqueuer/migrations.py` for examples of well-structured migrations.
