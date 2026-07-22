# Database Setup

PgQueuer requires initial setup in your PostgreSQL database: tables, triggers, and functions
for job queuing and processing.

## Table Structure

PgQueuer uses four primary tables:

- **`pgqueuer`**: the active job queue
- **`pgqueuer_log`**: job execution history and audit trail
- **`pgqueuer_statistics`**: aggregated statistics
- **`pgqueuer_schedules`**: recurring schedule definitions

## Installation

Install PgQueuer schema via the CLI. Make sure your PostgreSQL environment variables
(`PGHOST`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`) are set, or pass `--pg-dsn` / set
`PGDSN` with a full connection string:

```bash
pgq install
```

To see what SQL will be executed without applying it, use the offline
[`pgq sql`](cli.md#sql) group. It never connects to a database, so you can pipe
the script to psql or hand it to your migration tool (Flyway, sqitch, Alembic)
or a DBA with restricted permissions (see
[Database Permissions](database-permissions.md)):

```bash
pgq sql install
pgq sql install | psql -v ON_ERROR_STOP=1
pgq sql upgrade > migrations/V2__pgqueuer_upgrade.sql
```

## Uninstallation

```bash
pgq uninstall
```

## Upgrades

Apply schema migrations after upgrading PgQueuer:

```bash
pgq upgrade
```

## Verification

Check that all required objects exist:

```bash
pgq verify --expect present
```

Check that the schema has been removed:

```bash
pgq verify --expect absent
```

The command exits with code `1` if any mismatches are detected.

## Adjusting Durability

PgQueuer tables are installed with **durable** settings by default. You can select a
different durability level at install time:

```bash
pgq install --durability balanced
```

Or change the durability of existing tables without data loss:

```bash
pgq durability volatile
```

See [CLI Reference](cli.md) for full details on `volatile`, `balanced`, and `durable` modes.

## Autovacuum Optimization

After installation, tune PostgreSQL autovacuum settings for PgQueuer tables:

```bash
pgq autovac
```

This applies recommended values that reduce bloat on the queue while keeping the log table
mostly append-only. Reset to system defaults with:

```bash
pgq autovac --rollback
```
