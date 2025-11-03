# Database Initialization

PGQueuer requires some initial setup in your PostgreSQL database. This includes creating tables and associated database objects necessary for job queuing and processing.

## Table Structure

PGQueuer uses three primary tables: one for job queues, one for logging job statistics, and another for managing schedule definitions. 


## Database installation/uninstallation

PGQueuer provides a command-line interface for easy management of installation and uninstallation. Ensure you have configured your [environment variables](https://magicstack.github.io/asyncpg/current/api/index.html#connection) or use the appropriate flags to specify your database credentials.

### Installing PGQueuer database components:
```bash
pgq install
```

### Uninstalling PGQueuer database components:
```bash
pgq uninstall
```

The CLI supports several flags to customize the connection settings. Use `--help` to see all available options.

### Manual installation of database components

You can find the commands needed for setting up your database for you version of PGQueuer by running:
```bash
pgq install --dry-run
```

## Adjusting Durability

PGQueuer tables are installed with **durable** settings by default. You can
select a different durability level during installation or upgrade using the
``--durability`` flag. The ``pgq durability`` command also allows
changing the durability of existing tables without data loss. Durability levels
range from ``volatile`` (unlogged tables for maximum speed) to ``durable``
(fully logged tables). Refer to :doc:`cli` for a full explanation of each
option.

## Autovacuum Optimization

After installation you can tune PostgreSQL's autovacuum settings for the queue
tables with ``pgq autovac``. This command applies recommended values aimed at
reducing bloat on the queue while keeping the log table mostly append-only. Use
``pgq autovac --rollback`` to reset these parameters back to their system
defaults. See :doc:`cli` for detailed command options.
