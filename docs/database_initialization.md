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
