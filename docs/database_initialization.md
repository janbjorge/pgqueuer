# Database Initialization

PgQueuer requires some initial setup in your PostgreSQL database. This includes creating tables and associated database objects necessary for job queuing and processing.

## Table Structure

PgQueuer uses two primary tables: one for job queues and another for logging job statistics. Below is the structure of these tables along with explanations for each column.

### Queue Table

The queue table stores all the jobs that are to be processed.

```sql
CREATE TABLE pgqueuer (
    id SERIAL PRIMARY KEY,               -- Unique identifier for each job.
    priority INT NOT NULL,               -- Priority of the job, higher value means higher priority.
    created TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, -- Timestamp when the job was created.
    updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, -- Timestamp when the job was last updated.
    status pgqueuer_status NOT NULL,     -- Status of the job (queued, picked).
    entrypoint TEXT NOT NULL,            -- The entrypoint function that will process the job.
    payload BYTEA                        -- The job's data payload.
);
```

### Statistics Table

The statistics table logs information about processed jobs.

```sql
CREATE TABLE pgqueuer_statistics (
    id SERIAL PRIMARY KEY,               -- Unique identifier for each log entry.
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT DATE_TRUNC('sec', NOW() at time zone 'UTC'), -- Timestamp when the log entry was created.
    count BIGINT NOT NULL,               -- Number of jobs processed.
    priority INT NOT NULL,               -- Priority of the jobs being logged.
    time_in_queue INTERVAL NOT NULL,     -- Time the job spent in the queue.
    status pgqueuer_statistics_status NOT NULL, -- Status of the job processing (exception, successful).
    entrypoint TEXT NOT NULL             -- The entrypoint function that processed the job.
);
```

## Database installation/uninstallation

PgQueuer provides a command-line interface for easy management of installation and uninstallation. Ensure you have configured your [environment variables](https://magicstack.github.io/asyncpg/current/api/index.html#connection) or use the appropriate flags to specify your database credentials.

### Installing PgQueuer database components:
```bash
python -m PgQueuer install 
```

### Uninstalling PgQueuer database components:
```bash
python -m PgQueuer uninstall 
```

The CLI supports several flags to customize the connection settings. Use `--help` to see all available options.
