# PgQueuer Consumer Documentation

## Overview

The PgQueuer consumer is responsible for processing jobs that have been enqueued by producers. This document outlines how to set up and run a consumer for PgQueuer, detailing the job processing workflow and configuration options.

## Setting Up the Consumer

To set up the PgQueuer consumer, ensure that you have the following prerequisites:

- A running PostgreSQL database with the PgQueuer schema installed.
- The PgQueuer library installed in your Python environment.

### Configuration

The consumer can be configured using environment variables or a configuration file. Key configuration options include:

- **DATABASE_URL**: The connection string for your PostgreSQL database.
- **MAX_CONCURRENT_JOBS**: The maximum number of jobs to process concurrently.
- **JOB_TIMEOUT**: The maximum time allowed for a job to run before it is considered failed.

### Running the Consumer

To run the PgQueuer consumer, use the following command:

```bash
pgq run consumer:main
```

This command starts the consumer, which will begin processing jobs from the queue.

## Job Processing Workflow

1. **Job Retrieval**: The consumer retrieves jobs from the PostgreSQL database that are ready to be processed.
2. **Job Execution**: Each job is executed according to its defined entry point. The consumer will handle any exceptions that occur during job execution.
3. **Job Completion**: Upon successful completion, the job is marked as completed in the database. If a job fails, it can be retried based on the configured retry logic.

## Monitoring the Consumer

You can monitor the status of the consumer and the jobs being processed using the PgQueuer dashboard or CLI commands. This allows you to track job progress and identify any issues that may arise during processing.

## Best Practices

- Ensure that your job handlers are idempotent to avoid issues with job retries.
- Use appropriate logging to capture job execution details and errors.
- Regularly monitor the consumer's performance and adjust configuration settings as needed to optimize throughput.

By following these guidelines, you can effectively set up and run a PgQueuer consumer to handle your background job processing needs.