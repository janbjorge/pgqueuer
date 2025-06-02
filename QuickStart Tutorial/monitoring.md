# Monitoring PgQueuer

Monitoring is an essential aspect of managing your PgQueuer jobs and ensuring that your system is running smoothly. This document outlines the various monitoring options available for PgQueuer, including real-time dashboards and command-line interface (CLI) commands for checking job status.

## Real-time Dashboard

PgQueuer provides a built-in dashboard that allows you to monitor your job queues in real-time. You can start the dashboard using the following command:

```bash
pgq dashboard
```

You can customize the dashboard with various options:

```bash
pgq dashboard --interval 5 --tail 20
```

This command will refresh the dashboard every 5 seconds and display the last 20 entries.

### Monitoring Specific Databases

To monitor a specific database, use the following command:

```bash
pgq dashboard --database-url postgresql://user:pass@localhost/mydb
```

## CLI Commands for Monitoring

PgQueuer includes several helpful CLI commands for monitoring job status and managing your queues:

- **Check Queue Status**: To check the current status of your queues, use:

```bash
pgq status
```

- **Purge Completed Jobs**: To clean up completed jobs older than a specified duration, use:

```bash
pgq purge --older-than "1 week"
```

- **View Job Statistics**: You can implement custom queries to get statistics about your jobs. This is a placeholder for demonstration:

```bash
pgq stats
```

## Best Practices for Monitoring

- Regularly check the dashboard to ensure that jobs are being processed as expected.
- Set up alerts for job failures or significant delays in processing.
- Use the CLI commands to automate monitoring tasks and integrate them into your operational workflows.

By effectively monitoring your PgQueuer jobs, you can maintain a healthy job processing environment and quickly address any issues that arise.