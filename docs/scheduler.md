# Scheduler

PGQueuer includes a scheduling feature that allows you to register and run recurring jobs using cron-like expressions. This feature is ideal for scheduling tasks that need to run at fixed intervals, such as data synchronization, monitoring, or regular cleanup activities.

### Overview
The Scheduler class allows users to register asynchronous tasks with cron expressions for recurring execution. It leverages PostgreSQL for storing and managing schedules, ensuring robustness and visibility into the state of each scheduled job.

### How It Works
The `SchedulerManager` class is responsible for managing jobs, tracking their schedules, and dispatching them for execution. Jobs are defined using cron-like expressions and can be registered through the `@schedule` decorator. Once registered, the Scheduler ensures that each job runs at the specified intervals. It also tracks metadata, including the next scheduled run time and the status of the job.

The main components of the scheduling feature are:

1. **Scheduler Registration**: Use the `@schedule` decorator to register jobs with a specific cron expression. Each job must have a unique combination of entrypoint and cron expression.

   Example:
   ```python
   @sc.schedule("fetch_db", "* * * * *")
   async def fetch_db(schedule: Schedule) -> None:
       # Fetch data from the database
       await asyncio.sleep(0.1)
   ```

   In the above example, the `fetch_db` function is scheduled to run every minute. Multiple schedules for the same entrypoint are allowed, enabling varied timing configurations.

2. **Executor Model**: The Scheduler utilizes an `AbstractScheduleExecutor` class, which defines how each scheduled job should be executed. The provided `ScheduleExecutor` is a concrete implementation that runs the asynchronous function associated with each schedule. Executors maintain job state, calculate the next run time, and track heartbeats for monitoring.

3. **Database Integration**: The schedules are persisted in PostgreSQL to maintain reliability and state recovery across application restarts. The new schema includes a `schedules` table that keeps track of each job, its next run, last run, status, and the cron expression.

4. **Heartbeat and Monitoring**: The Scheduler maintains a `heartbeat` for each scheduled job, which is periodically updated to indicate job activity. This mechanism allows the Scheduler to detect stalled jobs and retry them as needed.

### Example Usage
To use the scheduling feature, you first need to create a `Scheduler` instance and connect it to your PostgreSQL database via an `AsyncpgDriver`.

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.sc import SchedulerManager

async def main() -> Scheduler:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    sc = SchedulerManager(driver)

    @sc.schedule("fetch_db", "* * * * *")
    async def fetch_db(schedule: Schedule) -> None:
        # Logic for the scheduled task
        await asyncio.sleep(0.1)

    return sc
```

After defining the schedules, the `Scheduler` can be started by calling its `run()` method. It will continuously poll for jobs that need to be executed and dispatch them accordingly.

### Integration with CLI
The CLI allows users to run either a `QueueManager` or a `SchedulerManager` instance. This means you can easily switch between queue and schedule modes by specifying the appropriate factory function in the CLI.

To run the `SchedulerManager` class using the CLI, you can use the `run` command and provide the path to your factory function that creates the `SchedulerManager` instance. For example:

```sh
pgq run myapp.create_scheduler
```

In the above command, `myapp.create_scheduler` should be the import path to your function that initializes and returns a `Scheduler` instance. This allows you to seamlessly start the Scheduler from the command line and manage your scheduled jobs.

The scheduling feature is a powerful addition to PGQueuer, providing users with a robust way to manage recurring jobs alongside traditional queue-based tasks.
