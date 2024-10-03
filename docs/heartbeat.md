### Automatic Heartbeat Logic

The Automatic Heartbeat system monitors the liveness of each active job by periodically updating a heartbeat timestamp in the PostgreSQL database. This ongoing update serves as an indicator that a job is actively being processed. If the heartbeat isn't updated within a specified interval, PGQueuer identifies the job as stalled and can take corrective actions, such as retrying the job or notifying administrators.

#### How It Works

1. **Heartbeat Management**:

   - **Activation**: When a job is dispatched for processing, PGQueuer activates a heartbeat mechanism specific to that job. This ensures that the system is aware the job is actively being worked on.

   - **Periodic Updates**: The heartbeat mechanism periodically updates a `heartbeat` timestamp associated with the job in the `pgqueuer` queue table. These updates are buffered and sent to the database in batches to optimize performance and reduce the number of write operations.

2. **Stall Detection**:

   - **Monitoring**: PGQueuer regularly checks the `heartbeat` timestamps of all active jobs. This monitoring ensures that each job's processing status is up-to-date.

   - **Identifying Stalls**: If a job's heartbeat hasn't been updated within the configured interval, the system marks the job as stalled. This indicates that the job may have encountered an issue during processing, such as a crash or network failure.

   - **Intervention**: Upon detecting a stalled job, PGQueuer can automatically re-queue the job for retrying, ensuring that it doesn't remain unprocessed. Additionally, administrators can be alerted to investigate persistent issues.

3. **Concurrency Control**:

   - The heartbeat system works in tandem with concurrency limiters to manage the number of jobs being processed simultaneously. This coordination ensures efficient utilization of system resources without overloading the system.

#### Configuration

- **Heartbeat Interval**:
  - **Definition**: Determines how frequently heartbeats are sent for each active job
  - **Typical Setting**: Configured to ensure timely detection of stalled jobs without causing excessive database load.

#### Benefits

- **Fault Detection**:
  - **Early Identification**: Quickly detects jobs that have stalled or failed unexpectedly, enabling prompt retries or alerts.

- **Resource Management**:
  - **Efficiency**: Prevents system resources from being tied up by unresponsive jobs, ensuring the system remains responsive and efficient.

- **Scalability**:
  - **High-Load Support**: Maintains reliability in job processing even in high-load environments by ensuring that stalled jobs are promptly handled.

- **Operational Insight**:
  - **Monitoring**: Provides valuable metrics for assessing job processing health and performance, facilitating proactive maintenance and optimization.
