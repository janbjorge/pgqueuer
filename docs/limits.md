## Throttling Job Processing

By default, there is no limitation on how frequently jobs can be dequeued from the database or the number of concurrent jobs that can be processed by each consumer. `PGQueuer` provides features that allow limitations to be applied to individual endpoints as needed by the user.

### Rate Limiting

Users can control the frequency of job processing by specifying a maximum number of requests per second for each job type. This type of control can be appropriate when jobs access a resource such as an API endpoint which imposes its own rate limits.

#### Setting Up Rate Limits

When defining job processing functions, you can specify rate limits directly in the `entrypoint` decorator. Below is an example of how to set up rate limits for different job types.

```python
from pgqueuer.qm import QueueManager
from pgqueuer.models import Job

# Assuming `qm` is an instance of QueueManager

@qm.entrypoint("data_processing", requests_per_second=10)
def process_data(job: Job):
    # Implementation for data processing jobs
    pass

@qm.entrypoint("image_processing", requests_per_second=5)
def process_image(job: Job):
    # Implementation for image processing jobs
    pass
```

#### Implementation

The rate limiting in `PGQueuer` is implemented by track the number of jobs processed for each registered entrypoint within a specific timeframe. This is achieved using the `entrypoint` decorator, which now accepts an optional parameter `requests_per_second`. When set, this parameter specifies the maximum number of jobs that can be processed per second for that particular entry point.

Rate limiting in PGQueuer is enhanced by several key components.

**Internal Tracking with `statistics`**: Uses a sliding window mechanism to monitor and adjust job processing rates in real-time.

**Usage of PostgreSQL NOTIFY**: Essential for broadcasting job count updates and controlling rates. This feature is particularly important for syncing rate limits across multiple workers in a distributed environment, ensuring consistent enforcement across all instances.

### Concurrency Limiting

Users can control the number of concurrent jobs of each type which can run on each consumer. This type of control can be appropriate when jobs use resources local to the consumer, such as compute- or filesystem-bound tasks.

#### Setting up Concurrency Limits

When defining job processing functions, you can specify concurrency limits directly in the `entrypoint` decorator via the `concurrency_limit` argument. The following example shows how to limit the number of concurrent jobs based on the CPU count of the system.

```python
import multiprocessing
from pgqueuer.qm import QueueManager
from pgqueuer.models import Job

# Assuming `qm` is an instance of QueueManager

@qm.entrypoint("data_processing", concurrency_limit=multiprocessing.cpu_count())
async def process_data(job: Job):
    # Implementation for data processing jobs
    pass
```

#### Implementation

When `concurrency_limit` is specified, a per-entrypoint semaphore is used to limit the number of concurrent calls to the user's entrypoint function. Additionally, while the semaphore is fully utilized, jobs for the endpoint will not be dequeued. This provides back-pressure to avoid consumers dequeuing more jobs than they can handle. Note that this means that up to `batch_size - 1` jobs may be dequeued by the consumer and waiting for the semaphore at any one time.

### Serialized Dispatch
Serialized dispatch is a new feature in PGQueuer designed to improve control over how job execution is managed in terms of concurrency. By using the `serialized_dispatch` flag, you can control whether jobs of the same type should be processed strictly in sequence or can be executed concurrently.

This feature is especially useful when you have jobs that modify shared resources and require strict serialization to avoid race conditions.

#### How to Use Serialized Dispatch

You can enable serialized dispatch by passing the `serialized_dispatch` parameter when defining a job entrypoint using `@QueueManager.entrypoint()`.

Example:

```python
from datetime import timedelta
from pgqueuer.qm import QueueManager

qm = QueueManager(...)

@qm.entrypoint(
    "process_shared_resource",
    serialized_dispatch=True,  # Enforces strict serialization for this entrypoint
)
async def process_shared_resource(job):
    # Job processing logic
    ...
```

In this example, jobs registered under the entrypoint `process_shared_resource` will be processed strictly one at a time, ensuring no other jobs of the same type are processed concurrently.

#### Implementation

Serialized dispatch is implemented through several key mechanisms to ensure exclusive processing of jobs:

- **Serialized Dispatch Parameter**: The `serialized_dispatch` parameter is a boolean flag that determines if jobs for a given entrypoint should be strictly serialized (`True`) or can be processed concurrently (`False`).

- **Job Query Changes**: The job selection logic in PGQueuer has been updated to incorporate serialized dispatch by enforcing that, if the `serialized_dispatch` flag is set to `True`, jobs will only be picked if no other jobs are currently marked as "in-progress" (`picked`) for the same entrypoint. This ensures that jobs are processed one at a time.

Serialized dispatch is suitable for jobs that:
  - Access shared resources and need to ensure no concurrent modifications occur.
  - Require consistent order of execution to maintain correctness.
