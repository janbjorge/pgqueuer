## Rate Limiting

pgqueuer now includes a rate limiting feature, which allows users to control the frequency of job processing by specifying a maximum number of requests per second for each job type. 

### Implementation

The rate limiting in pgqueuer is implemented by track the number of jobs processed for each registered entrypoint within a specific timeframe. This is achieved using the `entrypoint` decorator, which now accepts an optional parameter `requests_per_second`. When set, this parameter specifies the maximum number of jobs that can be processed per second for that particular entry point.

Rate limiting in pgqueuer is enhanced by several key components.

**Internal Tracking with `statistics`**: Uses a sliding window mechanism to monitor and adjust job processing rates in real-time.

**Usage of PostgreSQL NOTIFY**: Essential for broadcasting job count updates and controlling rates. This feature is particularly important for syncing rate limits across multiple workers in a distributed environment, ensuring consistent enforcement across all instances.

### Setting Up Rate Limits

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
