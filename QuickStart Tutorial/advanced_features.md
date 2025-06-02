# Advanced Features of PgQueuer

PgQueuer offers several advanced features that enhance its functionality and allow for more complex job processing scenarios. This document outlines these features, including scheduled jobs, custom retry logic, and job dependencies.

## Scheduled Jobs

PgQueuer supports scheduling jobs to run at specific intervals or times, similar to cron jobs. This feature allows you to automate recurring tasks without manual intervention.

### Example: Scheduling a Job

You can define a scheduled job using a cron expression. For example, to run a job every hour:

```python
@pgq.schedule("hourly_report", "0 * * * *")  # Every hour
async def generate_hourly_report(schedule: Schedule) -> None:
    """Generate hourly analytics report."""
    from datetime import datetime, timedelta
    
    now = datetime.now()
    hour_ago = now - timedelta(hours=1)
    
    print(f"📊 Generating report for {hour_ago} to {now}")
    
    # Your report generation logic
    report_data = await generate_analytics_report(hour_ago, now)
    await save_report(report_data)
    
    print("✅ Hourly report generated")
```

## Custom Job Retry Logic

Implementing custom retry logic allows you to handle transient errors gracefully. You can specify how many times a job should be retried and the delay between retries.

### Example: Custom Retry Logic

```python
class RetryableJobHandler:
    """Base class for jobs with custom retry logic."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
    
    async def exponential_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        delay = self.base_delay * (2 ** attempt)
        return delay
    
    async def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if job should be retried based on exception type."""
        if attempt >= self.max_retries:
            return False
        return True

@pgq.entrypoint("resilient_api_call")
async def resilient_api_call(job: Job) -> None:
    """Job handler with built-in retry logic."""
    handler = RetryableJobHandler(max_retries=5)
    
    payload = json.loads(job.payload.decode())
    attempt = payload.get("attempt", 0)
    
    try:
        # Simulate API call
        print(f"🌐 Making API call (attempt {attempt + 1})")
        result = await make_external_api_call(payload["url"])
        print(f"✅ API call successful: {result}")
        
    except Exception as e:
        print(f"❌ API call failed (attempt {attempt + 1}): {e}")
        
        if await handler.should_retry(e, attempt):
            delay = await handler.exponential_backoff(attempt)
            await asyncio.sleep(delay)
            retry_payload = payload.copy()
            retry_payload["attempt"] = attempt + 1
            await queries.enqueue(["resilient_api_call"], [json.dumps(retry_payload).encode()], [job.priority])
        else:
            print("❌ Max retries reached or non-retryable error")
            raise
```

## Job Dependencies and Workflows

PgQueuer allows you to define job dependencies, enabling complex workflows where the execution of one job depends on the completion of others.

### Example: Defining a Workflow

```python
class WorkflowManager:
    """Manage job workflows and dependencies."""
    
    def __init__(self):
        self.workflows = {}
    
    def create_workflow(self, workflow_id: str, jobs: List[Dict]) -> None:
        """Create a workflow with job dependencies."""
        self.workflows[workflow_id] = {
            "jobs": jobs,
            "completed_jobs": set(),
            "failed_jobs": set()
        }
    
    async def start_workflow(self, workflow_id: str) -> None:
        """Start executing a workflow."""
        workflow = self.workflows[workflow_id]
        ready_jobs = self.get_ready_jobs(workflow_id)
        
        for job_id in ready_jobs:
            await self.execute_job(workflow_id, job_id)
    
    def get_ready_jobs(self, workflow_id: str) -> List[str]:
        """Get jobs that are ready to run (all dependencies completed)."""
        workflow = self.workflows[workflow_id]
        ready_jobs = []
        
        for job in workflow["jobs"]:
            job_id = job["id"]
            dependencies = job.get("depends_on", [])
            if dependencies.issubset(workflow["completed_jobs"]):
                ready_jobs.append(job_id)
        
        return ready_jobs
    
    async def execute_job(self, workflow_id: str, job_id: str) -> None:
        """Execute a single job in the workflow."""
        workflow = self.workflows[workflow_id]
        job_def = next(job for job in workflow["jobs"] if job["id"] == job_id)
        await queries.enqueue([job_def["entrypoint"]], [json.dumps(job_def["payload"]).encode()], [job_def.get("priority", 0)])
```

These advanced features provide powerful tools for managing complex job processing scenarios in PgQueuer, allowing for greater flexibility and control over job execution.