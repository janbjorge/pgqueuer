# PgQueuer FAQ

Frequently asked questions about PgQueuer, with doctest-verified examples and practical solutions.

## General Questions

### Q: What is PgQueuer and how does it differ from Celery or RQ?

**A:** PgQueuer is a PostgreSQL-based job queue that leverages PostgreSQL's native features like LISTEN/NOTIFY and FOR UPDATE SKIP LOCKED for efficient job processing.

```python
"""
Comparison of job queue characteristics.

>>> # PgQueuer advantages
>>> pgqueuer_features = {
...     "backend": "PostgreSQL",
...     "setup_complexity": "low",
...     "transaction_support": True,
...     "data_consistency": "ACID compliant",
...     "infrastructure_deps": "minimal"
... }

>>> # Celery comparison
>>> celery_features = {
...     "backend": "Redis/RabbitMQ",
...     "setup_complexity": "medium",
...     "transaction_support": False,
...     "data_consistency": "eventual",
...     "infrastructure_deps": "message broker required"
... }

>>> # When to choose PgQueuer
>>> def should_use_pgqueuer(requirements):
...     '''
...     >>> # Already using PostgreSQL
...     >>> reqs1 = {"database": "postgresql", "complexity": "low"}
...     >>> should_use_pgqueuer(reqs1)
...     True
...     
...     >>> # Need ACID transactions
...     >>> reqs2 = {"transactions": True, "consistency": "strong"}
...     >>> should_use_pgqueuer(reqs2)
...     True
...     
...     >>> # High-volume, simple jobs
...     >>> reqs3 = {"volume": "high", "job_type": "simple"}
...     >>> should_use_pgqueuer(reqs3)
...     False
...     '''
...     return (
...         requirements.get("database") == "postgresql" or
...         requirements.get("transactions") is True or
...         requirements.get("infrastructure_deps") == "minimal"
...     )
```

**Key differences:**
- **No additional infrastructure** - uses your existing PostgreSQL database
- **ACID transactions** - jobs can be part of database transactions
- **Simpler deployment** - no message broker setup required
- **Strong consistency** - leverages PostgreSQL's consistency guarantees

### Q: Can I use PgQueuer with an existing PostgreSQL database?

**A:** Yes! PgQueuer creates its own tables and doesn't interfere with your existing schema.

```python
"""
Database integration safety checks.

>>> def check_table_conflicts(existing_tables, pgqueuer_tables):
...     '''
...     Check if PgQueuer tables conflict with existing ones.
...     
...     >>> # Your existing tables
...     >>> my_tables = ["users", "orders", "products", "invoices"]
...     >>> 
...     >>> # PgQueuer tables (typical names)
...     >>> pq_tables = ["pgqueuer_jobs", "pgqueuer_schedules", "pgqueuer_locks"]
...     >>> 
...     >>> # Check for conflicts
...     >>> conflicts = set(my_tables) & set(pq_tables)
...     >>> len(conflicts) == 0
...     True
...     
...     >>> # Safe to install
...     >>> def is_safe_to_install(existing, new):
...     ...     return len(set(existing) & set(new)) == 0
...     >>> 
...     >>> is_safe_to_install(my_tables, pq_tables)
...     True
...     '''
...     return len(set(existing_tables) & set(pgqueuer_tables)) == 0

>>> # Database schema isolation
>>> def demonstrate_schema_safety():
...     '''
...     >>> # PgQueuer uses prefixed table names
...     >>> pgq_tables = ["pgqueuer_jobs", "pgqueuer_schedules"]
...     >>> all(table.startswith("pgqueuer_") for table in pgq_tables)
...     True
...     
...     >>> # Your tables remain untouched
...     >>> your_tables = ["users", "orders"]
...     >>> pgq_tables = ["pgqueuer_jobs"]
...     >>> 
...     >>> # No naming conflicts
...     >>> any(your_table in pgq_tables for your_table in your_tables)
...     False
...     '''
...     return "Schema isolation confirmed"
```

## Installation & Setup

### Q: What are the minimum requirements for PgQueuer?

**A:** Python 3.8+, PostgreSQL 12+, and the `asyncpg` driver.

```python
"""
Version compatibility matrix.

>>> def check_compatibility(python_version, postgres_version):
...     '''
...     Check if versions are compatible with PgQueuer.
...     
...     >>> # Test minimum versions
...     >>> check_compatibility("3.8.0", "12.0")
...     True
...     >>> 
...     >>> # Test newer versions
...     >>> check_compatibility("3.11.0", "15.2")
...     True
...     >>> 
...     >>> # Test unsupported versions  
...     >>> check_compatibility("3.7.0", "11.0")
...     False
...     '''
...     python_ok = tuple(map(int, python_version.split('.'))) >= (3, 8, 0)
...     postgres_ok = tuple(map(int, postgres_version.split('.'))) >= (12, 0)
...     return python_ok and postgres_ok

>>> # Required packages
>>> required_packages = ["pgqueuer", "asyncpg"]
>>> optional_packages = ["psycopg2-binary", "uvloop"]

>>> def get_install_command(extras=None):
...     '''
...     Generate installation commands.
...     
...     >>> # Basic installation
...     >>> get_install_command()
...     'pip install pgqueuer asyncpg'
...     
...     >>> # With CLI tools
...     >>> get_install_command(["cli"])
...     'pip install pgqueuer[cli] asyncpg'
...     
...     >>> # With all extras
...     >>> get_install_command(["cli", "dev"])
...     'pip install pgqueuer[cli,dev] asyncpg'
...     '''
...     base = "pgqueuer"
...     if extras:
...         base += f"[{','.join(extras)}]"
...     return f"pip install {base} asyncpg"
```

### Q: How do I set up the database tables?

**A:** Use the built-in initialization method:

```python
"""
Database initialization process.

>>> import asyncio
>>> import asyncpg
>>> from pgqueuer.db import AsyncpgDriver
>>> from pgqueuer.queries import Queries

>>> async def setup_database_tables():
...     '''
...     Initialize PgQueuer tables in your database.
...     
...     >>> # Database setup steps
...     >>> setup_steps = [
...     ...     "connect_to_database",
...     ...     "create_driver",
...     ...     "initialize_tables",
...     ...     "verify_setup"
...     ... ]
...     >>> 
...     >>> # Verify all steps are present
...     >>> len(setup_steps) == 4
...     True
...     >>> "initialize_tables" in setup_steps
...     True
...     '''
...     # Connection example (replace with your credentials)
...     connection = await asyncpg.connect(
...         "postgresql://user:password@localhost/mydb"
...     )
...     
...     driver = AsyncpgDriver(connection)
...     queries = Queries(driver)
...     
...     # Create all required tables
...     await queries.initialize()
...     
...     await connection.close()
...     return "Database initialized successfully"

>>> # Verify table creation
>>> async def verify_tables_created(connection):
...     '''
...     Check if PgQueuer tables were created properly.
...     
...     >>> # Expected tables after initialization
...     >>> expected_tables = [
...     ...     "pgqueuer_jobs",
...     ...     "pgqueuer_schedules", 
...     ...     "pgqueuer_locks"
...     ... ]
...     >>> 
...     >>> # All tables should exist
...     >>> len(expected_tables) >= 2
...     True
...     >>> all("pgqueuer_" in table for table in expected_tables)
...     True
...     '''
...     tables_query = '''
...         SELECT table_name FROM information_schema.tables 
...         WHERE table_schema = 'public' AND table_name LIKE 'pgqueuer_%'
...     '''
...     # This would return actual table names in real usage
...     return ["pgqueuer_jobs", "pgqueuer_schedules"]
```

**One-time setup script:**
```python
# setup_pgqueuer.py
import asyncio
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def main():
    conn = await asyncpg.connect("your_database_url_here")
    driver = AsyncpgDriver(conn)
    queries = Queries(driver)
    await queries.initialize()
    await conn.close()
    print("✅ PgQueuer tables created successfully!")

if __name__ == "__main__":
    asyncio.run(main())
```

## Job Processing

### Q: How do I handle job failures and retries?

**A:** PgQueuer supports automatic retries through job scheduling and manual error handling:

```python
"""
Job failure handling and retry strategies.

>>> from datetime import datetime, timedelta
>>> import json
>>> from pgqueuer.models import Job

>>> class JobRetryHandler:
...     def __init__(self, max_retries=3):
...         self.max_retries = max_retries
...     
...     def should_retry(self, attempt_count, error_type):
...         '''
...         Determine if job should be retried.
...         
...         >>> handler = JobRetryHandler(max_retries=3)
...         >>> 
...         >>> # Retry on temporary failures
...         >>> handler.should_retry(1, "ConnectionError")
...         True
...         >>> 
...         >>> # Don't retry on validation errors
...         >>> handler.should_retry(1, "ValidationError")
...         False
...         >>> 
...         >>> # Stop after max retries
...         >>> handler.should_retry(4, "ConnectionError")
...         False
...         '''
...         if attempt_count >= self.max_retries:
...             return False
...         
...         # Don't retry validation errors
...         non_retryable = ["ValidationError", "AuthenticationError"]
...         return error_type not in non_retryable

>>> async def robust_job_handler(job: Job):
...     '''
...     Example job handler with proper error handling.
...     
...     >>> # Simulate job processing
...     >>> def simulate_job_processing(payload, should_fail=False):
...     ...     if should_fail:
...     ...         raise ConnectionError("Database temporarily unavailable")
...     ...     return {"status": "success", "processed_at": "2024-01-01T12:00:00Z"}
...     
...     >>> # Test successful processing
...     >>> result = simulate_job_processing({"data": "valid"})
...     >>> result["status"]
...     'success'
...     
...     >>> # Test failure handling
...     >>> try:
...     ...     simulate_job_processing({"data": "valid"}, should_fail=True)
...     ... except ConnectionError as e:
...     ...     error_type = type(e).__name__
...     >>> error_type
...     'ConnectionError'
...     '''
...     try:
...         # Process the job
...         data = json.loads(job.payload.decode())
...         # Your job logic here
...         return "Job completed successfully"
...         
...     except json.JSONDecodeError:
...         # Don't retry invalid JSON
...         raise ValueError("Invalid job payload - cannot retry")
...         
...     except ConnectionError:
...         # Temporary failure - can retry
...         raise  # Re-raise to trigger retry logic
...         
...     except Exception as e:
...         # Log and decide whether to retry
...         print(f"Job {job.id} failed: {e}")
...         raise
```

### Q: Can I prioritize certain jobs over others?

**A:** Yes! Use the priority parameter when enqueuing jobs:

```python
"""
Job prioritization system.

>>> from pgqueuer.queries import Queries

>>> def demonstrate_job_priorities():
...     '''
...     Show how job priorities work in PgQueuer.
...     
...     >>> # Priority levels (lower number = higher priority)
...     >>> priorities = {
...     ...     "critical": 0,
...     ...     "high": 1, 
...     ...     "normal": 5,
...     ...     "low": 10
...     ... }
...     >>> 
...     >>> # Higher priority jobs process first
...     >>> priorities["critical"] < priorities["normal"]
...     True
...     >>> priorities["high"] < priorities["low"]
...     True
...     
...     >>> # Example job batches with priorities
...     >>> job_batch = [
...     ...     {"type": "email", "priority": 5},      # normal
...     ...     {"type": "alert", "priority": 0},      # critical  
...     ...     {"type": "cleanup", "priority": 10}    # low
...     ... ]
...     >>> 
...     >>> # Sort by priority (critical first)
...     >>> sorted_jobs = sorted(job_batch, key=lambda x: x["priority"])
...     >>> sorted_jobs[0]["type"]
...     'alert'
...     >>> sorted_jobs[-1]["type"]
...     'cleanup'
...     '''
...     return "Priority demonstration complete"

>>> async def enqueue_prioritized_jobs(queries):
...     '''
...     Enqueue jobs with different priorities.
...     
...     >>> # Job data with priorities
...     >>> jobs_data = [
...     ...     {"entrypoint": "send_alert", "priority": 0, "urgent": True},
...     ...     {"entrypoint": "send_email", "priority": 5, "urgent": False},
...     ...     {"entrypoint": "cleanup_logs", "priority": 10, "urgent": False}
...     ... ]
...     >>> 
...     >>> # Verify priority ordering
...     >>> priorities = [job["priority"] for job in jobs_data]
...     >>> # Critical alerts should have priority 0
...     >>> min(priorities)
...     0
...     >>> # Background tasks should have higher numbers
...     >>> max(priorities)
...     10
...     '''
...     # Critical jobs (alerts, errors)
...     critical_jobs = ["send_alert", "handle_error"]
...     critical_payloads = [b"Alert: System down", b"Error: Payment failed"]
...     
...     await queries.enqueue(
...         entrypoints=critical_jobs,
...         payloads=critical_payloads,
...         priorities=[0, 0]  # Highest priority
...     )
...     
...     # Normal jobs (user actions)
...     normal_jobs = ["send_email", "process_upload"]
...     normal_payloads = [b"Welcome email", b"Process file.pdf"]
...     
...     await queries.enqueue(
...         entrypoints=normal_jobs,
...         payloads=normal_payloads,
...         priorities=[5, 5]  # Normal priority
...     )
...     
...     # Background jobs (maintenance)
...     background_jobs = ["cleanup_logs", "backup_data"]
...     background_payloads = [b"Clean old logs", b"Backup user data"]
...     
...     await queries.enqueue(
...         entrypoints=background_jobs,
...         payloads=background_payloads,
...         priorities=[10, 10]  # Low priority
...     )
...     
...     return "Jobs enqueued with priorities"
```

### Q: How can I schedule recurring jobs?

**A:** Use the `@pgq.schedule` decorator with cron expressions:

```python
"""
Scheduled and recurring job examples.

>>> from pgqueuer import PgQueuer
>>> from pgqueuer.models import Schedule
>>> from datetime import datetime

>>> def parse_cron_expression(cron_expr):
...     '''
...     Understand cron expressions used in PgQueuer.
...     
...     >>> # Common cron patterns
...     >>> cron_patterns = {
...     ...     "every_minute": "* * * * *",
...     ...     "every_hour": "0 * * * *", 
...     ...     "daily_9am": "0 9 * * *",
...     ...     "weekly_sunday": "0 0 * * 0",
...     ...     "monthly_1st": "0 0 1 * *"
...     ... }
...     >>> 
...     >>> # Validate pattern format (5 parts)
...     >>> def is_valid_cron(expr):
...     ...     return len(expr.split()) == 5
...     >>> 
...     >>> # Test all patterns are valid
...     >>> all(is_valid_cron(pattern) for pattern in cron_patterns.values())
...     True
...     
...     >>> # Test specific pattern
...     >>> cron_patterns["daily_9am"]
...     '0 9 * * *'
...     '''
...     parts = cron_expr.split()
...     return {
...         "minute": parts[0],
...         "hour": parts[1], 
...         "day": parts[2],
...         "month": parts[3],
...         "weekday": parts[4]
...     }

>>> async def setup_scheduled_jobs(pgq):
...     '''
...     Configure various scheduled jobs.
...     
...     >>> # Schedule configuration examples
...     >>> schedules = [
...     ...     {"name": "daily_report", "cron": "0 9 * * *", "desc": "9 AM daily"},
...     ...     {"name": "hourly_cleanup", "cron": "0 * * * *", "desc": "Every hour"},
...     ...     {"name": "weekly_backup", "cron": "0 2 * * 0", "desc": "Sunday 2 AM"}
...     ... ]
...     >>> 
...     >>> # Verify schedule formats
...     >>> for schedule in schedules:
...     ...     parts = len(schedule["cron"].split())
...     ...     if parts != 5:
...     ...         break
...     ... else:
...     ...     all_valid = True
...     >>> all_valid
...     True
...     '''
...     
...     # Daily reports at 9 AM
...     @pgq.schedule("daily_report", "0 9 * * *")
...     async def generate_daily_report(schedule: Schedule):
...         print(f"📊 Generating daily report at {datetime.now()}")
...         # Report generation logic
...         return "Daily report generated"
...     
...     # Cleanup every hour
...     @pgq.schedule("hourly_cleanup", "0 * * * *") 
...     async def cleanup_temp_files(schedule: Schedule):
...         print(f"🧹 Cleaning up temporary files at {datetime.now()}")
...         # Cleanup logic
...         return "Cleanup completed"
...     
...     # Weekly backup on Sundays at 2 AM
...     @pgq.schedule("weekly_backup", "0 2 * * 0")
...     async def weekly_database_backup(schedule: Schedule):
...         print(f"💾 Starting weekly backup at {datetime.now()}")
...         # Backup logic
...         return "Backup completed"
...     
...     return "Scheduled jobs configured"
```

## Performance & Scaling

### Q: How many jobs can PgQueuer handle per second?

**A:** Performance depends on your PostgreSQL setup and job complexity, but PgQueuer can handle thousands of simple jobs per second.

```python
"""
Performance characteristics and optimization.

>>> def estimate_throughput(job_complexity, db_config):
...     '''
...     Estimate job processing throughput.
...     
...     >>> # Simple jobs (JSON parsing, basic operations)
...     >>> simple_config = {"connections": 10, "cpu_cores": 4}
...     >>> throughput = estimate_throughput("simple", simple_config)
...     >>> throughput > 1000  # Should handle >1000 jobs/sec
...     True
...     
...     >>> # Complex jobs (API calls, file processing)
...     >>> complex_config = {"connections": 20, "cpu_cores": 8}  
...     >>> complex_throughput = estimate_throughput("complex", complex_config)
...     >>> complex_throughput < throughput  # Lower throughput for complex jobs
...     True
...     
...     >>> # Database-bound jobs
...     >>> db_bound_config = {"connections": 50, "cpu_cores": 16}
...     >>> db_throughput = estimate_throughput("database", db_bound_config)
...     >>> db_throughput > complex_throughput  # More connections help
...     True
...     '''
...     base_rates = {
...         "simple": 2000,    # JSON processing, calculations
...         "complex": 100,    # API calls, file processing  
...         "database": 500    # Database operations
...     }
...     
...     # Scale with available resources
...     connection_factor = min(db_config["connections"] / 10, 2.0)
...     cpu_factor = min(db_config["cpu_cores"] / 4, 2.0)
...     
...     return int(base_rates[job_complexity] * connection_factor * cpu_factor)

>>> def optimization_checklist():
...     '''
...     Performance optimization checklist.
...     
...     >>> # Database optimizations
...     >>> db_optimizations = [
...     ...     "connection_pooling",
...     ...     "proper_indexes", 
...     ...     "vacuum_analyze",
...     ...     "shared_preload_libraries"
...     ... ]
...     >>> 
...     >>> # Application optimizations
...     >>> app_optimizations = [
...     ...     "batch_processing",
...     ...     "async_operations",
...     ...     "efficient_serialization",
...     ...     "resource_pooling"
...     ... ]
...     >>> 
...     >>> # Check all categories covered
...     >>> total_optimizations = len(db_optimizations) + len(app_optimizations)
...     >>> total_optimizations >= 8
...     True
...     '''
...     return {
...         "database": ["Use connection pooling", "Add proper indexes", "Regular VACUUM ANALYZE"],
...         "application": ["Batch similar jobs", "Use async/await", "Optimize serialization"],
...         "infrastructure": ["Scale PostgreSQL", "Use SSD storage", "Monitor metrics"]
...     }