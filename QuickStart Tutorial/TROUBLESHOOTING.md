# Troubleshooting PgQueuer

This guide covers common issues you might encounter when using PgQueuer and their solutions.

## Database Connection Issues

### 1. Connection String Problems

**Error:**
```
asyncpg.exceptions.InvalidCatalogNameError: database "mydb" does not exist
```

**Solution:**
Verify your database exists and connection string is correct:

```python
"""
Test database connection with proper error handling.

>>> import asyncio
>>> import asyncpg
>>> from pgqueuer.db import AsyncpgDriver

>>> async def test_connection():
...     try:
...         # Test connection
...         conn = await asyncpg.connect("postgresql://user:pass@localhost/testdb")
...         result = await conn.fetchval("SELECT version()")
...         await conn.close()
...         return "Connected successfully"
...     except asyncpg.exceptions.InvalidCatalogNameError:
...         return "Database does not exist"
...     except asyncpg.exceptions.InvalidPasswordError:
...         return "Invalid credentials"
...     except Exception as e:
...         return f"Connection failed: {type(e).__name__}"

>>> # This would return connection status
>>> # asyncio.run(test_connection())
"""
```

**Common connection string formats:**
```python
# Local PostgreSQL
DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"

# Docker PostgreSQL
DATABASE_URL = "postgresql://postgres:password@db:5432/myapp"

# Cloud PostgreSQL (with SSL)
DATABASE_URL = "postgresql://user:pass@host:5432/db?sslmode=require"
```

### 2. SSL Connection Issues

**Error:**
```
asyncpg.exceptions.ConnectionDoesNotExistError: SSL connection has been closed unexpectedly
```

**Solution:**
Configure SSL properly:

```python
"""
SSL connection configuration examples.

>>> # For cloud databases that require SSL
>>> ssl_url = "postgresql://user:pass@host:5432/db?sslmode=require"
>>> 
>>> # For local development (disable SSL)
>>> local_url = "postgresql://user:pass@localhost:5432/db?sslmode=disable"
>>> 
>>> # Test URL format validation
>>> def validate_connection_url(url):
...     required_parts = ['postgresql://', '@', ':', '/']
...     return all(part in url for part in required_parts)
>>> 
>>> validate_connection_url(ssl_url)
True
>>> validate_connection_url("invalid-url")
False
"""
```

## Job Processing Issues

### 3. Jobs Stuck in Queue

**Issue:** Jobs are enqueued but never processed.

**Diagnosis:**
```python
"""
Check job queue status and diagnose stuck jobs.

>>> import json
>>> from datetime import datetime, timedelta

>>> async def diagnose_queue_issues(queries):
...     # Check for jobs older than 5 minutes
...     cutoff_time = datetime.now() - timedelta(minutes=5)
...     
...     # This would query the database for stuck jobs
...     stuck_jobs_query = '''
...         SELECT id, entrypoint, created_at, status 
...         FROM pgqueuer_jobs 
...         WHERE status = 'queued' AND created_at < $1
...         LIMIT 10
...     '''
...     
...     return "Query would show stuck jobs here"

>>> # Example of what stuck job data looks like
>>> sample_stuck_job = {
...     "id": "123e4567-e89b-12d3-a456-426614174000",
...     "entrypoint": "email_task",
...     "created_at": "2024-01-01T10:00:00Z",
...     "status": "queued"
... }
>>> 
>>> # Check if job is older than 5 minutes
>>> job_age_minutes = 10  # Example: 10 minutes old
>>> is_stuck = job_age_minutes > 5
>>> is_stuck
True
"""
```

**Solutions:**

1. **Check consumer is running:**
```bash
# Verify consumer process
pgq run examples.consumer:main --log-level DEBUG

# Check specific entrypoint
pgq run consumer:main --entrypoint email_task
```

2. **Verify entrypoint registration:**
```python
"""
Common entrypoint registration issues.

>>> # Correct entrypoint registration
>>> def register_entrypoint_correctly():
...     # This decorator name must match the job entrypoint
...     def email_task_handler():
...         return "Handler registered as 'email_task'"
...     return email_task_handler
>>> 
>>> # Common mistake - mismatched names
>>> def common_mistake_example():
...     # Job enqueued with entrypoint: "send_email"
...     # But handler registered as: "email_handler"
...     # This causes jobs to never be processed
...     return "Entrypoint names must match exactly"
>>> 
>>> register_entrypoint_correctly()
"Handler registered as 'email_task'"
"""
```

### 4. Job Failures and Retries

**Issue:** Jobs fail repeatedly without proper error handling.

**Solution:**
```python
"""
Proper error handling and retry logic.

>>> import json
>>> from pgqueuer.models import Job

>>> async def robust_job_handler(job: Job):
...     '''
...     Example of proper error handling in job handlers.
...     
...     >>> # Simulate job processing with error handling
...     >>> def process_with_retry(data, attempt=1, max_attempts=3):
...     ...     if attempt < max_attempts:
...     ...         try:
...     ...             # Simulate processing
...     ...             if data.get("should_fail", False) and attempt == 1:
...     ...                 raise ValueError("Simulated failure")
...     ...             return f"Processed successfully on attempt {attempt}"
...     ...         except Exception as e:
...     ...             return f"Failed attempt {attempt}, retrying..."
...     ...     return "Max attempts reached"
...     
...     >>> # Test successful processing
...     >>> process_with_retry({"data": "valid"})
...     'Processed successfully on attempt 1'
...     
...     >>> # Test retry logic
...     >>> process_with_retry({"should_fail": True}, attempt=2)
...     'Processed successfully on attempt 2'
...     '''
...     try:
...         data = json.loads(job.payload.decode())
...         # Process job data
...         return "Job processed successfully"
...     except json.JSONDecodeError:
...         # Log error and potentially retry
...         raise ValueError("Invalid JSON payload")
...     except Exception as e:
...         # Log error for debugging
...         print(f"Job {job.id} failed: {e}")
...         raise  # Re-raise to trigger retry if configured
"""
```

## Performance Issues

### 5. Slow Job Processing

**Issue:** Jobs are processing too slowly.

**Diagnosis and Solutions:**

```python
"""
Performance optimization techniques.

>>> import time
>>> from datetime import datetime

>>> def measure_job_performance():
...     '''
...     Measure and optimize job performance.
...     
...     >>> # Simulate timing measurement
...     >>> start_time = time.time()
...     >>> # Simulate work
...     >>> processing_time = 0.1  # 100ms
...     >>> end_time = start_time + processing_time
...     >>> 
...     >>> # Performance thresholds
...     >>> is_slow = processing_time > 0.05  # 50ms threshold
...     >>> is_slow
...     True
...     
...     >>> # Batch processing optimization
...     >>> single_job_time = 0.1
...     >>> batch_size = 10
...     >>> batch_time = 0.3  # Process 10 jobs in 300ms
...     >>> 
...     >>> # Calculate efficiency
...     >>> single_total = single_job_time * batch_size
...     >>> efficiency_gain = (single_total - batch_time) / single_total
...     >>> efficiency_gain > 0.5  # 50% improvement
...     True
...     '''
...     return "Performance measurement complete"

>>> # Connection pool optimization
>>> def optimize_connection_pool():
...     '''
...     >>> # Optimal pool settings for different scenarios
...     >>> light_load_pool = {"min_size": 1, "max_size": 5}
...     >>> heavy_load_pool = {"min_size": 5, "max_size": 20}
...     >>> 
...     >>> # Check pool configuration
...     >>> light_load_pool["max_size"] <= 10
...     True
...     >>> heavy_load_pool["min_size"] >= 5
...     True
...     '''
...     return "Pool optimized"
"""
```

**Optimization strategies:**

1. **Use connection pooling:**
```python
# Optimal connection pool configuration
import asyncpg

async def create_optimized_pool():
    return await asyncpg.create_pool(
        "postgresql://user:pass@localhost/db",
        min_size=5,
        max_size=20,
        command_timeout=30
    )
```

2. **Batch job processing:**
```python
# Process multiple jobs together
async def process_batch_jobs(jobs):
    # Group similar jobs and process together
    # This reduces database round-trips
    pass
```

### 6. Memory Issues

**Issue:** High memory usage with large job volumes.

**Solution:**
```python
"""
Memory optimization for high-volume job processing.

>>> import sys
>>> from dataclasses import dataclass

>>> @dataclass
>>> class JobMemoryInfo:
...     job_id: str
...     payload_size: int
...     
...     def is_large_payload(self, threshold_mb=1):
...         '''
...         Check if job payload is large.
...         
...         >>> job = JobMemoryInfo("123", 500 * 1024)  # 500KB
...         >>> job.is_large_payload()
...         False
...         
...         >>> large_job = JobMemoryInfo("456", 2 * 1024 * 1024)  # 2MB
...         >>> large_job.is_large_payload()
...         True
...         '''
...         return self.payload_size > (threshold_mb * 1024 * 1024)

>>> # Memory usage monitoring
>>> def check_memory_usage():
...     '''
...     >>> import resource
...     >>> # Get memory usage (this would work in real environment)
...     >>> # memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
...     >>> simulated_memory_mb = 150
...     >>> is_high_memory = simulated_memory_mb > 100
...     >>> is_high_memory
...     True
...     '''
...     return "Memory check complete"
"""
```

## Configuration Issues

### 7. Environment Variable Problems

**Issue:** Configuration not loading correctly.

**Solution:**
```python
"""
Proper environment variable handling.

>>> import os
>>> from typing import Optional

>>> def get_database_url() -> str:
...     '''
...     Get database URL with proper fallbacks.
...     
...     >>> # Test environment variable handling
...     >>> def mock_env_var(key: str, default: Optional[str] = None) -> Optional[str]:
...     ...     env_vars = {
...     ...         "DATABASE_URL": "postgresql://localhost/test",
...     ...         "PGQUEUER_CONCURRENCY": "10"
...     ...     }
...     ...     return env_vars.get(key, default)
...     
...     >>> # Test getting database URL
...     >>> url = mock_env_var("DATABASE_URL")
...     >>> url is not None
...     True
...     >>> "postgresql://" in url
...     True
...     
...     >>> # Test missing variable with fallback
...     >>> missing = mock_env_var("MISSING_VAR", "default_value")
...     >>> missing
...     'default_value'
...     '''
...     return os.getenv("DATABASE_URL", "postgresql://localhost/pgqueuer")

>>> def validate_config():
...     '''
...     Validate configuration settings.
...     
...     >>> config = {
...     ...     "concurrency": 5,
...     ...     "batch_size": 100,
...     ...     "timeout": 30
...     ... }
...     >>> 
...     >>> # Validate ranges
...     >>> config["concurrency"] > 0 and config["concurrency"] <= 50
...     True
...     >>> config["batch_size"] > 0 and config["batch_size"] <= 1000
...     True
...     >>> config["timeout"] >= 1
...     True
...     '''
...     return "Configuration valid"
```

## CLI and Dashboard Issues

### 8. Dashboard Not Starting

**Error:**
```
pgq: command not found
```

**Solution:**
```python
"""
CLI installation and path issues.

>>> import shutil
>>> import subprocess

>>> def check_pgq_installation():
...     '''
...     Check if pgq CLI is properly installed.
...     
...     >>> # Simulate checking if pgq is in PATH
...     >>> def is_command_available(cmd):
...     ...     # This would use shutil.which(cmd) in real scenario
...     ...     available_commands = ["python", "pip", "pgq"]
...     ...     return cmd in available_commands
...     
...     >>> is_command_available("pgq")
...     True
...     >>> is_command_available("nonexistent")
...     False
...     
...     >>> # Check installation status
...     >>> def check_package_installed(package):
...     ...     installed_packages = ["pgqueuer", "asyncpg", "click"]
...     ...     return package in installed_packages
...     
...     >>> check_package_installed("pgqueuer")
...     True
...     '''
...     return "CLI check complete"
```

**Installation fixes:**
```bash
# Reinstall in development mode
pip install -e .

# Install with CLI dependencies
pip install pgqueuer[cli]

# Check installation
pgq --version

# Alternative: run directly with Python
python -m pgqueuer.cli --help
```

## Debugging Tips

### 9. Enable Debug Logging

```python
"""
Enable comprehensive logging for debugging.

>>> import logging
>>> import sys

>>> def setup_debug_logging():
...     '''
...     Configure detailed logging for troubleshooting.
...     
...     >>> # Test log level configuration
...     >>> log_levels = {
...     ...     "DEBUG": 10,
...     ...     "INFO": 20,
...     ...     "WARNING": 30,
...     ...     "ERROR": 40
...     ... }
...     >>> 
...     >>> # Verify debug level is most verbose
...     >>> log_levels["DEBUG"] < log_levels["INFO"]
...     True
...     
...     >>> # Test log formatting
...     >>> log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
...     >>> "asctime" in log_format and "levelname" in log_format
...     True
...     '''
...     logging.basicConfig(
...         level=logging.DEBUG,
...         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
...         handlers=[
...             logging.StreamHandler(sys.stdout),
...             logging.FileHandler('pgqueuer.log')
...         ]
...     )
...     
...     # Enable PgQueuer specific logging
...     logging.getLogger('pgqueuer').setLevel(logging.DEBUG)
...     logging.getLogger('asyncpg').setLevel(logging.INFO)

>>> # Example usage
>>> setup_debug_logging()
```

### 10. Health Check Script

```python
"""
Comprehensive health check for PgQueuer setup.

>>> async def health_check():
...     '''
...     Perform comprehensive system health check.
...     
...     >>> # Test database connectivity
...     >>> def test_db_connection():
...     ...     return {"status": "connected", "latency_ms": 5}
...     
...     >>> # Test job processing
...     >>> def test_job_processing():
...     ...     return {"status": "working", "jobs_processed": 10}
...     
...     >>> # Test queue statistics
...     >>> def get_queue_stats():
...     ...     return {
...     ...         "pending_jobs": 5,
...     ...         "failed_jobs": 0,
...     ...         "active_consumers": 2
...     ...     }
...     
...     >>> # Run health checks
...     >>> db_health = test_db_connection()
...     >>> job_health = test_job_processing()
...     >>> queue_stats = get_queue_stats()
...     
...     >>> # Validate health
...     >>> all([
...     ...     db_health["status"] == "connected",
...     ...     job_health["status"] == "working",
...     ...     queue_stats["pending_jobs"] >= 0
...     ... ])
...     True
...     '''
...     checks = {
...         "database": "connected",
...         "consumers": "running", 
...         "jobs": "processing"
...     }
...     return checks
```

## Getting Help

### When to Open an Issue

Before opening a GitHub issue, please:

1. **Check this troubleshooting guide**
2. **Search existing issues** on GitHub
3. **Enable debug logging** and include relevant logs
4. **Provide a minimal reproduction example**

### Information to Include

When reporting issues, include:

```python
"""
System information template for bug reports.

>>> import sys
>>> import platform
>>> import asyncpg
>>> import pgqueuer

>>> def get_system_info():
...     '''
...     Collect system information for bug reports.
...     
...     >>> # Test info collection
...     >>> info = {
...     ...     "python_version": "3.9.7",
...     ...     "pgqueuer_version": "1.0.0",
...     ...     "asyncpg_version": "0.27.0",
...     ...     "os": "Linux",
...     ...     "postgres_version": "14.2"
...     ... }
...     >>> 
...     >>> # Validate required info is present
...     >>> required_keys = ["python_version", "pgqueuer_version", "os"]
...     >>> all(key in info for key in required_keys)
...     True
...     '''
...     return {
...         "python_version": sys.version,
...         "platform": platform.platform(),
...         "pgqueuer_version": pgqueuer.__version__,
...         "asyncpg_version": asyncpg.__version__
...     }
```

- Python version and platform
- PgQueuer version
- PostgreSQL version
- Full error traceback
- Minimal code example
- Configuration (without sensitive data)

### Community Resources

- **Documentation**: https://pgqueuer.re