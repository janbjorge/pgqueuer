# PgQueuer Producer Documentation

## Overview

This document provides detailed instructions on how to create a producer for PgQueuer. A producer is responsible for enqueuing jobs that will be processed by consumers. This guide includes code examples and explanations of the enqueueing process.

## Setting Up the Producer

To create a producer, you will need to set up a Python script that connects to your PgQueuer instance and enqueues jobs. Below is a basic example of how to implement a producer.

### Example Producer Code

```python
"""
PgQueuer producer example.

This script demonstrates how to enqueue jobs programmatically.
"""
from __future__ import annotations

import asyncio
import json
import random
import sys
from typing import Any

import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

async def enqueue_jobs(num_jobs: int = 100) -> None:
    """Enqueue multiple jobs for processing."""
    db_url = "postgresql://pguser:pgpass@localhost/pgqueuer"
    connection = await asyncpg.connect(db_url)
    driver = AsyncpgDriver(connection)
    queries = Queries(driver)

    for i in range(num_jobs):
        job_data = {
            "job_number": i,
            "task": "example_task"
        }
        payload_bytes = json.dumps(job_data).encode()
        await queries.enqueue(["example_task"], [payload_bytes], [0])

    await connection.close()

async def enqueue_single_job(entrypoint: str, priority: int = 0) -> None:
    """Enqueue a single job with custom data."""
    db_url = "postgresql://pguser:pgpass@localhost/pgqueuer"
    connection = await asyncpg.connect(db_url)
    driver = AsyncpgDriver(connection)
    queries = Queries(driver)

    job_data = {
        "task": entrypoint,
        "data": {"example_key": "example_value"}
    }
    payload_bytes = json.dumps(job_data).encode()
    await queries.enqueue([entrypoint], [payload_bytes], [priority])

    await connection.close()

async def main() -> None:
    """Main producer function."""
    await enqueue_jobs(100)

if __name__ == "__main__":
    import uvloop
    uvloop.run(main())
```

## Explanation of the Code

1. **Imports**: The necessary libraries are imported, including `asyncpg` for database connections and `pgqueuer` for interacting with the job queue.

2. **Database Connection**: The producer connects to the PostgreSQL database using the `asyncpg` library.

3. **Enqueueing Jobs**: The `enqueue_jobs` function demonstrates how to enqueue multiple jobs in a loop. Each job is represented as a JSON object.

4. **Single Job Enqueueing**: The `enqueue_single_job` function shows how to enqueue a single job with a specified entry point and priority.

5. **Main Function**: The `main` function serves as the entry point for the script, calling the `enqueue_jobs` function to enqueue a specified number of jobs.

## Running the Producer

To run the producer, execute the script in your terminal:

```bash
python producer.py
```

This will enqueue 100 jobs into the PgQueuer system, which can then be processed by consumers.

## Conclusion

This document provides a basic overview of how to create a producer for PgQueuer. You can customize the job data and entry points according to your application's requirements. For more advanced features and configurations, refer to the other documentation files in the PgQueuer project.