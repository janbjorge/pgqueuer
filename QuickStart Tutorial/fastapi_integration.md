# fastapi_integration.md

# FastAPI Integration with PgQueuer

This document outlines the integration of PgQueuer with FastAPI, providing examples of how to enqueue jobs and handle background processing.

## Overview

PgQueuer can be seamlessly integrated with FastAPI to manage background jobs efficiently. This integration allows you to enqueue tasks that can be processed asynchronously, improving the responsiveness of your FastAPI application.

## Setting Up FastAPI with PgQueuer

### Installation

Ensure you have FastAPI and PgQueuer installed in your environment. You can install them using pip:

```bash
pip install fastapi pgqueuer asyncpg
```

### Basic FastAPI Application

Here’s a simple FastAPI application that demonstrates how to integrate PgQueuer:

```python
from fastapi import FastAPI, BackgroundTasks
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from pgqueuer.queries import Queries

app = FastAPI()

# Database connection
async def get_db_connection():
    return await asyncpg.connect("postgresql://user:pass@localhost/pgqueuer_db")

# PgQueuer setup
async def setup_pgqueuer():
    connection = await get_db_connection()
    driver = AsyncpgDriver(connection)
    return PgQueuer(driver)

pgq_instance = setup_pgqueuer()

@app.post("/jobs/email")
async def enqueue_email_job(email: str, background_tasks: BackgroundTasks):
    """Enqueue an email job."""
    background_tasks.add_task(pgq_instance.enqueue, "send_email", {"email": email})
    return {"status": "email job queued"}

@app.post("/jobs/user")
async def enqueue_user_job(user_id: int, action: str):
    """Enqueue a user processing job."""
    await pgq_instance.enqueue("process_user", {"user_id": user_id, "action": action})
    return {"status": "user job queued"}

# Start the PgQueuer consumer
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

## Job Handlers

You need to define job handlers that will process the jobs you enqueue. Here’s an example of a job handler for sending emails:

```python
@pgq_instance.entrypoint("send_email")
async def send_email(job: Job):
    payload = job.payload.decode()
    email_data = json.loads(payload)
    email = email_data["email"]
    
    # Simulate sending an email
    print(f"Sending email to {email}")
    await asyncio.sleep(1)  # Simulate delay
    print(f"Email sent to {email}")
```

## Running the Application

To run your FastAPI application, use the following command:

```bash
uvicorn fastapi_app:app --reload
```

## Testing the API

You can test the API endpoints using curl or any API client:

```bash
curl -X POST "http://localhost:8000/jobs/email" -H "Content-Type: application/json" -d '{"email": "test@example.com"}'
```

This will enqueue an email job that will be processed by the PgQueuer consumer.

## Conclusion

Integrating PgQueuer with FastAPI allows you to handle background tasks efficiently, improving the performance and responsiveness of your application. You can extend this setup by adding more job handlers and endpoints as needed.