# Django Integration with PgQueuer

This document outlines how to integrate PgQueuer with Django applications, providing setup instructions and code examples for creating management commands.

## Prerequisites

Before you begin, ensure you have the following:

- A Django project set up.
- PgQueuer installed in your environment.

## Installation

To integrate PgQueuer with your Django project, you need to install the necessary dependencies. You can do this using pip:

```bash
pip install pgqueuer asyncpg
```

## Configuration

In your Django settings, configure the database connection for PgQueuer. Ensure that the database settings match your PostgreSQL configuration.

```python
# settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'pgqueuer',
        'USER': 'pguser',
        'PASSWORD': 'pgpass',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

## Creating a Management Command

To create a management command for running PgQueuer, follow these steps:

1. Create a new directory called `management/commands` inside one of your Django apps.
2. Create a new Python file for your command, e.g., `run_pgqueuer.py`.

Here’s an example of what your management command might look like:

```python
# management/commands/run_pgqueuer.py
from django.core.management.base import BaseCommand
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
import asyncpg
import asyncio

class Command(BaseCommand):
    help = 'Run the PgQueuer consumer'

    async def run_consumer(self):
        """Run the PgQueuer consumer."""
        connection = await asyncpg.connect(
            "postgresql://pguser:pgpass@localhost/pgqueuer"
        )
        driver = AsyncpgDriver(connection)
        pgq = PgQueuer(driver)
        await pgq.run()

    def handle(self, *args, **options):
        """Handle the command execution."""
        asyncio.run(self.run_consumer())
```

## Enqueuing Jobs from Django

You can enqueue jobs from your Django views or tasks using the following helper function:

```python
# views.py
from django.http import JsonResponse
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
import asyncpg
import json

async def enqueue_django_job(entrypoint: str, payload: dict, priority: int = 0):
    """Enqueue a job from Django views."""
    db_url = "postgresql://pguser:pgpass@localhost/pgqueuer"
    connection = await asyncpg.connect(db_url)
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    payload_bytes = json.dumps(payload).encode()
    await pgq.enqueue([entrypoint], [payload_bytes], [priority])
    await connection.close()

async def send_email_async(request):
    """Enqueue email sending job."""
    await enqueue_django_job("send_email", {"recipient": "test@example.com"}, priority=5)
    return JsonResponse({"status": "email_queued"})
```

## Running the Consumer

To run the PgQueuer consumer, use the management command you created:

```bash
python manage.py run_pgqueuer
```

This will start processing jobs that have been enqueued.

## Conclusion

Integrating PgQueuer with Django allows you to efficiently manage background jobs and tasks. By following the steps outlined in this document, you can set up your Django application to enqueue and process jobs seamlessly.