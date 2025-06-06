# PgQueuer Quickstart Guide

This quickstart guide will help you set up and use PgQueuer quickly. Follow the steps below to get started with your job queueing system.

## Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.7 or higher
- PostgreSQL 12 or higher
- Docker (optional, for containerized setup)

## Installation

1. **Clone the Repository**

   Clone the PgQueuer repository from GitHub:

   ```bash
   git clone https://github.com/yourusername/pgqueuer.git
   cd pgqueuer
   ```

2. **Install Dependencies**

   Install the required Python packages using pip:

   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up PostgreSQL**

   If you are using Docker, you can set up PostgreSQL using the provided `docker-compose.yml` file. Run the following command:

   ```bash
   docker-compose up -d
   ```

   This will start a PostgreSQL container with the necessary configurations.

## Configuration

Create a configuration file named `config.py` in your project directory with the following content:

```python
DATABASE_URL = "postgresql://pguser:pgpass@localhost:5432/pgqueuer"
```

Make sure to replace the credentials with your actual PostgreSQL user and password.

## Running the Consumer

To start processing jobs, run the PgQueuer consumer:

```bash
pgq run consumer:main
```

This command will start the consumer that listens for jobs in the queue and processes them.

## Enqueuing Jobs

You can enqueue jobs using the provided producer script. Create a file named `producer.py` with the following content:

```python
import asyncio
import json
from pgqueuer import PgQueuer

async def enqueue_job():
    pgq = PgQueuer()
    await pgq.enqueue("your_job_entrypoint", json.dumps({"key": "value"}).encode())

if __name__ == "__main__":
    asyncio.run(enqueue_job())
```

Run the producer script to enqueue a job:

```bash
python producer.py
```

## Monitoring Jobs

You can monitor the job queue using the built-in dashboard. Run the following command in a separate terminal:

```bash
pgq dashboard
```

This will provide you with real-time insights into the job processing status.

## Conclusion

You have now set up PgQueuer and are ready to start using it for your job queueing needs. For more detailed information, refer to the other documentation files in this project.