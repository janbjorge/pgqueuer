Prometheus Metrics Integration
==============================
PGQueuer includes an integration with Prometheus, enabling metrics collection. This feature allows users to gain insights into the performance and behavior of their job queues in real-time.

## Using the Metrics Router in Your Application

Starting with version X.X.X, PGQueuer provides a built-in metrics router that can be directly imported and integrated into your FastAPI application.

### Basic Usage

```python
import asyncpg
from fastapi import FastAPI
from pgqueuer.db import AsyncpgDriver
from pgqueuer import create_metrics_router

# Create your FastAPI app
app = FastAPI()

# Connect to your database and create a driver
async def setup():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    
    # Create and include the metrics router
    metrics_router = create_metrics_router(driver)
    app.include_router(metrics_router)

# Metrics will be available at /metrics endpoint
```

### Custom Metric Names

You can customize the metric names to fit your naming conventions:

```python
from datetime import timedelta

metrics_router = create_metrics_router(
    driver,
    queue_count_metric_name="my_app_queue_count",
    logs_count_metric_name="my_app_logs_count",
    log_statistics_last=timedelta(minutes=10)  # custom time window
)
```

### Parameters

- `driver`: A database driver instance (required)
- `queue_count_metric_name`: Custom name for the queue count metric (default: "pgqueuer_queue_count")
- `logs_count_metric_name`: Custom name for the logs count metric (default: "pgqueuer_logs_count")
- `log_statistics_last`: Time window for log statistics (default: 5 minutes)

## Standalone Metrics Service

Until there is a sufficient demand, no container image for the metrics service is published. Users must build and host the image themselves.

Building the Image
------------------

```bash
docker build -t pgq-prometheus-service -f tools/prometheus/Dockerfile .
```

Running the Service
-------------------

```bash
docker run -p 8000:8000 \
  -e PGHOST=your-postgres-host \
  -e PGDATABASE=your-database \
  -e PGPASSWORD=your-password \
  -e PGUSER=your-username \
  -e PGPORT=5432 \
  pgq-prometheus-service
```

Docker Compose
--------------

A docker-compose file `docker-compose.prometheus-metrics.yml` is provided:

```bash
docker compose -f docker-compose.prometheus-metrics.yml up
```

After the service starts, the metrics endpoint will be available at `http://localhost:8000/metrics` for Prometheus to scrape.
