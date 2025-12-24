Prometheus Metrics Integration
==============================
PGQueuer includes an integration with Prometheus, enabling metrics collection. This feature allows users to gain insights into the performance and behavior of their job queues in real-time.

## Using Metrics in Your Application

Starting with version X.X.X, PGQueuer provides a built-in `generate_metrics()` function that returns Prometheus-formatted metrics text. This can be integrated into any web framework (FastAPI, Flask, Django, etc.).

### Basic Usage

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer import generate_metrics

# Connect to your database and create a driver
conn = await asyncpg.connect()
driver = AsyncpgDriver(conn)

# Generate metrics text
metrics_text = await generate_metrics(driver)
# Returns a string in Prometheus text format
```

### Integration with FastAPI

```python
from fastapi import FastAPI, Response
from pgqueuer import generate_metrics
from pgqueuer.db import AsyncpgDriver

app = FastAPI()

@app.get("/metrics")
async def metrics(driver: AsyncpgDriver):  # Use dependency injection for driver
    metrics_text = await generate_metrics(driver)
    return Response(content=metrics_text, media_type="text/plain")
```

### Integration with Flask

```python
from flask import Flask, Response
from pgqueuer import generate_metrics
from pgqueuer.db import AsyncpgDriver

app = Flask(__name__)

@app.route("/metrics")
async def metrics():
    metrics_text = await generate_metrics(driver)
    return Response(metrics_text, mimetype="text/plain")
```

### Custom Metric Names

You can customize the metric names to fit your naming conventions:

```python
from datetime import timedelta

metrics_text = await generate_metrics(
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
