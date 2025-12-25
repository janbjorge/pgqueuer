Prometheus Metrics Integration
==============================

PGQueuer includes an integration with Prometheus, enabling metrics collection. This feature allows users to gain insights into the performance and behavior of their job queues in real-time.

Usage
-----

The `collect_metrics` function returns a Prometheus-formatted string:

```python
from pgqueuer.metrics.prometheus import collect_metrics

content = await collect_metrics(queries)
```

### FastAPI

Install the optional extra:

```bash
pip install pgqueuer[fastapi]
```

```python
from pgqueuer.metrics.fastapi import create_metrics_router

app.include_router(create_metrics_router(queries))
```

### Other Frameworks

Use `collect_metrics` directly and return as plain text.

Standalone Metrics Service
--------------------------

A Docker setup is available in the `tools/prometheus` directory.

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
