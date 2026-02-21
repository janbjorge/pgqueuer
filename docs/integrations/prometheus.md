# Prometheus Metrics

PGQueuer includes a Prometheus integration for real-time metrics on queue performance
and job behavior.

## Usage

The `collect_metrics` function returns a Prometheus-formatted string:

```python
from pgqueuer.metrics.prometheus import collect_metrics

content = await collect_metrics(queries)
```

## FastAPI Integration

Install the optional extra:

```bash
pip install pgqueuer[fastapi]
```

Then mount the metrics router:

```python
from pgqueuer.metrics.fastapi import create_metrics_router

app.include_router(create_metrics_router(queries))
```

The `/metrics` endpoint will be available for Prometheus to scrape.

## Other Frameworks

Use `collect_metrics` directly and return the result as plain text with content type
`text/plain; version=0.0.4`:

```python
content = await collect_metrics(queries)
return Response(content=content, media_type="text/plain; version=0.0.4")
```

## Standalone Metrics Service

A Docker-based standalone metrics service is available in `tools/prometheus`.

### Building the Image

```bash
docker build -t pgq-prometheus-service -f tools/prometheus/Dockerfile .
```

### Running the Service

```bash
docker run -p 8000:8000 \
  -e PGHOST=your-postgres-host \
  -e PGDATABASE=your-database \
  -e PGPASSWORD=your-password \
  -e PGUSER=your-username \
  -e PGPORT=5432 \
  pgq-prometheus-service
```

### Docker Compose

A docker-compose file `docker-compose.prometheus-metrics.yml` is provided:

```bash
docker compose -f docker-compose.prometheus-metrics.yml up
```

After the service starts, the metrics endpoint is available at
`http://localhost:8000/metrics` for Prometheus to scrape.
