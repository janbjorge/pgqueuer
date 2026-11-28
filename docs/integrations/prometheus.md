# Prometheus Metrics

PgQueuer includes a Prometheus integration for real-time metrics on queue performance
and job behavior.

## Usage

The `collect_metrics` function returns a Prometheus-formatted string:

```python
from pgqueuer.metrics.prometheus import collect_metrics

content = await collect_metrics(queries)
```

## FastAPI integration

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

## Other frameworks

Use `collect_metrics` directly and return the result as plain text with content type
`text/plain; version=0.0.4`:

```python
async def metrics() -> Response:
    content = await collect_metrics(queries)
    return Response(content=content, media_type="text/plain; version=0.0.4")
```

## Standalone metrics service

A Docker-based standalone metrics service is available in `tools/prometheus`. The
service formats its metrics with the `pgqueuer.metrics` module.

### Building the image

```bash
docker build -t pgq-prometheus-service -f tools/prometheus/Dockerfile .
```

### Running the service

```bash
docker run -p 8000:8000 \
  -e PGHOST=your-postgres-host \
  -e PGDATABASE=your-database \
  -e PGPASSWORD=your-password \
  -e PGUSER=your-username \
  -e PGPORT=5432 \
  pgq-prometheus-service
```

After the service starts, the metrics endpoint is available at
`http://localhost:8000/metrics` for Prometheus to scrape.

### Health check

`GET /health` runs `SELECT 1` on the database connection of the service. It returns
200 when the query succeeds and 503 when it fails.

The service does not reconnect. After a Postgres restart, a failover or a terminated
backend, `/metrics` fails until the service starts again. Point the liveness probe of
your orchestrator at `/health`. The orchestrator then restarts the container when the
service loses its connection. For example, in Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
```

PgQueuer workers follow the same policy. See
[Connection loss](../guides/deployment.md#connection-loss).
