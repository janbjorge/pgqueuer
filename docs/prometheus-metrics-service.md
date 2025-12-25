Prometheus Metrics Integration
==============================

PGQueuer includes a framework-agnostic Prometheus integration for metrics collection. This allows you to monitor queue sizes and job processing statistics in real-time.

The `collect_metrics` function returns a Prometheus-formatted string that you can serve from any web framework.

Usage
-----

Import the metrics helper from `pgqueuer.metrics.prometheus`:

```python
from pgqueuer.metrics.prometheus import collect_metrics, MetricNames
```

The `collect_metrics` function takes a `Queries` instance and returns a string ready for Prometheus to scrape.

FastAPI
-------

```python
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.responses import Response

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.prometheus import collect_metrics
from pgqueuer.queries import Queries


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.queries = Queries(AsyncpgDriver(await asyncpg.connect()))
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/metrics")
async def metrics() -> Response:
    return Response(
        content=await collect_metrics(app.state.queries),
        media_type="text/plain",
    )
```

Flask
-----

```python
import asyncio

import asyncpg
from flask import Flask

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.prometheus import collect_metrics
from pgqueuer.queries import Queries

app = Flask(__name__)


def get_queries() -> Queries:
    conn = asyncio.run(asyncpg.connect())
    return Queries(AsyncpgDriver(conn))


queries = get_queries()


@app.route("/metrics")
def metrics():
    content = asyncio.run(collect_metrics(queries))
    return content, 200, {"Content-Type": "text/plain"}
```

Starlette
---------

```python
from contextlib import asynccontextmanager

import asyncpg
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.prometheus import collect_metrics
from pgqueuer.queries import Queries


@asynccontextmanager
async def lifespan(app):
    app.state.queries = Queries(AsyncpgDriver(await asyncpg.connect()))
    yield


async def metrics(request):
    content = await collect_metrics(request.app.state.queries)
    return PlainTextResponse(content)


app = Starlette(lifespan=lifespan, routes=[Route("/metrics", metrics)])
```

Custom Metric Names
-------------------

You can customize the metric names to match your conventions:

```python
from pgqueuer.metrics.prometheus import collect_metrics, MetricNames

content = await collect_metrics(
    queries,
    metric_names=MetricNames(
        queue_count="myapp_queue_size",
        log_count="myapp_jobs_processed",
    ),
)
```

Time Window
-----------

By default, log statistics cover the last 5 minutes. You can adjust this:

```python
from datetime import timedelta

content = await collect_metrics(queries, last=timedelta(minutes=15))
```

Standalone Metrics Service
--------------------------

If you prefer to run metrics as a separate service, a Docker setup is available in the `tools/prometheus` directory.

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

After the service starts, the metrics endpoint will be available at `http://localhost:8000/metrics` for Prometheus to scrape.