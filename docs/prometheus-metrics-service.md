Prometheus Metrics Integration
==============================

PGQueuer includes a framework-agnostic Prometheus integration for metrics collection. This allows you to monitor queue sizes and job processing statistics in real-time.

Installation
------------

The core metrics functionality has no extra dependencies. For plug-and-play framework integrations, install the optional extras:

```bash
# For FastAPI integration
pip install pgqueuer[fastapi]

# For Flask integration
pip install pgqueuer[flask]
```

Core Usage
----------

The `collect_metrics` function returns a Prometheus-formatted string that you can serve from any web framework:

```python
from pgqueuer.metrics.prometheus import collect_metrics

content = await collect_metrics(queries)
```

FastAPI Integration
-------------------

```python
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.fastapi import create_metrics_router
from pgqueuer.queries import Queries


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.queries = Queries(AsyncpgDriver(await asyncpg.connect()))
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(create_metrics_router(app.state.queries))
```

With custom path:

```python
app.include_router(create_metrics_router(queries, path="/custom/metrics"))
```

Flask Integration
-----------------

```python
import asyncio

import asyncpg
from flask import Flask

from pgqueuer.db import AsyncpgDriver
from pgqueuer.metrics.flask import create_metrics_blueprint
from pgqueuer.queries import Queries

queries = Queries(AsyncpgDriver(asyncio.run(asyncpg.connect())))

app = Flask(__name__)
app.register_blueprint(create_metrics_blueprint(queries))
```

With URL prefix:

```python
app.register_blueprint(create_metrics_blueprint(queries, url_prefix="/monitoring"))
```

Manual Integration
------------------

For other frameworks or custom setups, use `collect_metrics` directly:

### Starlette

```python
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from pgqueuer.metrics.prometheus import collect_metrics


async def metrics(request):
    content = await collect_metrics(request.app.state.queries)
    return PlainTextResponse(content)


app = Starlette(routes=[Route("/metrics", metrics)])
```

### Any ASGI/WSGI Framework

```python
from pgqueuer.metrics.prometheus import collect_metrics

# Async frameworks
content = await collect_metrics(queries)

# Sync frameworks
import asyncio
content = asyncio.run(collect_metrics(queries))
```

Custom Metric Names
-------------------

Customize metric names to match your conventions:

```python
from pgqueuer.metrics.prometheus import MetricNames, collect_metrics

content = await collect_metrics(
    queries,
    metric_names=MetricNames(
        queue_count="myapp_queue_size",
        log_count="myapp_jobs_processed",
    ),
)
```

Or with the framework integrations:

```python
from pgqueuer.metrics.fastapi import create_metrics_router
from pgqueuer.metrics.prometheus import MetricNames

router = create_metrics_router(
    queries,
    metric_names=MetricNames(queue_count="myapp_queue_size"),
)
```

Time Window
-----------

By default, log statistics cover the last 5 minutes. Adjust with the `last` parameter:

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