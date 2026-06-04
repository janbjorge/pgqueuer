# Concurrency Control

PgQueuer provides fine-grained control over job execution concurrency at the
entrypoint level. Concurrency limits are enforced **globally** at the database
level via the dequeue SQL query, not per-worker. If you set `concurrency_limit=5`,
at most 5 jobs run across your entire fleet.

## Concurrency Limiting

Limit the number of jobs of a given type that run simultaneously:

```python
@pgq.entrypoint("data_processing", concurrency_limit=4)
async def process_data(job: Job) -> None:
    pass
```

This is useful for protecting external services with connection pool limits or
memory-intensive operations.

## Serialized Processing

Ensure jobs of the same type are processed strictly one at a time:

```python
@pgq.entrypoint("shared_resource", concurrency_limit=1)
async def process_shared_resource(job: Job) -> None:
    pass
```

Setting `concurrency_limit=1` guarantees that only one job of this entrypoint runs
at a time across all workers.

## Global Concurrency Limit

You can also cap the total number of concurrently running tasks across **all** entrypoints
at the worker level using the CLI flag:

```bash
pgq run myapp:main --max-concurrent-tasks 20
```

This limits the total across all entrypoints regardless of individual entrypoint settings.

## Combining Controls

You can combine concurrency limits with other entrypoint options:

```python
@pgq.entrypoint(
    "api_call",
    concurrency_limit=3,
    on_failure="hold",
)
async def call_external_api(job: Job) -> None:
    pass
```

## Configuring Timeouts

Two parameters on `pgq.run()` control job processing timing:

- **`dequeue_timeout`**: Maximum time to wait for new jobs before re-checking.
  Default: 30 seconds.
- **`heartbeat_timeout`**: Duration after which a picked job with a stale heartbeat
  becomes eligible for re-pickup by another worker. Heartbeats are sent automatically
  at half this interval. Default: 30 seconds.
