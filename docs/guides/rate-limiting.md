# Concurrency Control

PgQueuer provides fine-grained control over job execution concurrency at the
entrypoint level.

## Concurrency Limiting

Limit the number of jobs of a given type that run simultaneously:

```python
@pgq.entrypoint("data_processing", concurrency_limit=4)
async def process_data(job: Job) -> None:
    pass
```

This is useful for protecting external services with connection pool limits or
memory-intensive operations.

## Serialized Dispatch

Ensure jobs of the same type are processed strictly one at a time:

```python
@pgq.entrypoint("shared_resource", serialized_dispatch=True)
async def process_shared_resource(job: Job) -> None:
    pass
```

`serialized_dispatch=True` is equivalent to `concurrency_limit=1`.

## Global Concurrency Limit

You can also cap the total number of concurrently running tasks across **all** entrypoints
at the worker level using the CLI flag:

```bash
pgq run myapp:main --max-concurrent-tasks 20
```

This limits the total across all entrypoints regardless of individual entrypoint settings.

## Combining Controls

You can combine multiple controls on a single entrypoint:

```python
@pgq.entrypoint(
    "api_call",
    concurrency_limit=3,
)
async def call_external_api(job: Job) -> None:
    pass
```

## Configuring Timeouts

Two additional parameters control job processing timing:

- **`dequeue_timeout`**: Maximum time (in seconds) to wait for new jobs before returning an
  empty batch. Default: 30 seconds. Set at the `QueueManager` / `PgQueuer` level.
- **`retry_timer`**: Interval to retry unprocessed jobs. Default: 0 (no retry timer).
  Set per entrypoint via the `@entrypoint()` decorator.
