# Benchmarks

PgQueuer includes a built-in benchmarking tool for evaluating throughput in your own
environment. Results depend on hardware, PostgreSQL configuration, and network latency.

## Key Observations

- **Consistency**: PgQueuer maintains stable throughput across varying job volumes.
- **Peak throughput**: ~6.4k--18k jobs per second depending on concurrency, driver, and hardware.
- **asyncpg advantage**: The asyncpg driver achieves roughly 1.7x the throughput of psycopg
  under identical conditions, due to asyncpg's Cython-based implementation and lower per-query
  overhead.

## Running the Benchmark

The benchmark tool is located at `tools/benchmark.py` and supports two strategies.

### Throughput Strategy (default)

Continuously enqueues and dequeues jobs for a fixed duration:

```bash
python3 tools/benchmark.py --dequeue 5 --dequeue-batch-size 10
```

Example output:

```
Settings:
Timer:                  15.0 seconds
Dequeue:                5
Dequeue Batch Size:     10
Enqueue:                1
Enqueue Batch Size:     10

Queue size: 0
Queue size: 114
...
Jobs per Second: 18.35k
```

### Drain Strategy

Enqueues a fixed number of jobs and measures how long PgQueuer takes to empty the queue:

```bash
python3 tools/benchmark.py --strategy drain --jobs 50000
```

Use this when evaluating batch processing performance rather than sustained throughput.

## asyncpg vs psycopg

Both tests use identical settings: 10-second timer, 5 dequeue workers (batch size 10),
1 enqueue worker (batch size 10).

### asyncpg

```bash
python3 tools/benchmark.py -d apg -t 10
```

```
140k job [00:09, 14.1k job/s]
```

- Total jobs: 140k
- Throughput: **14.1k jobs/s**

### psycopg

```bash
python3 tools/benchmark.py -d psy -t 10
```

```
85.1k job [00:10, 8.35k job/s]
```

- Total jobs: 85.1k
- Throughput: **8.35k jobs/s**

### Supported Drivers

The benchmark tool supports all four drivers via the `-d` flag:

| Flag | Driver |
|------|--------|
| `apg` | `AsyncpgDriver` (single connection) |
| `apgpool` | `AsyncpgPoolDriver` (connection pool) |
| `psy` | `PsycopgDriver` (async psycopg) |
| `mem` | `InMemoryDriver` (no database) |

## Tuning for Higher Throughput

If you're not seeing the throughput you expect, see [Performance Tuning](../guides/performance-tuning.md)
for guidance on batch sizes, connection pooling, durability levels, and autovacuum settings.
