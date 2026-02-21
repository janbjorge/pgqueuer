# Benchmarks

PGQueuer includes a built-in benchmarking tool for evaluating performance in your environment.

## Key Observations

- **Consistency**: PGQueuer maintains consistent throughput across different job volumes.
- **Peak throughput**: ~6.4k–18k jobs per second depending on concurrency and driver.
- **Scalability**: Performance increases with concurrency.

## Running the Benchmark

```bash
python3 tools/benchmark.py -dq 5 -eqbs 10
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

## Drain Strategy

The drain strategy enqueues a fixed number of jobs and measures how long PGQueuer takes
to empty the queue:

```bash
python3 tools/benchmark.py --strategy drain --jobs 50000
```

Use this when evaluating batch processing performance.

## asyncpg vs psycopg Comparison

Both tests run with the same settings:

- **Timer**: 10.0 seconds
- **Dequeue**: 5 workers, batch size 10
- **Enqueue**: 1 worker, batch size 10

### asyncpg

```bash
python3 tools/benchmark.py -d apg
```

```
140k job [00:09, 14.1k job/s]
```

- Total jobs: 140k
- Throughput: **14.1k jobs/s**

### psycopg

```bash
python3 tools/benchmark.py -d psy
```

```
85.1k job [00:10, 8.35k job/s]
```

- Total jobs: 85.1k
- Throughput: **8.35k jobs/s**

### Key Observations

- `asyncpg` achieves roughly double the throughput of `psycopg` under these conditions.
- The asynchronous nature of `asyncpg` allows better system resource utilization.
- For latency-sensitive or throughput-critical workloads, `asyncpg` or the asyncpg pool
  driver is recommended.
