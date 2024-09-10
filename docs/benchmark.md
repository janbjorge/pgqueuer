# Benchmark

PGQueuer underwent benchmark testing to evaluate its performance across varying job volumes and concurrency levels.

## Key Observations:
- **Consistency**: PGQueuer maintains consistent throughput across different job counts.
- **Performance**: The highest throughput observed was at ~6,4k jobs per second.
- **Scalability**: Performance increases with concurrency.

## Benchmarking Tool

PGQueuer includes a built-in benchmarking tool to help you assess performance in your environment. You can customize various parameters such as timer duration, number of workers, and batch sizes for enqueueing and dequeueing.

## Running the Benchmark

To run a benchmark, use the following command, ensuring you have set the appropriate environment variables for your PostgreSQL credentials.

```bash
python3 tools/benchmark.py -dq 5 -eqbs 10
Settings:
Timer:                  15.0 seconds
Dequeue:                5
Dequeue Batch Size:     10
Enqueue:                1
Enqueue Batch Size:     10

Queue size: 0
Queue size: 114
Queue size: 84
Queue size: 74
Queue size: 114
Queue size: 184
Queue size: 126
Queue size: 455
Queue size: 1622
Queue size: 3843
Jobs per Second: 18.35k
```

## Performance Comparison: asyncpg vs psycopg

In our benchmarking tests, we compared the performance of two PostgreSQL drivers: `asyncpg` and `psycopg`. These tests were conducted to understand the differences in throughput and efficiency when using each driver with PGQueuer under the same conditions.

### Test Setup

Both tests were run with the following settings to ensure a fair comparison:
- **Timer**: 10.0 seconds
- **Dequeue**: 5
- **Dequeue Batch Size**: 10
- **Enqueue**: 1
- **Enqueue Batch Size**: 10

### Results

#### asyncpg

The first test used `asyncpg`, an asynchronous PostgreSQL driver, and produced the following results:

```bash
python3 tools/benchmark.py -d apg
Settings:
Timer:                  10.0 seconds
Dequeue:                5
Dequeue Batch Size:     10
Enqueue:                1
Enqueue Batch Size:     10

140k job [00:09, 14.1k job/s]
```

- **Total Jobs Processed**: 140k
- **Throughput**: 14.1k jobs per second

#### psycopg

The second test used `psycopg`, a popular PostgreSQL driver for Python, yielding the following results:

```bash
python3 tools/benchmark.py -d psy
Settings:
Timer:                  10.0 seconds
Dequeue:                5
Dequeue Batch Size:     10
Enqueue:                1
Enqueue Batch Size:     10

85.1k job [00:10, 8.35k job/s]
```

- **Total Jobs Processed**: 85.1k
- **Throughput**: 8.35k jobs per second

### Key Observations

- **Throughput**: `asyncpg` demonstrated significantly higher throughput compared to `psycopg`, processing jobs at nearly double the rate.
- **Efficiency**: The asynchronous nature of `asyncpg` allows for better utilization of system resources, leading to more efficient job processing.
