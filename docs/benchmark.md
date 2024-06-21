# Benchmark

PgQueuer underwent benchmark testing to evaluate its performance across varying job volumes and concurrency levels.

## Key Observations:
- **Consistency**: PgQueuer maintains consistent throughput across different job counts.
- **Performance**: The highest throughput observed was at ~6,4k jobs per second.
- **Scalability**: Performance increases with concurrency.

## Benchmarking Tool

PgQueuer includes a built-in benchmarking tool to help you assess performance in your environment. You can customize various parameters such as timer duration, number of workers, and batch sizes for enqueueing and dequeueing.

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
