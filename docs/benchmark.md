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
time python3 tools/benchmark.py
Settings:
Timer:                  15.0 seconds
Dequeue:                2
Dequeue Batch Size:     10
Enqueue:                1
Enqueue Batch Size:     20

Queue size: 80
Queue size: 70
Queue size: 69
Queue size: 48
Queue size: 22
Queue size: 34
Queue size: 71
Queue size: 114
Queue size: 187
Queue size: 30
Jobs per Second: 6.44k
```
