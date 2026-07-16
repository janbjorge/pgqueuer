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

Example output — the tool prints a settings table, a live `tqdm` progress bar, then a
results table (exact numbers depend on your environment):

```
+--------------------+------------+
| Field              | Value      |
+--------------------+------------+
| Driver             | apg        |
| Strategy           | throughput |
| Timer (s)          | 30.0       |
| Dequeue Tasks      | 5          |
| Dequeue Batch Size | 10         |
| Enqueue Tasks      | 1          |
| Enqueue Batch Size | 10         |
| Output JSON        | None       |
+--------------------+------------+
550k job [00:30, 18.3k job/s]
```

### Drain Strategy

Enqueues a fixed number of jobs and measures how long PgQueuer takes to empty the queue:

```bash
python3 tools/benchmark.py --strategy drain --jobs 50000
```

Use this when evaluating batch processing performance rather than sustained throughput.

## asyncpg vs psycopg

The figures below come from a single reference run on one machine and are illustrative
only — run the tool in your own environment for numbers that mean anything. Both tests use
identical settings: 10-second timer, 5 dequeue workers (batch size 10), 1 enqueue worker
(batch size 10).

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

## CI Benchmark History and Regression Gate

Every CI run benchmarks all four drivers under both strategies. History from `main`
(pushes and the 6-hourly schedule) lives in the `benchmark-history` workflow
artifact: monthly NDJSON files (`benchmark/<YYYY-MM>.ndjson`), one JSON object per
line in the `--output-json` format of `tools/benchmark.py`.

Workflow artifacts expire after 90 days, so the `store-benchmark` job rolls the
history forward: on every main run it downloads the newest `benchmark-history`
artifact, appends the fresh results, and re-uploads it — resetting the expiry
clock each time. If no artifact exists (first run, or after a long dormant
period), the tooling bootstraps the full history from the public
[artifacts-storage](https://github.com/janbjorge/artifacts-storage) archive.

Fetch the history:

```bash
gh run download --repo janbjorge/PgQueuer -n benchmark-history
```

Rows before 2025 may lack `strategy` (implied `throughput`) and `queued`.

### The Gate

The `validate-benchmark` CI job compares every fresh result — including pull
requests — against the newest 30 `main` samples for the same driver/strategy
pair:

- **Fail** below `median − 3·MAD` (MAD: median absolute deviation).
- **Warn** below `median − 2·MAD`.
- **Skip** when fewer than 5 baseline samples exist.

MAD makes the gate robust to outlier CI runs and adapts it to each pair's real
variance. When the baseline is perfectly flat (MAD of 0) the thresholds fall back
to 95% / 97.5% of the median.

### Tooling

- `tools/benchmark_data.py` — row model, NDJSON storage, gate math.
- `tools/fetch_history.py` — downloads the newest `benchmark-history` artifact,
  falling back to the artifacts-storage bootstrap.
- `tools/compare_rps.py` — the gate: `python3 -m tools.compare_rps --data-dir <history> --current-dir <results>`.
- `tools/append_benchmarks.py` — appends results to the history, deduplicating on
  `(created_at, driver, strategy)`.

## Tuning for Higher Throughput

If you're not seeing the throughput you expect, see [Performance Tuning](../guides/performance-tuning.md)
for guidance on batch sizes, connection pooling, durability levels, and autovacuum settings.
