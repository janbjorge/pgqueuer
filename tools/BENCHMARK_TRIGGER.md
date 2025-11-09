# Trigger Performance Benchmark

This benchmark demonstrates the performance improvement from the new trigger implementation that filters out heartbeat-only updates.

## Background

The queue manager regularly updates the `heartbeat` column on active jobs to indicate they're still being processed. Previously, every heartbeat update triggered a database notification, creating significant overhead.

The new trigger implementation filters out these heartbeat-only updates, sending notifications only when meaningful data changes. This dramatically reduces notification spam.

## Running the Benchmark

### Prerequisites

1. Ensure PostgreSQL is running and accessible
2. Set environment variables for database connection (or use defaults):
   ```bash
   export PGHOST=localhost
   export PGPORT=5432
   export PGUSER=pgquser
   export PGPASSWORD=pgqpw
   export PGDATABASE=pgqdb
   ```

### Quick Start

```bash
# Run with default settings (100 jobs, 1000 heartbeat operations)
python tools/benchmark_trigger_performance.py

# Run with custom settings
python tools/benchmark_trigger_performance.py --num-jobs 200 --num-heartbeats 2000

# See all options
python tools/benchmark_trigger_performance.py --help
```

### Options

- `--num-jobs`: Number of jobs to insert into the queue (default: 100)
- `--num-heartbeats`: Number of heartbeat update operations to perform (default: 1000)

## What It Measures

The benchmark compares two scenarios:

### Old Trigger
Sends notifications for:
- Job inserts: ~num_jobs notifications
- Heartbeat updates: ~num_heartbeats notifications
- **Total**: ~(num_jobs + num_heartbeats) notifications

### New Trigger  
Sends notifications for:
- Job inserts: ~num_jobs notifications
- Heartbeat updates: **0 notifications** (filtered out)
- **Total**: ~num_jobs notifications

## Expected Results

With default settings (100 jobs, 1000 heartbeats):
- **Old trigger**: ~1,100 notifications
- **New trigger**: ~100 notifications
- **Reduction**: ~90%

The notification reduction translates directly to:
- Less CPU overhead in PostgreSQL
- Less network traffic
- Faster queue operations
- Better scalability

## Sample Output

```
==============================================================
TRIGGER PERFORMANCE BENCHMARK
==============================================================

Configuration:
  Jobs to insert: 100
  Heartbeat operations: 1000
  Expected heartbeat updates: ~10000

------------------------------------------------------------
PHASE 1: OLD TRIGGER (notifies on all updates)
------------------------------------------------------------
✓ Installed old trigger (notifies on all updates)

Running benchmark with OLD trigger...
  - Inserting 100 jobs...
  - Performing 1000 heartbeat updates...
  ✓ Completed in 2.145s, 1,100 notifications

------------------------------------------------------------
PHASE 2: NEW TRIGGER (filters heartbeat-only updates)
------------------------------------------------------------
✓ Installed new trigger (filters heartbeat-only updates)

Running benchmark with NEW trigger...
  - Inserting 100 jobs...
  - Performing 1000 heartbeat updates...
  ✓ Completed in 1.923s, 100 notifications

==============================================================
RESULTS
==============================================================

--- Old Trigger Results ---
+----------------------+--------+
| Metric               | Value  |
+----------------------+--------+
| Trigger Type         | OLD    |
| Jobs Created         | 100    |
| Heartbeat Updates    | 10,000 |
| Notifications        | 1,100  |
| Elapsed Time (s)     | 2.145  |
+----------------------+--------+

--- New Trigger Results ---
+------------------------+--------+
| Metric                 | Value  |
+------------------------+--------+
| Trigger Type           | NEW    |
| Jobs Created           | 100    |
| Heartbeat Updates      | 10,000 |
| Notifications          | 100    |
| Elapsed Time (s)       | 1.923  |
| Notification Reduction | 90.9%  |
+------------------------+--------+

==============================================================
ANALYSIS
==============================================================

Notification Reduction: 90.9%
  • Old trigger: 1,100 notifications
  • New trigger: 100 notifications
  • Saved: 1,000 notifications

✅ EXCELLENT! The new trigger reduces notification overhead by 90.9%.
   This significantly improves performance by eliminating unnecessary
   notification spam from heartbeat-only updates.

💡 In production with frequent heartbeat updates, this reduction
   translates to less CPU overhead and improved scalability.
```

## Understanding the Impact

In a production environment with:
- 1,000 active jobs
- Heartbeat updates every 30 seconds
- Queue running for 1 hour

**Old trigger**: ~120,000 notifications/hour  
**New trigger**: ~1,000 notifications/hour (job state changes only)  
**Savings**: 99% reduction in notification overhead

This translates to measurable performance improvements in CPU usage, network traffic, and overall system responsiveness.
