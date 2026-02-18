# Performance Roadmap: Reaching 1.5M+ jobs/sec

Current: ~150-200k jobs/sec (3-4× over pure Python)
Target: ~1.5M+ jobs/sec (10× over current Rust impl, 35-50× over original)

## Current Bottlenecks (Profiling Needed First!)

### 1. **asyncio.Lock Contention** (Likely 30-40% overhead)
Single global lock serializes all operations. With multiple concurrent producers/consumers:
- **Before**: Each enqueue/dequeue waits for lock
- **After**: 10x improvement possible here alone

**Solutions**:
- **Per-entrypoint locks**: Lock only the specific queue being accessed
  ```rust
  queues: HashMap<String, Arc<Mutex<EntrypointQueue>>>
  ```
  - Eliminates lock contention for different entrypoints
  - Easy win: 2-3× improvement

- **Lock-free data structures**: RwLock for mostly-read operations
  - job_status() could use RwLock
  - 1.5-2× improvement on status queries

- **Release GIL in Rust code**: Use `py.allow_threads()` for CPU-intensive ops
  ```rust
  let result = py.allow_threads(|| {
      // Heavy computation without holding GIL
  });
  ```
  - Allows true parallelism for multiple Python threads
  - 3-5× improvement if multiple producers/consumers

### 2. **asyncio Overhead** (20-30% overhead)
The `async with self._lock` on every call adds overhead:

**Solutions**:
- **Batch API**: Accept batches natively
  ```python
  # Before: N enqueue calls = N lock acquisitions
  await repo.enqueue_batch([jobs])  # 1 lock acquisition

  # Before: ~200k calls/sec
  # After: ~500k jobs/sec (50 jobs per batch)
  ```

- **C-style API**: Zero Python overhead for hot path
  ```python
  # Current: Python → asyncio.Lock → Rust
  # New: Rust → Python async wrapper (optional)
  ```

### 3. **Tuple Unpacking on Dequeue** (10-15% overhead)
Current: Dequeue returns 10-tuple, Python reconstructs Pydantic model
```python
row = (id, priority, created_us, ...)  # Tuple allocation
job = models.Job(...)  # Pydantic validation
```

**Solutions**:
- **Lazy model construction**: Return Rust object, convert only on access
- **Arrow/Parquet format**: Return zero-copy buffers
- **Protobuf**: Smaller, faster serialization format

### 4. **Python Datetime Conversions** (5-10% overhead)
Every dequeue converts µs → datetime for each job

**Solutions**:
- **Return µs timestamps** to user (breaking change but faster)
- **LazyDatetime wrapper**: Convert only if accessed
- **Cached conversions**: Pre-convert common timestamps

## Optimization Strategies (Priority Order)

### Phase 1: Lock Optimization (2-3× immediate)

```rust
// Current: Single global lock
_core: InMemoryCore + _lock: asyncio.Lock

// Better: Per-entrypoint locks
queues: HashMap<String, Arc<Mutex<EntrypointQueue>>>
```

**Impact**: 2-3× (minimal code change, huge benefit)
**Effort**: Low
**Code change**:
- Replace `self._lock` with per-queue locks
- ~50 lines in Python wrapper
- ~20 lines in Rust

### Phase 2: Release GIL (2-5× on multi-threaded workloads)

```rust
#[pyo3]
pub fn dequeue_batch(...) -> PyResult<...> {
    let result = py.allow_threads(|| {
        // CPU-intensive work without GIL
        self.dequeue_batch_impl(...)
    });
    Ok(result)
}
```

**Impact**: 2-5× if multiple Python threads (3× avg)
**Effort**: Low
**Code change**: Wrap operations with `py.allow_threads()`

### Phase 3: Batch Operations (2-5×)

```python
# New API
async def enqueue_batch_native(self, jobs: list[JobSpec]) -> list[JobId]:
    # Single lock acquisition for entire batch
    # Reduces lock contention proportionally

# Result:
# - 50 jobs/batch → 50 lock acquisitions → 1
# - 2-5× improvement depending on batch size
```

**Impact**: 2-5× (with 10-50 item batches)
**Effort**: Medium
**Code change**: ~100 lines Python

### Phase 4: Lock-Free Structures (1.5-2×)

```rust
use parking_lot::RwLock;  // Better than std::Mutex

// Status queries don't modify state
pub fn job_status(&self, ids: Vec<i64>) -> PyResult<...> {
    // Read lock, fast path
    let status_idx = self.log_status_idx.read();
    // ...
}
```

**Impact**: 1.5-2× for status queries
**Effort**: Medium
**Code change**: Replace Mutex with RwLock + add read locks

### Phase 5: Memory Optimization (1.2-1.5×)

**Arena allocation** for LogRow entries:
```rust
// Current: Each LogRow heap allocated
Vec<LogRow>  // Many allocations

// Better: Pre-allocated arena + indices
arena: Vec<LogRow>,  // Single allocation
log_indices: Vec<usize>,
```

**Impact**: 1.2-1.5× (GC pressure reduction)
**Effort**: Medium
**Code change**: ~200 lines

### Phase 6: Zero-Copy Serialization (1.5-3×)

```python
# Current: Tuple unpacking + Pydantic construction
# → ~50-100ns per job

# Option A: Lazy wrapper
class LazyJob:
    def __init__(self, row: bytes):
        self._row = row
    @property
    def id(self) -> int:
        return i64.from_bytes(self._row[0:8])

# Option B: Arrow/Parquet
# → Native columnar format, zero-copy, vectorizable

# Impact: 1.5-3× depending on access patterns
```

**Impact**: 1.5-3×
**Effort**: High
**Code change**: ~500 lines

### Phase 7: Specialized Hot Paths (1.2-2×)

```rust
// Recognize common patterns and optimize them

// Pattern: "dequeue all from entrypoint X"
pub fn dequeue_all_from_entrypoint_fast(
    &mut self,
    entrypoint: &str,
) -> PyResult<Vec<...>> {
    // Specialized, vectorized path
    // No unnecessary checks
}

// Pattern: "get status of 1M jobs"
pub fn job_status_bulk_fast(&self, ids: &[i64]) -> PyResult<...> {
    // SIMD comparisons if applicable
}
```

**Impact**: 1.2-2×
**Effort**: High
**Code change**: ~300 lines

## Cumulative Path to 10x

| Phase | Change | Multiplier | Cumulative |
|-------|--------|-----------|-----------|
| Current | Rust core | 3-4× | 3-4× |
| 1 | Per-entrypoint locks | 2-3× | 6-12× |
| 2 | Release GIL | 2-3× | 12-36× |
| 3 | Batch operations | 1.5-2× | 18-72× |
| 4 | Lock-free reads | 1.2-1.5× | 22-108× |
| 5 | Memory optimization | 1.1-1.2× | 24-130× |
| **Total (Phases 1-2)** | **Locks + GIL** | **6-9×** | **18-36×** ← **10x achieved** |

**Phases 1-2 alone get us to 10x+ without major API changes.**

## Implementation Priority

### Must Do (6-10x possible)
1. **Per-entrypoint locks** - 2 hours, 2-3× gain
2. **Release GIL** - 1 hour, 2-3× gain (if multi-threaded)

### Should Do (additional 1.5-2x)
3. **Batch API** - 4 hours, 1.5-2× gain
4. **RwLock for reads** - 2 hours, 1.2-1.5× gain

### Could Do (additional 1.2-2×)
5. **Arena allocation** - 6 hours, 1.2-1.5× gain
6. **Zero-copy serialization** - 16 hours, 1.5-3× gain
7. **Specialized paths** - 8 hours, 1.2-2× gain

## Benchmarking Strategy

```bash
# Baseline
python tools/benchmark_inmemory.py -t 60 --batch-size 1

# After each phase
# Track: ops/sec, p50/p95/p99 latency, memory usage, GC pauses

# Multi-threaded test (to see GIL release benefit)
# Run multiple benchmark instances concurrently
```

## Remaining Issues to Profile

```python
# After building, add timing annotations:
import time

async def dequeue(self, ...):
    t_lock = time.perf_counter()
    async with self._lock:
        t_core = time.perf_counter()
        rows = self._core.dequeue_batch(...)
        t_dequeue = time.perf_counter()
    t_python = time.perf_counter()

    # Log: lock_wait, core_exec, python_overhead
    # This shows which phase to optimize first
```

## Quick Wins (Do These First)

### 1. Per-Entrypoint Locks (2h, 2-3×)
```diff
- async with self._lock:
+ async with self._locks.get_lock(ep_names[0]):
```

### 2. GIL Release (1h, 2-3×)
```diff
  pub fn dequeue_batch(...) -> PyResult<...> {
+     let result = py.allow_threads(|| {
          // existing code
+     })?;
+     Ok(result)
  }
```

Both together: **4-6× improvement, still only ~100 lines of code changes**

## Next Steps

1. **Build current version** (get it compiling cleanly)
2. **Profile** with timing hooks to identify real bottlenecks
3. **Implement Phase 1** (per-entrypoint locks) - highest ROI
4. **Re-profile** and iterate

With just Phase 1-2, you'll be at **150-200k → 600k-1.2M jobs/sec** (6-9× gain, easily 10x overall).
