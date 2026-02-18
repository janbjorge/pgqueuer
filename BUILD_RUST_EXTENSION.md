# Building the Rust InMemoryCore Extension

This document explains how to build the Rust PyO3 extension for the InMemoryRepository.

## Overview

The Rust extension (`pgqueuer._core`) is a PyO3 module that implements high-performance in-memory job queue storage. It replaces the pure-Python implementation with a compiled Rust backend for 3-4× throughput improvement.

## Prerequisites

1. **Rust Toolchain**: Install from [rustup.rs](https://rustup.rs/)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Python 3.10+**: Required for PyO3 compatibility

3. **uv**: The package manager and tool runner (usually included with the project setup)

4. **Network Access**: Required to download Rust crates from crates.io

**Note**: maturin is automatically invoked via `uvx maturin` (no separate installation needed)

## Building

### Option 1: Full Development Build (Recommended)

```bash
# Using the provided script
./build_rust_extension.sh

# Or directly with uvx
uvx maturin develop --release
```

This:
- Compiles the Rust extension in release mode (optimized)
- Installs it in your current virtualenv
- Allows the Python test suite to import `pgqueuer.core_rs`

### Option 2: Build Wheel

```bash
# Using the provided script
./build_rust_extension.sh --wheel

# Or directly with uvx
uvx maturin build --release

# Outputs to: target/wheels/pgqueuer_core-0.1.0-cp*.whl
```

### Option 3: Build and Install from Source

```bash
# Build + install in one step (uses maturin automatically)
uv pip install -e .
```

## Verifying the Build

After building, verify the extension loads correctly:

```bash
python -c "from pgqueuer.core_rs import InMemoryCore; print(InMemoryCore)"
# Output: <class 'pgqueuer.core_rs.InMemoryCore'>
```

## Running Tests

```bash
# Full test suite (uses InMemoryRepository via InMemoryCore)
pytest test/ -x -q

# Port conformance tests (verify interface is correct)
pytest test/test_port_conformance.py -v

# In-memory specific tests
pytest test/ -k inmemory -v

# Benchmark
python tools/benchmark_inmemory.py -t 10 --batch-size 10
```

## Performance Expected

After building successfully, you should see:

| Operation | Before Rust | After Rust | Expected |
|---|---|---|---|
| Enqueue throughput | ~42k jobs/sec | 150-200k/sec | **3-4×** |
| Dequeue throughput | ~42k jobs/sec | 200k+/sec | **2-3×** |
| `job_status(ids)` lookup | O(n) reverse scan | O(1) per ID | **large** |

## Architecture

### Directory Structure

```
pgqueuer/core_rs/
  Cargo.toml            # Rust crate manifest
  src/
    lib.rs              # PyO3 module entry point
    core.rs             # InMemoryCore #[pyclass]
    entrypoint_queue.rs # Per-entrypoint queue storage
    types.rs            # JobStatus, JobRow, LogRow, etc.
pgqueuer/
  core_rs.pyi           # Type stubs for mypy
  adapters/persistence/inmemory.py  # Python wrapper
```

### How It Works

1. **Rust Core** (`InMemoryCore`): Holds all mutable state (jobs, logs, queues)
   - Pure Rust data structures: BinaryHeap, HashMap, HashSet
   - Timestamps stored as i64 microseconds (no datetime overhead)
   - O(1) job status lookups via `log_status_idx`

2. **Python Wrapper** (`InMemoryRepository`): Thin async interface
   - asyncio.Lock for GIL coordination
   - Time conversion: datetime ↔ microseconds
   - Pydantic model construction only at API boundaries (dequeue, queue_log, etc.)

## Troubleshooting

### Build fails with "failed to download from crates.io"

**Cause**: Network timeout downloading Rust dependencies.

**Solution**:
- Ensure you have internet access (no proxy blocking crates.io)
- Try again (transient network issues are common)
- If behind a proxy, configure Cargo:
  ```toml
  # ~/.cargo/config.toml
  [http]
  proxy = "http://proxy.example.com:8080"
  ```

### "No module named pgqueuer._core" after build

**Cause**: Extension not installed in current environment.

**Solution**:
```bash
# Rebuild in current environment
maturin develop --release

# Or install directly
pip install -e .
```

### Tests fail with "KeyError: 'entrypoint'"

**Cause**: Mismatch between Rust and Python wrapper timestamp handling.

**Solution**:
- Verify `_us()` returns correct microsecond timestamps
- Check that Rust types.rs and Python inmemory.py use same epoch

## Development Workflow

If modifying Rust code:

```bash
# 1. Edit src/core.rs, src/types.rs, or src/entrypoint_queue.rs
# 2. Rebuild (auto-installs in venv)
maturin develop --release

# 3. Run tests to verify
pytest test/test_port_conformance.py -v

# 4. Benchmark to confirm performance
python tools/benchmark_inmemory.py -t 10
```

## Release Build

To create an optimized release wheel for distribution:

```bash
# Build wheels for multiple Python versions (requires maturin-action or Docker)
maturin build --release --interpreter python3.10 python3.11 python3.12

# Outputs wheels to: target/wheels/
ls target/wheels/pgqueuer_core-*.whl
```

## Type Checking

The `pgqueuer/core_rs.pyi` stub file enables mypy type checking:

```bash
mypy pgqueuer/adapters/persistence/inmemory.py --strict
```

If you modify the Rust interface, update `_core.pyi` to match.

## References

- [PyO3 Documentation](https://pyo3.rs/)
- [maturin Documentation](https://maturin.rs/)
- [Rust Book](https://doc.rust-lang.org/book/)
