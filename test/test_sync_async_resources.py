"""
Tests demonstrating correct and incorrect use of async (event‑loop bound) resources
inside sync entrypoints.

Context
-------
Sync entrypoints in PgQueuer run in a worker thread via anyio.to_thread.run_sync.
Async resources (e.g. asyncpg pools, httpx.AsyncClient, arbitrary async callables)
MUST NOT be invoked directly inside these sync functions, because that produces an
un-awaited coroutine (or worse, tries to interact with the loop from the wrong thread).

These tests showcase:
1. Misuse: Directly calling an async dependency inside a sync entrypoint -> returns a coroutine.
2. Correct bridging: Using anyio.from_thread.run (imported as from_thread.run) to schedule/await
   the async callable back on the main event loop.
3. Drain mode usage: We use QueueExecutionMode.drain instead of a custom loop/shutdown helper.

They serve both as documentation and regression coverage to ensure the current
behavior (no hidden magic making async objects thread-safe) remains explicit.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Awaitable, Callable

import pytest
from anyio import from_thread

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from pgqueuer.types import QueueExecutionMode


async def _async_dependency(value: str) -> str:
    """
    Simulated async resource dependency (e.g. DB query, HTTP call).

    Returns a simple formatted string after an await boundary to prove it's truly async.
    """
    await asyncio.sleep(0)  # Yield control to event loop
    return f"dep:{value}"


@pytest.mark.parametrize("N", (1, 4))
async def test_sync_entrypoint_async_resource_misuse(
    apgdriver: db.Driver,
    N: int,
) -> None:
    """
    Demonstrate the incorrect usage pattern:
        - A sync entrypoint calls an async resource directly.
        - The call returns a coroutine object instead of a resolved result.
        - We detect this and collect the coroutine so we can await it later
          (avoids un-awaited coroutine warnings polluting test output).
    """
    q = Queries(apgdriver)
    leaked_coroutines: list[Awaitable[str]] = []
    misuse_detected: list[bool] = []

    # Provide the async callable as a "resource"
    qm = QueueManager(
        apgdriver,
        resources={
            "async_func": _async_dependency,
        },
    )

    @qm.entrypoint("misuse")
    def misuse(job: Job) -> None:
        # Retrieve the async function
        ctx = qm.get_context(job.id)
        async_func: Callable[[str], Awaitable[str]] = ctx.resources["async_func"]
        # INCORRECT: Direct invocation returns a coroutine, not the awaited value
        result = async_func(f"job:{job.id}")
        # Record that the object is a coroutine (has __await__)
        misuse_detected.append(hasattr(result, "__await__"))
        # Keep for later awaiting (so test environment stays clean)
        leaked_coroutines.append(result)

    await q.enqueue(["misuse"] * N, [None] * N, [0] * N)

    # Drain mode processes all queued jobs then exits
    await qm.run(dequeue_timeout=timedelta(seconds=0.05), mode=QueueExecutionMode.drain)

    # All invocations produced coroutine objects (misuse)
    assert misuse_detected == [True] * N

    # Await leaked coroutines now so they don't trigger un-awaited warnings
    resolved = [await c for c in leaked_coroutines]
    # Confirm the async function itself behaves normally when awaited
    assert all(r.startswith("dep:job:") for r in resolved)


@pytest.mark.parametrize("N", (1, 4))
async def test_sync_entrypoint_async_resource_correct_bridging(
    apgdriver: db.Driver,
    N: int,
) -> None:
    """
    Demonstrate correct bridging:
        - A sync entrypoint calls async resource via from_thread.run
        - The result is the awaited value (string), not a coroutine
    """
    q = Queries(apgdriver)
    results: list[str] = []

    qm = QueueManager(
        apgdriver,
        resources={
            "async_func": _async_dependency,
        },
    )

    @qm.entrypoint("bridge")
    def bridge(job: Job) -> None:
        ctx = qm.get_context(job.id)
        async_func: Callable[[str], Awaitable[str]] = ctx.resources["async_func"]
        # CORRECT: Bridge the async call back to the main loop
        value = from_thread.run(async_func, f"job:{job.id}")
        results.append(value)

    await q.enqueue(["bridge"] * N, [None] * N, [0] * N)

    await qm.run(dequeue_timeout=timedelta(seconds=0.05), mode=QueueExecutionMode.drain)

    # All results are resolved strings (no coroutine objects)
    assert len(results) == N
    assert all(r.startswith("dep:job:") for r in results)


@pytest.mark.parametrize("N", (2,))
async def test_sync_entrypoint_shared_mutation_with_async_bridge(
    apgdriver: db.Driver,
    N: int,
) -> None:
    """
    Show that:
        - Shared resources mapping is the same across jobs.
        - Async invocation via bridging can mutate shared state safely.
    """
    q = Queries(apgdriver)
    qm = QueueManager(
        apgdriver,
        resources={
            "async_func": _async_dependency,
            "counter": {"value": 0},
        },
    )

    @qm.entrypoint("mutate")
    def mutate(job: Job) -> None:
        ctx = qm.get_context(job.id)
        from_thread.run(ctx.resources["async_func"], f"job:{job.id}")  # fire & await
        # Update shared mutation target
        ctx.resources["counter"]["value"] += 1

    await q.enqueue(["mutate"] * N, [None] * N, [0] * N)

    await qm.run(dequeue_timeout=timedelta(seconds=0.05), mode=QueueExecutionMode.drain)

    # Counter reflects all processed jobs (shared mutable state)
    assert qm.resources["counter"]["value"] == N
