from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.completion import CompletionWatcher
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.types import QueueExecutionMode


async def test_completion_successful(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None: ...

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "successful" for w in waiters)


async def test_completion_already_successful(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None: ...

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "successful" for w in waiters)


async def test_completion_exception(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        raise ValueError

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "exception" for w in waiters)


async def test_for_completion_canceled(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)
    await qm.queries.mark_job_as_cancelled(jids)

    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "canceled" for w in waiters)


async def test_completion_deleted(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    @dataclass
    class FakeJob:
        id: int

    await qm.queries.log_jobs(
        [
            (
                FakeJob(jid),  # type: ignore
                "deleted",
                None,
            )
            for jid in jids
        ]
    )

    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "deleted" for w in waiters)


@pytest.mark.parametrize("status", ("canceled", "deleted", "exception", "successful"))
async def test_completion_is_terminal(apgdriver: db.Driver, status: str) -> None:
    assert CompletionWatcher(apgdriver)._is_terminal(status)  # type: ignore


# ─────────────────────────────────────────────────────────────────────
# Concurrent _refresh_waiters execution test
# ─────────────────────────────────────────────────────────────────────


async def test_no_concurrent_refresh_waiters(
    apgdriver: db.Driver,
) -> None:
    """
    Multiple _debounced tasks must not call _refresh_waiters() concurrently.

    Regression test for https://github.com/janbjorge/pgqueuer/issues/510

    The bug occurred because _debounced() set debounce_task = None in the
    finally block BEFORE calling _refresh_waiters(). This allowed new
    debounce tasks to be created while a refresh was in progress, leading
    to concurrent database operations on the same connection:

    - InternalClientError: got result for unknown protocol state 3
    - InterfaceError: cannot perform operation: another operation is in progress

    This test tracks how many _debounced tasks attempt to call _refresh_waiters
    concurrently. With the bug, we see overlapping calls.
    """
    watcher = CompletionWatcher(
        apgdriver,
        debounce=timedelta(milliseconds=5),
        refresh_interval=timedelta(seconds=60),
    )

    # Track concurrent refresh attempts (before the internal lock)
    active_refreshes = 0
    max_concurrent_refreshes = 0
    total_refresh_calls = 0
    first_started = asyncio.Event()
    allow_continue = asyncio.Event()

    async def tracking_refresh() -> None:
        nonlocal active_refreshes, max_concurrent_refreshes, total_refresh_calls
        total_refresh_calls += 1
        active_refreshes += 1
        max_concurrent_refreshes = max(max_concurrent_refreshes, active_refreshes)

        if total_refresh_calls == 1:
            first_started.set()
            # Block first refresh to create window for concurrent calls
            await allow_continue.wait()

        active_refreshes -= 1

    # Replace _refresh_waiters to track concurrency
    watcher._refresh_waiters = tracking_refresh  # type: ignore[method-assign]

    # Manually trigger initial refresh
    watcher._schedule_refresh_waiters()

    # Wait for first refresh to start and block
    await asyncio.wait_for(first_started.wait(), timeout=2.0)

    # While first refresh is blocked, rapidly trigger more
    # With the bug, these create new _debounced tasks that call _refresh_waiters
    for _ in range(5):
        watcher._schedule_refresh_waiters()
        await asyncio.sleep(0.01)  # Longer than debounce to trigger new tasks

    # Release the blocked refresh
    allow_continue.set()

    # Let pending tasks complete
    await asyncio.sleep(0.1)

    # The key assertion: only 1 refresh should be active at a time
    # If max > 1, the debounce mechanism failed to prevent concurrent calls
    assert max_concurrent_refreshes == 1, (
        f"Bug reproduced: {max_concurrent_refreshes} concurrent _refresh_waiters() calls "
        f"detected (total: {total_refresh_calls}). Issue #510."
    )
