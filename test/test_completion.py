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
# Debounce-specific tests
# ─────────────────────────────────────────────────────────────────────


async def test_debounce_coalesces_burst(
    monkeypatch: pytest.MonkeyPatch,
    apgdriver: db.Driver,
) -> None:
    """
    Rapidly trigger `_schedule_on_change` many times inside a single debounce
    window and assert that the expensive `_on_change` body executes only once.
    """
    watcher = CompletionWatcher(
        apgdriver,
        debounce=timedelta(milliseconds=20),
    )
    await watcher.__aenter__()

    call_count = 0

    async def fake_refresh_waiters() -> None:
        nonlocal call_count
        call_count += 1

    # Patch before scheduling so the debounced coroutine sees the stub
    monkeypatch.setattr(watcher, "_refresh_waiters", fake_refresh_waiters)

    for _ in range(10):
        watcher._schedule_refresh_waiters()  # burst of triggers

    await asyncio.sleep(0.05)  # > debounce window
    assert call_count == 1

    await watcher.__aexit__(None, None, None)


async def test_debounce_allows_separate_windows(
    monkeypatch: pytest.MonkeyPatch,
    apgdriver: db.Driver,
) -> None:
    """
    Ensure that events separated by more than the debounce interval result in
    multiple `_on_change` executions.
    """
    watcher = CompletionWatcher(
        apgdriver,
        debounce=timedelta(milliseconds=20),
    )
    await watcher.__aenter__()

    call_count = 0

    async def fake_refresh_waiters() -> None:
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(watcher, "_refresh_waiters", fake_refresh_waiters)

    watcher._schedule_refresh_waiters()
    await asyncio.sleep(0.05)  # wait past first debounce firing

    watcher._schedule_refresh_waiters()
    await asyncio.sleep(0.05)  # wait past second debounce firing

    assert call_count == 2

    await watcher.__aexit__(None, None, None)
