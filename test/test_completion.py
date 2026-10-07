from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest

from pgqueuer import db
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.domain.types import JOB_STATUS, JobId, QueueEntrypoint, QueueManagerId
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import EntrypointExecutionParameter, Queries
from pgqueuer.types import QueueExecutionMode


async def test_completion_successful(apgdriver: db.Driver) -> None:
    qm = QueueManager(Queries(apgdriver))

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None: ...

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver, queries=Queries(apgdriver)) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "successful" for w in waiters)


async def test_completion_already_successful(apgdriver: db.Driver) -> None:
    qm = QueueManager(Queries(apgdriver))

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None: ...

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver, queries=Queries(apgdriver)) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "successful" for w in waiters)


async def test_completion_exception(apgdriver: db.Driver) -> None:
    qm = QueueManager(Queries(apgdriver))

    @qm.entrypoint("fetch")
    async def fetch(context: Job) -> None:
        raise ValueError

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    await qm.run(mode=QueueExecutionMode.drain)
    async with CompletionWatcher(apgdriver, queries=Queries(apgdriver)) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "exception" for w in waiters)


async def test_for_completion_canceled(apgdriver: db.Driver) -> None:
    qm = QueueManager(Queries(apgdriver))

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)
    await qm.queries.mark_job_as_cancelled(jids)

    async with CompletionWatcher(apgdriver, queries=Queries(apgdriver)) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "canceled" for w in waiters)


async def test_completion_deleted(apgdriver: db.Driver) -> None:
    qm = QueueManager(Queries(apgdriver))

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)

    @dataclass
    class FakeJob:
        id: int

    entries = [(FakeJob(jid), "deleted", None) for jid in jids]
    await qm.queries.log_jobs(entries)  # type: ignore[arg-type]

    async with CompletionWatcher(apgdriver, queries=Queries(apgdriver)) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "deleted" for w in waiters)


@pytest.mark.parametrize(
    ("status", "resolves"),
    (("successful", True), ("failed", False)),
)
async def test_completion_inmemory_log_jobs_notifies(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    status: JOB_STATUS,
    resolves: bool,
) -> None:
    """In-memory log_jobs wakes the watcher without the poll (#901)."""
    await queries.enqueue(["fetch"], [None], [0])
    (job,) = await queries.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("fetch"): EntrypointExecutionParameter(0)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(minutes=10),
    )

    async with CompletionWatcher(
        driver,
        queries=queries,
        refresh_interval=timedelta(minutes=10),
    ) as watcher:
        waiter = watcher.wait_for(job.id)
        await asyncio.sleep(0.1)
        await queries.log_jobs([(job, status, None)])
        await asyncio.sleep(0.1)
        assert waiter.done() is resolves
        await queries.mark_job_as_cancelled([job.id])  # lets exit resolve a pending waiter


async def test_completion_refresh_interval_none_disables_poll(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """refresh_interval=None runs without a poll task (#887)."""
    (jid,) = await queries.enqueue(["fetch"], [None], [0])
    await queries.mark_job_as_cancelled([jid])

    with caplog.at_level(logging.ERROR):
        async with CompletionWatcher(driver, queries=queries, refresh_interval=None) as watcher:
            assert await watcher.wait_for(jid) == "canceled"

    assert "Unhandled exception" not in caplog.text


async def test_completion_refresh_interval_polls_for_lost_notify(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
) -> None:
    """A set refresh_interval resolves a job whose NOTIFY never came."""
    (jid,) = await queries.enqueue(["fetch"], [None], [0])

    async with CompletionWatcher(
        driver,
        queries=queries,
        refresh_interval=timedelta(milliseconds=50),
    ) as watcher:
        waiter = watcher.wait_for(jid)
        await asyncio.sleep(0.1)
        await queries.mark_job_as_cancelled([jid])  # emits no table_changed
        assert await asyncio.wait_for(waiter, timeout=2) == "canceled"


async def test_completion_exit_with_cancelled_waiter(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
) -> None:
    """Cancelling a pending waiter does not make exit raise (#884)."""
    done_id, pending_id = await queries.enqueue(["fetch", "fetch"], [None, None], [0, 0])
    await queries.mark_job_as_cancelled([done_id])

    async with CompletionWatcher(driver, queries=queries) as watcher:
        waiters = [watcher.wait_for(done_id), watcher.wait_for(pending_id)]
        done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for fut in pending:
            fut.cancel()

    assert next(iter(done)).result() == "canceled"


async def test_completion_exit_waits_for_uncancelled_waiter(apgdriver: db.Driver) -> None:
    """Exit still waits for the waiters that were not cancelled."""
    queries = Queries(apgdriver)
    cancelled_id, waited_id = await queries.enqueue(["fetch", "fetch"], [None, None], [0, 0])

    async def cancel_later() -> None:
        await asyncio.sleep(0.1)
        await queries.mark_job_as_cancelled([waited_id])

    async with CompletionWatcher(apgdriver, queries=queries) as watcher:
        watcher.wait_for(cancelled_id).cancel()
        waiter = watcher.wait_for(waited_id)
        canceller = asyncio.create_task(cancel_later())

    assert waiter.result() == "canceled"
    await canceller


async def test_completion_exit_polls_for_lost_notify(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
) -> None:
    """Exit keeps polling until a job whose NOTIFY never came resolves (#885)."""
    (jid,) = await queries.enqueue(["fetch"], [None], [0])

    async def cancel_later() -> None:
        await asyncio.sleep(0.2)
        await queries.mark_job_as_cancelled([jid])  # emits no table_changed

    async def watch() -> asyncio.Future[JOB_STATUS]:
        async with CompletionWatcher(
            driver,
            queries=queries,
            refresh_interval=timedelta(milliseconds=50),
        ) as watcher:
            waiter = watcher.wait_for(jid)
            canceller = asyncio.create_task(cancel_later())
        await canceller
        return waiter

    waiter = await asyncio.wait_for(watch(), timeout=2)
    assert waiter.result() == "canceled"


@pytest.mark.parametrize("error", (ValueError, asyncio.CancelledError))
async def test_completion_exit_on_error_cancels_waiters(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    error: type[BaseException],
) -> None:
    """An error in the body cancels pending waiters instead of waiting on them (#885)."""
    (jid,) = await queries.enqueue(["fetch"], [None], [0])

    async def watch() -> asyncio.Future[JOB_STATUS]:
        with pytest.raises(error):
            async with CompletionWatcher(driver, queries=queries) as watcher:
                waiter = watcher.wait_for(jid)
                raise error
        return waiter

    task = asyncio.create_task(watch())
    done, _ = await asyncio.wait({task}, timeout=2)
    if task not in done:
        task.cancel()
        pytest.fail("exit hung on a pending waiter")
    assert task.result().cancelled()


async def test_debounce_runs_one_query_at_a_time(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NOTIFYs during a slow status query coalesce into one follow-up query (#900)."""
    calls = 0
    job_status = queries.job_status

    async def slow_job_status(ids: list[JobId]) -> list[tuple[JobId, JOB_STATUS]]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.2)
        return await job_status(ids)

    monkeypatch.setattr(queries, "job_status", slow_job_status)

    async with CompletionWatcher(
        driver,
        queries=queries,
        refresh_interval=None,
        debounce=timedelta(milliseconds=10),
    ):
        for _ in range(20):
            await queries.emit_table_changed("update")
            await asyncio.sleep(0.02)

    assert calls <= 3


async def test_debounce_refreshes_after_notify_during_query(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A NOTIFY that lands during a status query still gets its own refresh."""
    (jid,) = await queries.enqueue(["fetch"], [None], [0])
    job_status = queries.job_status

    async def stale_job_status(ids: list[JobId]) -> list[tuple[JobId, JOB_STATUS]]:
        snapshot = await job_status(ids)
        await asyncio.sleep(0.2)
        return snapshot

    monkeypatch.setattr(queries, "job_status", stale_job_status)

    async with CompletionWatcher(
        driver,
        queries=queries,
        refresh_interval=None,
        debounce=timedelta(milliseconds=10),
    ) as watcher:
        waiter = watcher.wait_for(jid)
        await asyncio.sleep(0.05)
        await queries.mark_job_as_cancelled([jid])
        await queries.emit_table_changed("delete")
        assert await asyncio.wait_for(waiter, timeout=2) == "canceled"


async def test_completion_exit_removes_listener(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A closed watcher stops reacting to NOTIFYs (#886)."""
    for _ in range(3):
        async with CompletionWatcher(driver, queries=queries, refresh_interval=None):
            pass

    calls = 0

    async def counting_job_status(ids: list[JobId]) -> list[tuple[JobId, JOB_STATUS]]:
        nonlocal calls
        calls += 1
        return []

    monkeypatch.setattr(queries, "job_status", counting_job_status)
    await queries.emit_table_changed("update")
    await asyncio.sleep(0.1)

    assert calls == 0


async def test_completion_exit_keeps_other_watchers_listening(
    driver: InMemoryDriver,
    queries: InMemoryQueries,
) -> None:
    """Closing one watcher does not unhook another that is still open."""
    await queries.enqueue(["fetch"], [None], [0])
    (job,) = await queries.dequeue(
        batch_size=1,
        entrypoints={QueueEntrypoint("fetch"): EntrypointExecutionParameter(0)},
        queue_manager_id=QueueManagerId(uuid.uuid4()),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(minutes=10),
    )

    async with CompletionWatcher(driver, queries=queries, refresh_interval=None) as open_watcher:
        waiter = open_watcher.wait_for(job.id)
        async with CompletionWatcher(driver, queries=queries, refresh_interval=None):
            pass
        await asyncio.sleep(0.1)
        await queries.log_jobs([(job, "successful", None)])
        assert await asyncio.wait_for(waiter, timeout=2) == "successful"


@pytest.mark.parametrize("status", ("canceled", "deleted", "exception", "successful"))
async def test_completion_is_terminal(apgdriver: db.Driver, status: JOB_STATUS) -> None:
    assert CompletionWatcher(apgdriver, queries=Queries(apgdriver))._is_terminal(status)


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
        queries=Queries(apgdriver),
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
        queries=Queries(apgdriver),
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
