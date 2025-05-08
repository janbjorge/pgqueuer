from __future__ import annotations

from dataclasses import dataclass

import pytest

from pgqueuer import db
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.types import QueueExecutionMode
from pgqueuer.wait_for_completion import CompletionWatcher


async def test_wait_for_completion_successful(apgdriver: db.Driver) -> None:
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


async def test_wait_for_completion_already_successful(apgdriver: db.Driver) -> None:
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


async def test_wait_for_completion_exception(apgdriver: db.Driver) -> None:
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


async def test_wait_for_completion_canceled(apgdriver: db.Driver) -> None:
    qm = QueueManager(apgdriver)

    N = 25
    jids = await qm.queries.enqueue(["fetch"] * N, [None] * N, [0] * N)
    await qm.queries.mark_job_as_cancelled(jids)

    async with CompletionWatcher(apgdriver) as grp:
        waiters = [grp.wait_for(jid) for jid in jids]

    assert len(waiters) == N
    assert all(w.result() == "canceled" for w in waiters)


async def test_wait_for_completion_deleted(apgdriver: db.Driver) -> None:
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
async def test_wait_for_completion_is_terminal(apgdriver: db.Driver, status: str) -> None:
    assert CompletionWatcher(apgdriver)._is_terminal(status)  # type: ignore
