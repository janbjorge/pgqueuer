"""A worker's first listener health probe must not race its own LISTEN (#1118).

``run`` starts the health-check task before it awaits ``add_listener``, so the first
probe's NOTIFY goes out as soon as ``add_listener`` yields. PostgreSQL drops a NOTIFY
sent before the LISTEN commits, and the probe times out on a healthy listener. On a
pool with no idle connection this happens about one start in ten. ``LateListen``
makes it deterministic: it holds LISTEN until a NOTIFY has gone out, or GATE passes.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta

import asyncpg
import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.db import AsyncpgPoolDriver, Driver
from pgqueuer.domain.models import HealthCheckEvent
from pgqueuer.errors import FailingListenerError
from pgqueuer.models import Job
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.driver import NotifyCallback
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

pytestmark = pytest.mark.xfail(
    reason="#1118: the first health probe is sent before LISTEN is in place", strict=True
)

# Long enough for a premature NOTIFY to go out first; after the fix only startup pays it.
GATE = timedelta(milliseconds=500)
# Stands in for the 10 s default, so a lost probe fails within the test timeout.
PROBE = timedelta(seconds=1)


@dataclass
class LateListen:
    """Hold ``add_listener`` back and record when LISTEN and each NOTIFY complete."""

    driver: Driver
    order: list[str] = field(default_factory=list)
    notified: asyncio.Event = field(default_factory=asyncio.Event)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_notify, real_add_listener = self.driver.notify, self.driver.add_listener

        async def notify(channel: str, payload: str) -> None:
            self.notified.set()
            await real_notify(channel, payload)
            self.order.append("notify")

        async def add_listener(channel: str, callback: NotifyCallback) -> None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.notified.wait(), GATE.total_seconds())
            await real_add_listener(channel, callback)
            self.order.append("listen")

        monkeypatch.setattr(self.driver, "notify", notify)
        monkeypatch.setattr(self.driver, "add_listener", add_listener)


@dataclass
class Worker:
    qm: QueueManager
    late: LateListen
    probes: list[str] = field(default_factory=list)
    first_probe_done: asyncio.Event = field(default_factory=asyncio.Event)


@pytest.fixture(params=["asyncpg-pool", "inmemory"])
async def worker(
    request: pytest.FixtureRequest,
    dsn_or_none: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[Worker]:
    async with contextlib.AsyncExitStack() as stack:
        queries: RepositoryPort
        driver: Driver
        if request.param == "inmemory":
            driver = InMemoryDriver()
            queries = InMemoryQueries(driver=driver)
        else:
            assert dsn_or_none is not None
            pool = await stack.enter_async_context(
                asyncpg.create_pool(dsn_or_none, min_size=1, max_size=4)
            )
            driver = AsyncpgPoolDriver(pool)
            queries = Queries(driver)

        late = LateListen(driver)
        late.install(monkeypatch)
        qm = QueueManager(queries)

        @qm.entrypoint("noop")
        async def noop(job: Job) -> None: ...

        state = Worker(qm, late)
        real_listener_healthy = qm.listener_healthy

        async def short_probe(timeout: timedelta = PROBE) -> HealthCheckEvent:
            try:
                event = await real_listener_healthy(timeout=PROBE)
            except FailingListenerError:
                state.probes.append("lost")
                raise
            else:
                state.probes.append("answered")
                return event
            finally:
                state.first_probe_done.set()

        monkeypatch.setattr(qm, "listener_healthy", short_probe)
        yield state


@pytest.fixture
def dsn_or_none(request: pytest.FixtureRequest) -> str | None:
    # Sync on purpose: getfixturevalue cannot set up an async fixture from a running loop.
    if request.node.callspec.params["worker"] == "inmemory":
        return None
    dsn: str = request.getfixturevalue("dsn")
    return dsn


async def test_first_probe_waits_for_listen(worker: Worker) -> None:
    """LISTEN is in place before the first probe's NOTIFY, and that probe is answered."""
    run = asyncio.create_task(worker.qm.run(dequeue_timeout=timedelta(seconds=30)))
    await asyncio.wait_for(worker.first_probe_done.wait(), (GATE + 3 * PROBE).total_seconds())
    worker.qm.shutdown.set()
    await asyncio.wait_for(run, 10)

    assert worker.late.order[:2] == ["listen", "notify"]
    assert worker.probes[:1] == ["answered"]


@pytest.mark.parametrize("worker", ["asyncpg-pool"], indirect=True)
async def test_healthy_worker_survives_startup(worker: Worker) -> None:
    """With shutdown_on_listener_failure, a healthy worker keeps running past its first probe."""
    run = asyncio.create_task(
        worker.qm.run(dequeue_timeout=timedelta(seconds=30), shutdown_on_listener_failure=True)
    )
    done, _ = await asyncio.wait({run}, timeout=(GATE + 3 * PROBE).total_seconds())
    assert not done, run.exception()

    worker.qm.shutdown.set()
    await asyncio.wait_for(run, 10)
