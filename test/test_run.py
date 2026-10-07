from __future__ import annotations

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator, Callable

import pytest

import pgqueuer
from pgqueuer import Job, PgQueuer
from pgqueuer.domain.types import QueueExecutionMode

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="loop signal handlers")

handled: list[int] = []


@asynccontextmanager
async def factory() -> AsyncGenerator[PgQueuer, None]:
    pgq = PgQueuer.in_memory()

    @pgq.entrypoint("work")
    async def work(job: Job) -> None:
        handled.append(job.id)

    await pgq.qm.queries.enqueue(["work"], [None], [0])
    yield pgq


@pytest.mark.parametrize("target", (factory, f"{__name__}:factory"))
async def test_run_drains_a_job_from_factory(target: str | Callable[[], object]) -> None:
    """pgqueuer.run runs a worker like `pgq run`, from a callable or an import path (#459)."""
    handled.clear()
    await asyncio.wait_for(
        pgqueuer.run(target, mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(0.1)),
        timeout=5,
    )
    loop = asyncio.get_running_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)

    assert len(handled) == 1


async def test_run_stops_on_caller_shutdown_without_signal_handlers() -> None:
    """A caller-owned shutdown event stops the worker and leaves signals to the caller."""
    shutdown = asyncio.Event()
    run = asyncio.create_task(pgqueuer.run(factory, shutdown=shutdown))
    await asyncio.sleep(0.2)
    shutdown.set()
    await asyncio.wait_for(run, timeout=5)

    assert not asyncio.get_running_loop().remove_signal_handler(signal.SIGINT)


async def test_run_installs_signal_handlers_by_default() -> None:
    """Without a shutdown event, run() owns SIGINT/SIGTERM like `pgq run`."""
    await asyncio.wait_for(
        pgqueuer.run(factory, mode=QueueExecutionMode.drain, dequeue_timeout=timedelta(0.1)),
        timeout=5,
    )
    loop = asyncio.get_running_loop()

    assert loop.remove_signal_handler(signal.SIGINT)
    assert loop.remove_signal_handler(signal.SIGTERM)
