"""Tests for the factory -> supervisor -> orchestrator chain.

The factory contract requires an async context manager (AsyncContextManager).
Bare awaitables and sync context managers are rejected with clear migration
instructions.
"""

from __future__ import annotations

import asyncio
import functools
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from typing import AsyncGenerator, Generator

import async_timeout
import pytest

from pgqueuer.adapters.cli import supervisor
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.qm import QueueManager
from pgqueuer.core.sm import SchedulerManager
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import QueueExecutionMode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pgqueuer() -> PgQueuer:
    return PgQueuer.in_memory()


def _make_queue_manager() -> QueueManager:
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver=driver)
    return QueueManager(connection=driver, queries=queries)


def _make_scheduler_manager() -> SchedulerManager:
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver=driver)
    return SchedulerManager(connection=driver, queries=queries)


# ---------------------------------------------------------------------------
# Async CM factory x all 3 manager types
# ---------------------------------------------------------------------------


async def test_asynccm_factory_pgqueuer() -> None:
    pgq = _make_pgqueuer()
    teardown_called = False

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        nonlocal teardown_called
        yield pgq
        teardown_called = True

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task
    assert teardown_called, "Async CM teardown must execute on shutdown"


async def test_asynccm_factory_queue_manager() -> None:
    qm = _make_queue_manager()
    teardown_called = False

    @asynccontextmanager
    async def factory() -> AsyncGenerator[QueueManager, None]:
        nonlocal teardown_called
        yield qm
        teardown_called = True

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task
    assert teardown_called


async def test_asynccm_factory_scheduler_manager() -> None:
    sm = _make_scheduler_manager()
    teardown_called = False

    @asynccontextmanager
    async def factory() -> AsyncGenerator[SchedulerManager, None]:
        nonlocal teardown_called
        yield sm
        teardown_called = True

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task
    assert teardown_called


# ---------------------------------------------------------------------------
# Rejected factory types
# ---------------------------------------------------------------------------


async def test_coroutine_factory_rejected_with_migration() -> None:
    async def factory() -> PgQueuer:
        return _make_pgqueuer()

    with pytest.raises(TypeError, match="AsyncContextManager") as exc_info:
        await supervisor.runit(
            factory=factory,  # type: ignore[arg-type]
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg
    assert "yield" in msg


async def test_sync_cm_factory_rejected_with_migration() -> None:
    @contextmanager
    def factory() -> Generator[PgQueuer, None, None]:
        yield _make_pgqueuer()

    with pytest.raises(TypeError, match="AsyncContextManager") as exc_info:
        await supervisor.runit(
            factory=factory,  # type: ignore[arg-type]
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg


async def test_arbitrary_return_rejected_with_migration() -> None:
    def factory() -> str:
        return "not a manager"

    with pytest.raises(TypeError, match="AsyncContextManager") as exc_info:
        await supervisor.runit(
            factory=factory,  # type: ignore[arg-type]
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    msg = str(exc_info.value)
    assert "@asynccontextmanager" in msg


# ---------------------------------------------------------------------------
# Shutdown injection
# ---------------------------------------------------------------------------


async def test_shutdown_injected_into_pgqueuer() -> None:
    pgq = _make_pgqueuer()
    shutdown = asyncio.Event()

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        yield pgq

    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    assert pgq.shutdown is shutdown
    assert pgq.qm.shutdown is shutdown
    assert pgq.sm.shutdown is shutdown
    shutdown.set()
    async with async_timeout.timeout(2):
        await task


async def test_shutdown_injected_into_queue_manager() -> None:
    qm = _make_queue_manager()
    shutdown = asyncio.Event()

    @asynccontextmanager
    async def factory() -> AsyncGenerator[QueueManager, None]:
        yield qm

    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    assert qm.shutdown is shutdown
    shutdown.set()
    async with async_timeout.timeout(2):
        await task


async def test_shutdown_injected_into_scheduler_manager() -> None:
    sm = _make_scheduler_manager()
    shutdown = asyncio.Event()

    @asynccontextmanager
    async def factory() -> AsyncGenerator[SchedulerManager, None]:
        yield sm

    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    assert sm.shutdown is shutdown
    shutdown.set()
    async with async_timeout.timeout(2):
        await task


# ---------------------------------------------------------------------------
# Error scenarios
# ---------------------------------------------------------------------------


async def test_asynccm_factory_setup_raises_propagates() -> None:
    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        raise RuntimeError("setup failed")
        yield  # unreachable, needed for generator

    with pytest.raises(RuntimeError, match="setup failed"):
        await supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )


async def test_asynccm_teardown_error_propagates() -> None:
    pgq = _make_pgqueuer()

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        yield pgq
        raise RuntimeError("teardown boom")

    with pytest.raises(RuntimeError, match="teardown boom"):
        await supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.drain,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )


async def test_factory_raises_restart_on_failure_retries() -> None:
    call_count = 0

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("transient error")
        yield  # unreachable, needed for generator

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0.05),
            restart_on_failure=True,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.3)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task
    assert call_count >= 2, f"Expected >=2 factory calls, got {call_count}"


# ---------------------------------------------------------------------------
# run_manager dispatch
# ---------------------------------------------------------------------------


async def test_run_manager_dispatches_to_queue_manager() -> None:
    qm = _make_queue_manager()
    qm.shutdown.set()

    original_run = qm.run
    called_with: dict = {}

    async def spy_run(**kwargs: object) -> None:
        called_with.update(kwargs)
        return await original_run(**kwargs)  # type: ignore[arg-type]

    qm.run = spy_run  # type: ignore[assignment]

    await supervisor.run_manager(
        qm,
        dequeue_timeout=timedelta(seconds=5),
        batch_size=7,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        shutdown_on_listener_failure=True,
    )
    assert called_with["dequeue_timeout"] == timedelta(seconds=5)
    assert called_with["batch_size"] == 7
    assert called_with["mode"] is QueueExecutionMode.drain
    assert called_with["max_concurrent_tasks"] == 100
    assert called_with["shutdown_on_listener_failure"] is True


async def test_run_manager_dispatches_to_scheduler_manager() -> None:
    sm = _make_scheduler_manager()
    sm.shutdown.set()

    called = False
    original_run = sm.run

    async def spy_run() -> None:
        nonlocal called
        called = True
        return await original_run()

    sm.run = spy_run  # type: ignore[method-assign]

    await supervisor.run_manager(
        sm,
        dequeue_timeout=timedelta(seconds=1),
        batch_size=10,
        mode=QueueExecutionMode.continuous,
        max_concurrent_tasks=None,
        shutdown_on_listener_failure=False,
    )
    assert called


async def test_run_manager_dispatches_to_pgqueuer() -> None:
    pgq = _make_pgqueuer()
    pgq.shutdown.set()

    called_with: dict = {}
    original_run = pgq.run

    async def spy_run(**kwargs: object) -> None:
        called_with.update(kwargs)
        return await original_run(**kwargs)  # type: ignore[arg-type]

    pgq.run = spy_run  # type: ignore[assignment]

    await supervisor.run_manager(
        pgq,
        dequeue_timeout=timedelta(seconds=3),
        batch_size=5,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=50,
        shutdown_on_listener_failure=True,
    )
    assert called_with["dequeue_timeout"] == timedelta(seconds=3)
    assert called_with["batch_size"] == 5
    assert called_with["mode"] is QueueExecutionMode.drain
    assert called_with["max_concurrent_tasks"] == 50
    assert called_with["shutdown_on_listener_failure"] is True


# ---------------------------------------------------------------------------
# End-to-end job processing
# ---------------------------------------------------------------------------


async def test_pgqueuer_processes_job_via_supervisor() -> None:
    processed_jobs: list[int] = []
    pgq = _make_pgqueuer()

    @pgq.entrypoint("test_job")
    async def handler(job: Job) -> None:
        processed_jobs.append(int(job.id))

    await pgq.qm.queries.enqueue("test_job", b"hello", priority=0)

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        yield pgq

    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.drain,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )

    async with async_timeout.timeout(5):
        await task

    assert len(processed_jobs) == 1


async def test_queue_manager_processes_job_via_supervisor() -> None:
    processed_jobs: list[int] = []
    qm = _make_queue_manager()

    @qm.entrypoint("qm_job")
    async def handler(job: Job) -> None:
        processed_jobs.append(int(job.id))

    await qm.queries.enqueue("qm_job", b"payload", priority=0)

    @asynccontextmanager
    async def factory() -> AsyncGenerator[QueueManager, None]:
        yield qm

    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=asyncio.Event(),
            mode=QueueExecutionMode.drain,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )

    async with async_timeout.timeout(5):
        await task

    assert len(processed_jobs) == 1


# ---------------------------------------------------------------------------
# Factory fresh instance per restart cycle
# ---------------------------------------------------------------------------


async def test_factory_called_each_restart_cycle() -> None:
    instances: list[PgQueuer] = []
    call_count = 0

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        nonlocal call_count
        call_count += 1
        pgq = _make_pgqueuer()
        instances.append(pgq)
        if call_count <= 2:
            raise RuntimeError("transient")
        yield pgq

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0.05),
            restart_on_failure=True,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.5)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task

    assert call_count >= 3, f"Expected >=3 cycles, got {call_count}"
    assert len({id(p) for p in instances}) == len(instances)


# ---------------------------------------------------------------------------
# Extra args passthrough (-- args forwarded to factory)
# ---------------------------------------------------------------------------


async def test_extra_args_forwarded_to_factory() -> None:
    received_args: list[list[str]] = []

    @asynccontextmanager
    async def factory(args: list[str]) -> AsyncGenerator[PgQueuer, None]:
        received_args.append(args)
        yield _make_pgqueuer()

    bound = functools.partial(factory, ["--db-url", "postgres://localhost", "task_a,task_b"])

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=bound,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task

    assert len(received_args) == 1
    assert received_args[0] == ["--db-url", "postgres://localhost", "task_a,task_b"]


async def test_no_extra_args_factory_stays_zero_arg() -> None:
    called = False

    @asynccontextmanager
    async def factory() -> AsyncGenerator[PgQueuer, None]:
        nonlocal called
        called = True
        yield _make_pgqueuer()

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        supervisor.runit(
            factory=factory,
            dequeue_timeout=timedelta(seconds=1),
            batch_size=10,
            restart_delay=timedelta(seconds=0),
            restart_on_failure=False,
            shutdown=shutdown,
            mode=QueueExecutionMode.continuous,
            max_concurrent_tasks=None,
            shutdown_on_listener_failure=False,
        )
    )
    await asyncio.sleep(0.1)
    shutdown.set()
    async with async_timeout.timeout(2):
        await task

    assert called


def test_cli_run_forwards_extra_args_via_typer() -> None:
    """Verify the CLI plumbing: args after -- reach the factory_args parameter."""
    from typer.testing import CliRunner

    from pgqueuer.adapters.cli.cli import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "nonexistent_module:fn", "--batch-size", "5", "--", "arg1", "--flag"],
    )
    assert result.exit_code != 0
    assert "nonexistent_module" in (result.output + str(result.exception))


def test_cli_run_rejects_typo_before_separator() -> None:
    """Typos in pgq options before -- must still be caught."""
    from typer.testing import CliRunner

    from pgqueuer.adapters.cli.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["run", "mod:fn", "--btach-size", "5"])
    assert result.exit_code != 0
    assert "No such option" in result.output or "btach-size" in result.output
