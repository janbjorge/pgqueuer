"""Tests for the factory -> supervisor -> orchestrator chain.

The factory contract requires an async context manager (AsyncContextManager).
Bare awaitables and sync context managers are rejected with clear migration
instructions.

Covers:
  - Async CM factory x all 3 manager types (PgQueuer, QueueManager, SchedulerManager)
  - Teardown execution on shutdown
  - Shutdown event injection into all manager variants
  - Rejected factory types (coroutine, sync CM, arbitrary) with migration messages
  - restart_on_failure cycling
  - run_manager dispatch to the correct .run() signature
  - End-to-end job processing through the full chain
  - Fresh factory instance per restart cycle
  - validate_factory_result unit tests
  - load_factory string resolution
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from typing import AsyncGenerator, Generator

import async_timeout
import pytest

from pgqueuer.adapters.cli import factories, supervisor
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.qm import QueueManager
from pgqueuer.core.sm import SchedulerManager
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import QueueExecutionMode

# ---------------------------------------------------------------------------
# Helpers — build managers backed by the in-memory adapter
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
# Async CM factory x all 3 manager types — the only supported convention
# ---------------------------------------------------------------------------


class TestAsyncContextManagerFactory:
    async def test_asynccm_pgqueuer(self) -> None:
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

    async def test_asynccm_queue_manager(self) -> None:
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

    async def test_asynccm_scheduler_manager(self) -> None:
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
# Rejected factory types — must give clear migration instructions
# ---------------------------------------------------------------------------


class TestRejectedFactoryTypes:
    async def test_coroutine_factory_rejected_with_migration(self) -> None:
        """Bare 'async def' that returns a manager is no longer accepted."""

        async def factory() -> PgQueuer:
            return _make_pgqueuer()

        with pytest.raises(TypeError, match="returned a coroutine") as exc_info:
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

    async def test_sync_cm_factory_rejected_with_migration(self) -> None:
        """Sync @contextmanager factory is no longer accepted."""

        @contextmanager
        def factory() -> Generator[PgQueuer, None, None]:
            yield _make_pgqueuer()

        with pytest.raises(TypeError, match="synchronous") as exc_info:
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

    async def test_arbitrary_return_rejected_with_migration(self) -> None:
        """Factory returning a plain object is rejected."""

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
# Shutdown injection — supervisor must wire shutdown into every manager type
# ---------------------------------------------------------------------------


class TestShutdownInjection:
    async def test_shutdown_injected_into_pgqueuer(self) -> None:
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

    async def test_shutdown_injected_into_queue_manager(self) -> None:
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

    async def test_shutdown_injected_into_scheduler_manager(self) -> None:
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


class TestFactoryErrors:
    async def test_asynccm_factory_setup_raises_propagates(self) -> None:
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

    async def test_asynccm_teardown_error_propagates(self) -> None:
        pgq = _make_pgqueuer()

        @asynccontextmanager
        async def factory() -> AsyncGenerator[PgQueuer, None]:
            yield pgq
            raise RuntimeError("teardown boom")

        shutdown = asyncio.Event()

        # Use drain mode with empty queue so run() exits quickly,
        # then the CM __aexit__ fires and raises.
        with pytest.raises(RuntimeError, match="teardown boom"):
            await supervisor.runit(
                factory=factory,
                dequeue_timeout=timedelta(seconds=1),
                batch_size=10,
                restart_delay=timedelta(seconds=0),
                restart_on_failure=False,
                shutdown=shutdown,
                mode=QueueExecutionMode.drain,
                max_concurrent_tasks=None,
                shutdown_on_listener_failure=False,
            )

    async def test_factory_raises_restart_on_failure_retries(self) -> None:
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
# validate_factory_result — unit-level tests
# ---------------------------------------------------------------------------


class TestValidateFactoryResult:
    def test_accepts_async_cm(self) -> None:
        @asynccontextmanager
        async def make() -> AsyncGenerator[str, None]:
            yield "value"

        result = factories.validate_factory_result(make())
        assert hasattr(result, "__aenter__")

    async def test_rejects_coroutine(self) -> None:
        async def make() -> str:
            return "value"

        with pytest.raises(TypeError, match="returned a coroutine"):
            factories.validate_factory_result(make())

    def test_rejects_sync_cm(self) -> None:
        @contextmanager
        def make() -> Generator[str, None, None]:
            yield "value"

        with pytest.raises(TypeError, match="synchronous"):
            factories.validate_factory_result(make())

    def test_rejects_arbitrary(self) -> None:
        with pytest.raises(TypeError, match="AsyncContextManager"):
            factories.validate_factory_result(42)


# ---------------------------------------------------------------------------
# run_manager dispatch — ensures the correct .run() signature is called
# ---------------------------------------------------------------------------


class TestRunManagerDispatch:
    async def test_dispatches_to_queue_manager_run(self) -> None:
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

    async def test_dispatches_to_scheduler_manager_run(self) -> None:
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

    async def test_dispatches_to_pgqueuer_run(self) -> None:
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
# End-to-end: factory produces manager, supervisor runs it, job gets processed
# ---------------------------------------------------------------------------


class TestEndToEndJobProcessing:
    async def test_pgqueuer_processes_job_via_supervisor(self) -> None:
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

    async def test_queue_manager_processes_job_via_supervisor(self) -> None:
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


class TestFactoryPerCycle:
    async def test_factory_called_each_restart_cycle(self) -> None:
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
# load_factory — string path resolution
# ---------------------------------------------------------------------------


class TestLoadFactory:
    def test_callable_passthrough(self) -> None:
        def my_fn() -> None:
            pass

        assert factories.load_factory(my_fn) is my_fn

    def test_module_colon_function(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unittest.mock import Mock

        sentinel = Mock()
        mock_module = Mock(spec_set=["my_factory"])
        mock_module.my_factory = sentinel

        monkeypatch.setattr(
            "importlib.import_module",
            lambda name: mock_module
            if name == "myapp.workers"
            else (_ for _ in ()).throw(ImportError(name)),
        )

        result = factories.load_factory("myapp.workers:my_factory")
        assert result is sentinel

    def test_nonexistent_module_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "importlib.import_module",
            lambda name: (_ for _ in ()).throw(ImportError(f"No module named '{name}'")),
        )
        with pytest.raises(ImportError, match="No module named 'ghost'"):
            factories.load_factory("ghost:fn")

    def test_nonexistent_attr_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unittest.mock import Mock

        mock_module = Mock(spec_set=[])
        monkeypatch.setattr("importlib.import_module", lambda name: mock_module)
        with pytest.raises(AttributeError):
            factories.load_factory("mod:missing_fn")
