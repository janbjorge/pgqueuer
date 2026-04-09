import asyncio
import functools
import inspect
from datetime import timedelta
from multiprocessing import Process, Queue as MPQueue
from typing import Awaitable, Callable

import anyio
import pytest

from pgqueuer.db import Driver
from pgqueuer.executors import (
    AbstractEntrypointExecutor,
    EntrypointExecutor,
    EntrypointExecutorParameters,
    is_async_callable,
)
from pgqueuer.models import Context, Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries
from test.helpers import mocked_job

# NOTE: Resources feature branch (pseudo-branch: feature/context-resources)
# Tests updated to explicitly pass a resources mapping to Context to document new API.


class MultiprocessingExecutor(AbstractEntrypointExecutor):
    def __init__(self) -> None:
        self.concurrency_limit = 2
        self.queue: MPQueue[object] = MPQueue()

    async def execute(self, job: Job, context: Context) -> None:
        process = Process(target=self.process_function, args=(job,))
        process.start()
        process.join()

    def process_function(self, job: Job) -> None:
        # Simulate processing and put result in queue
        if job.payload:
            self.queue.put(job.payload)


async def test_entrypoint_executor_async(apgdriver: Driver) -> None:
    result = []

    async def async_function(job: Job) -> None:
        if job.payload:
            result.append(job.payload)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            concurrency_limit=10,
            func=async_function,
        )
    )
    job = mocked_job(payload=b"test_payload")

    await executor.execute(
        job,
        Context(anyio.CancelScope(), resources={"test_key": "async_executor"}),
    )
    assert result == [b"test_payload"]


async def test_entrypoint_executor_async_with_context(apgdriver: Driver) -> None:
    markers: list[str] = []

    async def async_function(job: Job, ctx: Context) -> None:
        marker = ctx.resources.get("marker")
        if marker:
            markers.append(marker)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            concurrency_limit=10,
            func=async_function,
            accepts_context=True,
        )
    )
    job = mocked_job(payload=b"context_payload")
    job_context = Context(anyio.CancelScope(), resources={"marker": "async"})

    await executor.execute(job, job_context)

    assert markers == ["async"]


@pytest.mark.parametrize("accepts_context", [False, True])
def test_sync_entrypoint_raises_type_error(accepts_context: bool) -> None:
    def sync_fn(job: Job) -> None:
        pass

    with pytest.raises(TypeError, match="must be async"):
        EntrypointExecutor(
            EntrypointExecutorParameters(
                concurrency_limit=10,
                func=sync_fn,  # type: ignore[arg-type]
                accepts_context=accepts_context,
            )
        )


async def test_entrypoint_executor_forward_reference_with_flag(apgdriver: Driver) -> None:
    contexts: list[Context] = []

    async def async_function(job: Job, context: "Context") -> None:
        contexts.append(context)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            concurrency_limit=10,
            func=async_function,
            accepts_context=True,
        )
    )
    job = mocked_job(payload=b"context_forward_ref")
    job_context = Context(anyio.CancelScope(), resources={"marker": "forward"})

    assert executor.accepts_context
    assert executor.parameters.accepts_context

    await executor.execute(job, job_context)
    assert contexts and contexts[0] is job_context


async def test_entrypoint_executor_without_context_detection(apgdriver: Driver) -> None:
    calls: list[tuple[Job, bytes | None]] = []

    async def async_function(job: Job, extra: bytes | None = None) -> None:
        calls.append((job, extra))

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            concurrency_limit=10,
            func=async_function,
        )
    )
    job = mocked_job(payload=b"no_context")
    job_context = Context(anyio.CancelScope(), resources={"marker": "unused"})

    assert not executor.accepts_context
    assert not executor.parameters.accepts_context

    await executor.execute(job, job_context)
    assert calls == [(job, None)]


async def test_custom_threading_executor() -> None:
    class ThreadingExecutor(AbstractEntrypointExecutor):
        def __init__(self) -> None:
            self.concurrency_limit = 5
            self.result = list[bytes]()

        async def execute(self, job: Job, context: Context) -> None:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.threaded_function, job)

        def threaded_function(self, job: Job) -> None:
            if job.payload:
                self.result.append(job.payload)

    executor = ThreadingExecutor()
    job = mocked_job(payload=b"thread_payload")

    await executor.execute(
        job,
        Context(anyio.CancelScope(), resources={"test_key": "threading_executor"}),
    )

    assert executor.result == [b"thread_payload"]


async def test_custom_multiprocessing_executor() -> None:
    executor = MultiprocessingExecutor()
    job = mocked_job(payload=b"process_payload")

    await executor.execute(
        job,
        Context(anyio.CancelScope(), resources={"test_key": "multiprocessing_executor"}),
    )
    result = executor.queue.get()

    assert result == b"process_payload"


async def test_queue_manager_with_custom_executor(apgdriver: Driver) -> None:
    qm = QueueManager(connection=apgdriver)
    results = []

    class CustomExecutor(AbstractEntrypointExecutor):
        async def execute(self, job: Job, context: Context) -> None:
            if job.payload:
                results.append(job.payload)

    @qm.entrypoint(
        name="custom_entrypoint",
        executor_factory=CustomExecutor,
    )
    async def entrypoint_function(job: Job) -> None:
        pass  # Not used since executor handles execution

    queries = Queries(apgdriver)
    await queries.enqueue(entrypoint="custom_entrypoint", payload=b"test_data")

    async def run_queue_manager() -> None:
        await qm.run(dequeue_timeout=timedelta(seconds=2), batch_size=1)

    task = asyncio.create_task(run_queue_manager())
    await asyncio.sleep(1)  # Allow some time for the job to be processed
    qm.shutdown.set()  # Signal the queue manager to stop
    await task

    assert results == [b"test_data"]


async def _async_fn(job: Job) -> None:
    await asyncio.sleep(0)


def _sync_fn(job: Job) -> None:
    pass


class _AsyncCallable:
    async def __call__(self, job: Job) -> None:  # pragma: no cover - trivial
        await asyncio.sleep(0)


class _SyncCallable:
    def __call__(self, job: Job) -> None:  # pragma: no cover - trivial
        pass


class _AsyncMethodHolder:
    async def method(self, job: Job) -> None:
        await asyncio.sleep(0)


class _SyncMethodHolder:
    def method(self, job: Job) -> None:
        pass


def _build_basic_cases() -> list[tuple[object, bool, str]]:
    """Build (callable, expected, label) tuples for basic is_async_callable tests."""
    return [
        (_async_fn, True, "async_function"),
        (_sync_fn, False, "sync_function"),
        (functools.partial(_async_fn), True, "partial_async_function"),
        (functools.partial(_sync_fn), False, "partial_sync_function"),
        (_AsyncMethodHolder().method, True, "async_class_method"),
        (_SyncMethodHolder().method, False, "sync_class_method"),
        (_AsyncCallable(), True, "async_callable_instance"),
        (_SyncCallable(), False, "sync_callable_instance"),
    ]


@pytest.mark.parametrize(
    "fn, expected, label",
    _build_basic_cases(),
    ids=[c[2] for c in _build_basic_cases()],
)
def test_is_async_callable_basic(fn: object, expected: bool, label: str) -> None:
    assert is_async_callable(fn) is expected


def _build_multi_level_partial_cases() -> list[tuple[object, bool, str]]:
    """Build multi-level functools.partial chains."""
    async_inst = _AsyncCallable()
    sync_inst = _SyncCallable()

    async def async_base(job: Job) -> None:
        await asyncio.sleep(0)

    def sync_base(job: Job) -> None:
        pass

    def chain3(fn: Callable[..., object]) -> functools.partial[object]:
        return functools.partial(functools.partial(functools.partial(fn)))

    return [
        (chain3(async_inst), True, "partial_chain_async_callable"),
        (chain3(sync_inst), False, "partial_chain_sync_callable"),
        (chain3(async_base), True, "partial_chain_async_function"),
        (chain3(sync_base), False, "partial_chain_sync_function"),
    ]


@pytest.mark.parametrize(
    "fn, expected, label",
    _build_multi_level_partial_cases(),
    ids=[c[2] for c in _build_multi_level_partial_cases()],
)
def test_is_async_callable_multi_level_partial(fn: object, expected: bool, label: str) -> None:
    assert is_async_callable(fn) is expected


def test_is_async_callable_async_decorator_wrapper() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def async_decorator(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        @functools.wraps(f)
        async def wrapper(job: Job) -> None:
            await f(job)

        return wrapper

    assert is_async_callable(async_decorator(base)) is True


def test_is_async_callable_sync_wrapper_returning_coroutine() -> None:
    """Sync def that returns a coroutine object should be classified as sync."""

    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def sync_decorator(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        @functools.wraps(f)
        def wrapper(job: Job) -> Awaitable[None]:
            return f(job)

        return wrapper

    assert is_async_callable(sync_decorator(base)) is False


def test_is_async_callable_deeply_nested_async_wrappers() -> None:
    def make_async_wrapper(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        async def w(job: Job) -> None:
            result = f(job)
            if asyncio.iscoroutine(result):
                await result

        return w

    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    lvl3 = make_async_wrapper(make_async_wrapper(make_async_wrapper(base)))
    assert is_async_callable(lvl3) is True


def test_is_async_callable_deeply_nested_sync_wrappers() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def wrap_sync(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        def w(job: Job) -> Awaitable[None]:
            return f(job)

        return w

    lvl3 = wrap_sync(wrap_sync(wrap_sync(base)))
    assert is_async_callable(lvl3) is False


def test_is_async_callable_sync_dunder_call_returning_coroutine() -> None:
    async def async_inner(job: Job) -> None:
        await asyncio.sleep(0)

    class HybridCallable:
        def __call__(self, job: Job) -> Awaitable[None]:
            return async_inner(job)

    assert is_async_callable(HybridCallable()) is False


def test_is_async_callable_inspect_vs_call_attribute() -> None:
    async_inst = _AsyncCallable()

    # Plain async function: its own object is a coroutine function, its __call__ isn't.
    assert inspect.iscoroutinefunction(_async_fn) is True
    assert inspect.iscoroutinefunction(getattr(_async_fn, "__call__", None)) is False

    # Instance with async __call__: the instance itself is not a coroutine function
    # but its __call__ method is.
    assert inspect.iscoroutinefunction(async_inst) is False
    assert inspect.iscoroutinefunction(getattr(async_inst, "__call__", None)) is True

    # Helper should classify both as async callables.
    assert is_async_callable(_async_fn) is True
    assert is_async_callable(async_inst) is True
