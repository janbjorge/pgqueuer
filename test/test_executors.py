import asyncio
import functools
import inspect
from datetime import timedelta
from multiprocessing import Process, Queue as MPQueue
from typing import Awaitable, Callable

import anyio
import pytest
from helpers import mocked_job

from pgqueuer.db import Driver
from pgqueuer.errors import MaxRetriesExceeded, MaxTimeExceeded
from pgqueuer.executors import (
    AbstractEntrypointExecutor,
    EntrypointExecutor,
    EntrypointExecutorParameters,
    RetryWithBackoffEntrypointExecutor,
    is_async_callable,
)
from pgqueuer.helpers import timer
from pgqueuer.models import Channel, Context, Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

# NOTE: Resources feature branch (pseudo-branch: feature/context-resources)
# Tests updated to explicitly pass a resources mapping to Context to document new API.


class MultiprocessingExecutor(AbstractEntrypointExecutor):
    def __init__(self) -> None:
        self.requests_per_second = 5
        self.retry_timer = timedelta(seconds=10)
        self.serialized_dispatch = False
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


async def test_entrypoint_executor_sync(apgdriver: Driver) -> None:
    result = []

    def sync_function(job: Job) -> None:
        if job.payload:
            result.append(job.payload)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=sync_function,
        )
    )
    job = mocked_job(payload=b"test_payload")

    await executor.execute(
        job,
        Context(anyio.CancelScope(), resources={"test_key": "sync_executor"}),
    )

    assert result == [b"test_payload"]


async def test_entrypoint_executor_async(apgdriver: Driver) -> None:
    result = []

    async def async_function(job: Job) -> None:
        if job.payload:
            result.append(job.payload)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=async_function,
        )
    )
    job = mocked_job(payload=b"test_payload")

    await executor.execute(
        job,
        Context(anyio.CancelScope(), resources={"test_key": "async_executor"}),
    )
    assert result == [b"test_payload"]


async def test_entrypoint_executor_sync_with_context(apgdriver: Driver) -> None:
    contexts: list[Context] = []

    def sync_function(job: Job, ctx: Context) -> None:
        contexts.append(ctx)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=sync_function,
            accepts_context=True,
        )
    )
    job = mocked_job(payload=b"context_payload")
    job_context = Context(anyio.CancelScope(), resources={"marker": "sync"})

    await executor.execute(job, job_context)

    assert contexts and contexts[0] is job_context


async def test_entrypoint_executor_async_with_context(apgdriver: Driver) -> None:
    markers: list[str] = []

    async def async_function(job: Job, ctx: Context) -> None:
        marker = ctx.resources.get("marker")
        if marker:
            markers.append(marker)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=async_function,
            accepts_context=True,
        )
    )
    job = mocked_job(payload=b"context_payload")
    job_context = Context(anyio.CancelScope(), resources={"marker": "async"})

    await executor.execute(job, job_context)

    assert markers == ["async"]


async def test_entrypoint_executor_forward_reference_with_flag(apgdriver: Driver) -> None:
    contexts: list[Context] = []

    def sync_function(job: Job, context: "Context") -> None:
        contexts.append(context)

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=sync_function,
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

    def sync_function(job: Job, extra: bytes | None = None) -> None:
        calls.append((job, extra))

    executor = EntrypointExecutor(
        EntrypointExecutorParameters(
            channel=Channel("foo"),
            concurrency_limit=10,
            connection=apgdriver,
            queries=Queries(apgdriver),
            requests_per_second=float("+inf"),
            retry_timer=timedelta(seconds=300),
            serialized_dispatch=False,
            shutdown=asyncio.Event(),
            func=sync_function,
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
            self.requests_per_second = 10
            self.retry_timer = timedelta(seconds=5)
            self.serialized_dispatch = False
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
    def entrypoint_function(job: Job) -> None:
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


async def async_function(job: Job) -> None:
    await asyncio.sleep(0)


def sync_function(job: Job) -> None:
    pass


def test_is_async_callable_with_async_function() -> None:
    assert is_async_callable(async_function) is True


def test_is_async_callable_with_sync_function() -> None:
    assert is_async_callable(sync_function) is False


def test_is_async_callable_with_partial_async_function() -> None:
    partial_async = functools.partial(async_function)
    assert is_async_callable(partial_async) is True


def test_is_async_callable_with_partial_sync_function() -> None:
    partial_sync = functools.partial(sync_function)
    assert is_async_callable(partial_sync) is False


def test_is_async_callable_with_async_class_method() -> None:
    class MyClass:
        async def async_method(self, job: Job) -> None:
            await asyncio.sleep(0)

    instance = MyClass()
    assert is_async_callable(instance.async_method) is True


def test_is_async_callable_with_sync_class_method() -> None:
    class MyClass:
        def sync_method(self, job: Job) -> None:
            pass

    instance = MyClass()
    assert is_async_callable(instance.sync_method) is False


def test_is_async_callable_with_async_callable_instance() -> None:
    class AsyncCallable:
        async def __call__(self, job: Job) -> None:  # pragma: no cover - trivial
            await asyncio.sleep(0)

    inst = AsyncCallable()
    assert is_async_callable(inst) is True


def test_is_async_callable_with_sync_callable_instance() -> None:
    class SyncCallable:
        def __call__(self, job: Job) -> None:  # pragma: no cover - trivial
            pass

    inst = SyncCallable()
    assert is_async_callable(inst) is False


def test_is_async_callable_with_multi_level_partial_async_callable_instance() -> None:
    class AsyncCallable:
        async def __call__(self, job: Job) -> None:  # pragma: no cover - trivial
            await asyncio.sleep(0)

    inst = AsyncCallable()
    p1 = functools.partial(inst)
    p2 = functools.partial(p1)
    p3 = functools.partial(p2)
    assert is_async_callable(p3) is True


def test_is_async_callable_with_multi_level_partial_sync_callable_instance() -> None:
    class SyncCallable:
        def __call__(self, job: Job) -> None:
            pass

    inst = SyncCallable()
    p1 = functools.partial(inst)
    p2 = functools.partial(p1)
    p3 = functools.partial(p2)
    assert is_async_callable(p3) is False


def test_is_async_callable_with_multi_level_partial_plain_async_function() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    p1 = functools.partial(base)
    p2 = functools.partial(p1)
    p3 = functools.partial(p2)
    assert is_async_callable(p3) is True


def test_is_async_callable_with_multi_level_partial_plain_sync_function() -> None:
    def base(job: Job) -> None:
        pass

    p1 = functools.partial(base)
    p2 = functools.partial(p1)
    p3 = functools.partial(p2)
    assert is_async_callable(p3) is False


def test_is_async_callable_with_async_decorator_wrapper() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def async_decorator(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        @functools.wraps(f)
        async def wrapper(job: Job) -> None:
            await f(job)

        return wrapper

    wrapped = async_decorator(base)
    assert is_async_callable(wrapped) is True


def test_is_async_callable_with_sync_decorator_returning_coroutine() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def sync_decorator(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        @functools.wraps(f)
        def wrapper(job: Job) -> Awaitable[None]:
            # Returns coroutine object but wrapper itself is sync
            return f(job)

        return wrapper

    wrapped = sync_decorator(base)
    # wrapper is a sync def that returns a coroutine object -> should be False
    assert is_async_callable(wrapped) is False


def test_is_async_callable_with_deeply_nested_sync_wrappers_over_async_function() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def wrap_sync(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        def w(job: Job) -> Awaitable[None]:
            return f(job)

        return w

    lvl1 = wrap_sync(base)
    lvl2 = wrap_sync(lvl1)
    lvl3 = wrap_sync(lvl2)
    # All wrappers are sync functions returning coroutine objects
    assert is_async_callable(lvl3) is False


def test_is_async_callable_with_deeply_nested_async_wrappers() -> None:
    def make_async_wrapper(f: Callable[[Job], Awaitable[None]]) -> Callable[[Job], Awaitable[None]]:
        async def w(job: Job) -> None:
            result = f(job)
            # If underlying returns coroutine, await it
            if asyncio.iscoroutine(result):
                await result

        return w

    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    lvl1 = make_async_wrapper(base)
    lvl2 = make_async_wrapper(lvl1)
    lvl3 = make_async_wrapper(lvl2)
    assert is_async_callable(lvl3) is True


def test_is_async_callable_with_function_returning_coroutine_object() -> None:
    async def base(job: Job) -> None:
        await asyncio.sleep(0)

    def returns_coroutine(job: Job) -> Awaitable[None]:
        # Synchronous function returning a coroutine object
        return base(job)

    assert is_async_callable(returns_coroutine) is False


def test_is_async_callable_with_partial_wrapped_sync_function() -> None:
    def inner(job: Job) -> None:
        pass

    def decorator(f: Callable[[Job], None]) -> Callable[[Job], None]:
        def wrapper(job: Job) -> None:
            return f(job)

        return wrapper

    wrapped = decorator(inner)
    p = functools.partial(wrapped)
    assert is_async_callable(p) is False


def test_is_async_callable_with_sync_dunder_call_returning_coroutine() -> None:
    async def async_inner(job: Job) -> None:
        await asyncio.sleep(0)

    class HybridCallable:
        # Synchronous __call__ that returns a coroutine object
        def __call__(self, job: Job) -> Awaitable[None]:
            return async_inner(job)

    inst = HybridCallable()
    assert is_async_callable(inst) is False


async def test_retry_with_backoff_entrypoint_executor_max_attempts(apgdriver: Driver) -> None:
    jobs = list[Job]()

    async def raises(job: Job) -> None:
        await asyncio.sleep(0)
        jobs.append(job)
        raise ValueError

    parameters = EntrypointExecutorParameters(
        channel=Channel("test_retry_with_backoff_entrypoint_executor_max_attempts"),
        concurrency_limit=10,
        connection=apgdriver,
        queries=Queries(apgdriver),
        requests_per_second=float("+inf"),
        retry_timer=timedelta(seconds=300),
        serialized_dispatch=False,
        shutdown=asyncio.Event(),
        func=raises,
    )
    exc = RetryWithBackoffEntrypointExecutor(
        parameters,
        initial_delay=0,
        jitter=lambda: 0,
    )

    exc.max_attempts = 5
    exc.max_time = timedelta(seconds=300)
    mj = mocked_job()
    with pytest.raises(MaxRetriesExceeded):
        await exc.execute(
            mj, Context(anyio.CancelScope(), resources={"test_key": "retry_max_attempts_2"})
        )
    assert sum(j.id == mj.id for j in jobs) == exc.max_attempts

    exc.max_attempts = 10
    exc.max_time = timedelta(seconds=300)
    mj = mocked_job()
    with pytest.raises(MaxRetriesExceeded):
        await exc.execute(
            mj, Context(anyio.CancelScope(), resources={"test_key": "retry_max_time_1"})
        )
    assert sum(j.id == mj.id for j in jobs) == exc.max_attempts


async def test_retry_with_backoff_entrypoint_executor_max_time(apgdriver: Driver) -> None:
    async def raises(_: Job) -> None:
        await asyncio.sleep(0.01)
        raise ValueError

    parameters = EntrypointExecutorParameters(
        channel=Channel("test_retry_with_backoff_entrypoint_executor_max_time"),
        concurrency_limit=10,
        connection=apgdriver,
        queries=Queries(apgdriver),
        requests_per_second=float("+inf"),
        retry_timer=timedelta(seconds=300),
        serialized_dispatch=False,
        shutdown=asyncio.Event(),
        func=raises,
    )
    exc = RetryWithBackoffEntrypointExecutor(
        parameters,
        initial_delay=0,
        jitter=lambda: 0,
    )

    exc.max_attempts = 1000
    exc.max_time = timedelta(seconds=0.01)
    mj = mocked_job()
    with (
        timer() as elp,
        pytest.raises(MaxTimeExceeded),
    ):
        await exc.execute(
            mj, Context(anyio.CancelScope(), resources={"test_key": "retry_max_time_2"})
        )

    leeway = 1.1
    assert elp() * leeway >= exc.max_time

    exc.max_attempts = 1000
    exc.max_time = timedelta(seconds=0.1)
    mj = mocked_job()
    with (
        timer() as elp,
        pytest.raises(MaxTimeExceeded),
    ):
        await exc.execute(
            mj, Context(anyio.CancelScope(), resources={"test_key": "retry_until_pass"})
        )

    assert elp() * leeway >= exc.max_time


async def test_retry_with_backoff_entrypoint_executor_until_pass(apgdriver: Driver) -> None:
    N = 10
    jobs = list[Job]()

    async def raises(job: Job) -> None:
        await asyncio.sleep(0.001)
        jobs.append(job)
        if len(jobs) > N:
            return
        raise ValueError

    parameters = EntrypointExecutorParameters(
        channel=Channel("test_retry_with_backoff_entrypoint_executor_until_pass"),
        concurrency_limit=10,
        connection=apgdriver,
        queries=Queries(apgdriver),
        requests_per_second=float("+inf"),
        retry_timer=timedelta(seconds=300),
        serialized_dispatch=False,
        shutdown=asyncio.Event(),
        func=raises,
    )
    exc = RetryWithBackoffEntrypointExecutor(
        parameters,
        initial_delay=0,
        jitter=lambda: 0,
    )

    exc.max_attempts = 1000
    exc.max_time = timedelta(seconds=0.1)
    mj = mocked_job()
    await exc.execute(mj, Context(anyio.CancelScope()))


def test_is_async_callable_with_inspect_vs_call_attribute() -> None:
    async def af(job: Job) -> None:
        pass

    class AsyncCallable:
        async def __call__(self, job: Job) -> None:
            pass

    async_inst = AsyncCallable()

    # Plain async function: its own object is a coroutine function, its __call__ isn't.
    assert inspect.iscoroutinefunction(af) is True
    assert inspect.iscoroutinefunction(getattr(af, "__call__", None)) is False

    # Instance with async __call__: the instance itself is not a coroutine function
    # but its __call__ method is.
    assert inspect.iscoroutinefunction(async_inst) is False
    assert inspect.iscoroutinefunction(getattr(async_inst, "__call__", None)) is True

    # Helper should classify both as async callables.
    assert is_async_callable(af) is True
    assert is_async_callable(async_inst) is True
