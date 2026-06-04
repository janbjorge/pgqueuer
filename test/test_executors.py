import asyncio
import functools
import inspect
from datetime import timedelta
from multiprocessing import Process, Queue as MPQueue
from typing import Awaitable, Callable, cast
from unittest.mock import Mock

import anyio
import pytest

from pgqueuer.db import Driver
from pgqueuer.executors import (
    AbstractEntrypointExecutor,
    EntrypointExecutor,
    EntrypointExecutorParameters,
    is_async_callable,
    wants_context,
)
from pgqueuer.models import Context, Job, ScheduleContext
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

    assert not executor.parameters.accepts_context

    await executor.execute(job, job_context)
    assert calls == [(job, None)]


async def handler_no_context(job: Job) -> None: ...
async def handler_positional_context(job: Job, ctx: Context) -> None: ...
async def handler_positional_only_context(job: Job, ctx: Context, /) -> None: ...
async def handler_string_context(job: Job, ctx: "Context") -> None: ...
async def handler_context_first(ctx: Context) -> None: ...
async def handler_unannotated_second(job: Job, ctx) -> None: ...  # type: ignore[no-untyped-def]
async def handler_unrelated_second(job: Job, config: dict | None = None) -> None: ...
async def handler_keyword_only_context(job: Job, *, ctx: Context) -> None: ...
async def handler_var_positional(job: Job, *args: object) -> None: ...
async def handler_var_keyword(job: Job, **kwargs: object) -> None: ...
async def handler_no_params() -> None: ...
async def handler_schedule_context(job: Job, ctx: ScheduleContext) -> None: ...


@pytest.mark.parametrize(
    ("func", "context_type", "expected"),
    [
        pytest.param(handler_no_context, Context, False, id="no-second-param"),
        pytest.param(handler_positional_context, Context, True, id="positional-or-keyword"),
        pytest.param(handler_positional_only_context, Context, True, id="positional-only"),
        pytest.param(handler_string_context, Context, True, id="string-forward-ref"),
        pytest.param(handler_context_first, Context, True, id="context-in-first-slot"),
        pytest.param(handler_unannotated_second, Context, False, id="unannotated-second"),
        pytest.param(handler_unrelated_second, Context, False, id="unrelated-second"),
        pytest.param(handler_keyword_only_context, Context, False, id="keyword-only-excluded"),
        pytest.param(handler_var_positional, Context, False, id="var-positional-excluded"),
        pytest.param(handler_var_keyword, Context, False, id="var-keyword-excluded"),
        pytest.param(handler_no_params, Context, False, id="no-params"),
        pytest.param(handler_schedule_context, ScheduleContext, True, id="schedulecontext-match"),
        pytest.param(handler_schedule_context, Context, False, id="schedulecontext-not-context"),
        pytest.param(handler_positional_context, ScheduleContext, False, id="context-not-schedule"),
    ],
)
def test_wants_context_matrix(
    func: Callable[..., Awaitable[None]],
    context_type: type,
    expected: bool,
) -> None:
    assert wants_context(func, context_type) is expected


def test_wants_context_unwraps_partial() -> None:
    async def handler(prefix: str, job: Job, ctx: Context) -> None: ...

    # functools.partial drops the bound leading argument; ctx stays positional.
    assert wants_context(functools.partial(handler, "prefix"), Context)


def test_wants_context_partial_keyword_binding_excludes_context() -> None:
    async def handler(job: Job, ctx: Context) -> None: ...

    # Binding ctx by keyword turns it keyword-only, so it is no longer positionally bindable.
    # The bound value is never used — wants_context only inspects the signature.
    bound = functools.partial(handler, ctx=cast(Context, object()))
    assert not wants_context(bound, Context)


def test_wants_context_detects_callable_object() -> None:
    class Handler:
        async def __call__(self, job: Job, ctx: Context) -> None: ...

    assert wants_context(Handler(), Context)


def test_wants_context_false_when_annotations_unresolvable() -> None:
    async def handler(job: Job, ctx: Context, extra: object) -> None: ...

    # eval_str fails on the unresolvable ref, so detection degrades to False even though a
    # Context parameter is present (documents the all-or-nothing fallback in the except branch).
    handler.__annotations__["extra"] = "Undefined_Name_For_Test"
    assert not wants_context(handler, Context)


def test_entrypoint_auto_detects_context() -> None:
    qm = QueueManager(Mock())

    @qm.entrypoint("with_context")
    async def with_context(job: Job, ctx: Context) -> None: ...

    @qm.entrypoint("without_context")
    async def without_context(job: Job) -> None: ...

    @qm.entrypoint("unrelated_second")
    async def unrelated_second(job: Job, config: dict | None = None) -> None: ...

    assert qm.entrypoint_registry["with_context"].parameters.accepts_context
    assert not qm.entrypoint_registry["without_context"].parameters.accepts_context
    assert not qm.entrypoint_registry["unrelated_second"].parameters.accepts_context


def test_entrypoint_explicit_flag_overrides_detection() -> None:
    qm = QueueManager(Mock())

    @qm.entrypoint("forced_off", accepts_context=False)
    async def forced_off(job: Job, ctx: Context) -> None: ...

    @qm.entrypoint("forced_on", accepts_context=True)
    async def forced_on(job: Job) -> None: ...

    assert not qm.entrypoint_registry["forced_off"].parameters.accepts_context
    assert qm.entrypoint_registry["forced_on"].parameters.accepts_context


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
    qm = QueueManager(Queries(apgdriver))
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
