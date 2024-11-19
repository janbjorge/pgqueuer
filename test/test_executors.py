import asyncio
import functools
import uuid
from datetime import datetime, timedelta, timezone
from multiprocessing import Process, Queue as MPQueue

import anyio
import pytest

from pgqueuer.db import Driver
from pgqueuer.executors import (
    AbstractEntrypointExecutor,
    DefaultEntrypointExecutor,
    EntrypointExecutorParameters,
    is_async_callable,
)
from pgqueuer.models import Context, Job, PGChannel
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


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


@pytest.mark.asyncio
async def test_entrypoint_executor_sync(apgdriver: Driver) -> None:
    result = []

    def sync_function(job: Job) -> None:
        if job.payload:
            result.append(job.payload)

    executor = DefaultEntrypointExecutor(
        EntrypointExecutorParameters(
            channel=PGChannel("foo"),
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
    now = datetime.now(timezone.utc)
    job = Job(
        id=1,
        priority=1,
        created=now,
        heartbeat=now,
        execute_after=now,
        updated=now,
        status="queued",
        entrypoint="test",
        payload=b"test_payload",
        queue_manager_id=uuid.uuid4(),
    )

    await executor.execute(
        job,
        Context(anyio.CancelScope()),
    )

    assert result == [b"test_payload"]


@pytest.mark.asyncio
async def test_entrypoint_executor_async(apgdriver: Driver) -> None:
    result = []

    async def async_function(job: Job) -> None:
        if job.payload:
            result.append(job.payload)

    executor = DefaultEntrypointExecutor(
        EntrypointExecutorParameters(
            channel=PGChannel("foo"),
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
    now = datetime.now(timezone.utc)
    job = Job(
        id=1,
        priority=1,
        created=now,
        heartbeat=now,
        execute_after=now,
        updated=now,
        status="queued",
        entrypoint="test",
        payload=b"test_payload",
        queue_manager_id=uuid.uuid4(),
    )

    await executor.execute(
        job,
        Context(anyio.CancelScope()),
    )

    assert result == [b"test_payload"]


@pytest.mark.asyncio
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
    now = datetime.now(timezone.utc)
    job = Job(
        id=1,
        priority=1,
        created=now,
        heartbeat=now,
        execute_after=now,
        updated=now,
        status="queued",
        entrypoint="test",
        payload=b"thread_payload",
        queue_manager_id=uuid.uuid4(),
    )

    await executor.execute(
        job,
        Context(anyio.CancelScope()),
    )

    assert executor.result == [b"thread_payload"]


@pytest.mark.asyncio
async def test_custom_multiprocessing_executor() -> None:
    executor = MultiprocessingExecutor()
    now = datetime.now(timezone.utc)
    job = Job(
        id=1,
        priority=1,
        created=now,
        heartbeat=now,
        execute_after=now,
        updated=now,
        status="queued",
        entrypoint="test",
        payload=b"process_payload",
        queue_manager_id=uuid.uuid4(),
    )

    await executor.execute(
        job,
        Context(anyio.CancelScope()),
    )
    result = executor.queue.get()

    assert result == b"process_payload"


@pytest.mark.asyncio
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


def test_is_async_callable_with_class_method() -> None:
    class MyClass:
        async def async_method(self, job: Job) -> None:
            await asyncio.sleep(0)

        def sync_method(self, job: Job) -> None:
            pass

    instance = MyClass()
    assert is_async_callable(instance.async_method) is True
    assert is_async_callable(instance.sync_method) is False
