import asyncio

import pytest

from pgqueuer.tm import TaskManager


@pytest.mark.parametrize("N", (1, 2, 3, 5, 64))
async def test_task_manager(N: int) -> None:
    future = asyncio.Future[None]()

    tm = TaskManager()
    assert len(tm.tasks) == 0

    async def waiter(future: asyncio.Future) -> None:
        return await future

    for _ in range(N):
        tm.add(asyncio.create_task(waiter(future)))

    assert len(tm.tasks) == N

    future.set_result(None)
    await asyncio.gather(*tm.tasks)
    assert len(tm.tasks) == 0


@pytest.mark.parametrize("N", (1, 2, 3, 5, 64))
async def test_task_manager_ctx_mngr(N: int) -> None:
    async def waiter(future: asyncio.Future) -> None:
        return await future

    future = asyncio.Future[None]()

    async with TaskManager() as tm:
        assert len(tm.tasks) == 0

        for _ in range(N):
            tm.add(asyncio.create_task(waiter(future)))

        assert len(tm.tasks) == N

        future.set_result(None)

    assert len(tm.tasks) == 0


@pytest.mark.parametrize("N", (1, 2, 3, 5, 64))
async def test_task_manager_ctx_mngr_exception(
    N: int,
) -> None:
    async def waiter(event: asyncio.Event) -> None:
        await event.wait()
        raise RuntimeError

    event = asyncio.Event()

    async with TaskManager() as tm:
        assert len(tm.tasks) == 0

        for _ in range(N):
            tm.add(asyncio.create_task(waiter(event)))

        assert len(tm.tasks) == N

        event.set()

    assert len(tm.tasks) == 0


@pytest.mark.parametrize("N", (1, 2, 3))
async def test_task_manager_logs_unhandled_exception(
    N: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = asyncio.Event()

    async def raises() -> None:
        await event.wait()
        raise ValueError("Raises ValueError")

    async with TaskManager() as tm:
        for _ in range(N):
            tm.add(asyncio.create_task(raises()))
        event.set()

    assert len(caplog.messages) == N


async def test_task_manager_no_log_on_cancel(
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = asyncio.Event()

    async with TaskManager() as tm:
        task = asyncio.create_task(event.wait())
        tm.add(task)
        task.cancel()

    assert len(caplog.messages) == 0
