import asyncio

import pytest
from PgQueuer.tm import TaskManager


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
